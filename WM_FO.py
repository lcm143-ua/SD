# Aplicación del operario de campo: pide riegos a la central y sigue su evolución.
# Toda la comunicación es por Kafka (no hay sockets con la central).
import sys
import threading
import time
import queue
import logging
from kafka import KafkaProducer, KafkaConsumer   # pip install kafka-python

FORMAT = 'utf-8'
FIN = "FIN"

TOPIC_PETICIONES = "WM_peticiones"  # FO -> Central: SOLICITUD, LISTA
TOPIC_RESPUESTAS = "WM_respuestas"  # Central -> FO
REINTENTO_KAFKA = 3         # segundos entre intentos de conexión con el broker
TIMEOUT_CENTRAL = 5         # segundos sin respuesta a una petición = la central no está
TIMEOUT_RESPUESTA = 10      # segundos sin noticias de un riego del fichero = se da por fallido
ESPERA_FICHERO = 4          # segundos entre una petición del fichero y la siguiente (enunciado)
SESION_KAFKA_MS = 6000      # tiempo máximo para que el broker dé por muerta una sesión anterior del FO
SIN_CENTRAL = "Imposible conectar con WM_Central"

# Mensajes que cierran una petición (con éxito o sin él)
FINALES = ("DENEGADO", "RESUMEN")

logging.getLogger("kafka").setLevel(logging.CRITICAL)

id_operario = None
productor = None
consumidor_listo = threading.Event()

# El hilo de Kafka deja aquí los mensajes para el que está procesando el fichero
cola_fichero = queue.Queue()
procesando_fichero = threading.Event()

# Se activa con cualquier mensaje de la central para este operario. Si tras una
# petición no se activa en TIMEOUT_CENTRAL segundos, la central no está.
respuesta_central = threading.Event()


########## KAFKA ##########

def conectar_kafka(bootstrap):
    global productor
    while productor is None:
        try:
            productor = KafkaProducer(bootstrap_servers=bootstrap, max_block_ms=2000)
        except Exception:
            print(f"[KAFKA] No se puede conectar con el broker {bootstrap}, reintentando en {REINTENTO_KAFKA}s")
            time.sleep(REINTENTO_KAFKA)
    print(f"[KAFKA] Conectado al broker {bootstrap}")


def escuchar_respuestas(bootstrap):
    # Todos los FO leen el mismo topic y cada uno se queda con los que llevan su ID.
    # Cada FO tiene su grupo (WM_FO_FO_01) y confirma (commit) lo que ya ha mostrado:
    # si el FO se cierra a mitad de un riego, al volver recibe el RESUMEN pendiente.
    consumidor = None
    while consumidor is None:
        try:
            consumidor = KafkaConsumer(TOPIC_RESPUESTAS, bootstrap_servers=bootstrap,
                                       group_id=f"WM_FO_{id_operario}", auto_offset_reset="latest",
                                       enable_auto_commit=False, session_timeout_ms=SESION_KAFKA_MS,
                                       heartbeat_interval_ms=SESION_KAFKA_MS // 3)
        except Exception:
            time.sleep(REINTENTO_KAFKA)

    # Hay que tener asignada la partición antes de pedir nada: si no, con "latest"
    # se perderían las respuestas que lleguen mientras tanto.
    # poll puede devolver ya mensajes pendientes: se muestran, no se descartan.
    # Unirse al grupo tarda un par de segundos: solo se avisa si tarda más de 5.
    intentos = 0
    while not consumidor.assignment():
        for registros in consumidor.poll(timeout_ms=1000).values():
            for registro in registros:
                atender_registro(registro)
        intentos += 1
        if not consumidor.assignment() and intentos == 5:
            print(f"[KAFKA] Esperando al topic {TOPIC_RESPUESTAS} (¿está arrancada la central?)")
    for particion in consumidor.assignment():
        consumidor.position(particion)
    consumidor.commit()
    consumidor_listo.set()

    for registro in consumidor:
        atender_registro(registro)
        consumidor.commit()


def atender_registro(registro):
    partes = registro.value.decode(FORMAT).split('#')
    if len(partes) >= 3 and partes[1] == id_operario:
        respuesta_central.set()
        mostrar_respuesta(partes)
        if procesando_fichero.is_set():
            cola_fichero.put(partes)


def vigilar_central():
    # Tras una petición manual: si la central no contesta nada, se avisa al operario
    if not respuesta_central.wait(TIMEOUT_CENTRAL):
        print(f"[CENTRAL] {SIN_CENTRAL} (sin respuesta en {TIMEOUT_CENTRAL} s)")


def enviar(mensaje, vigilar=False):
    # vigilar=True: petición manual, se avisa si la central no contesta
    if productor is None or not consumidor_listo.is_set():
        print("[KAFKA] Todavía no hay conexión con el broker, prueba en unos segundos")
        return False
    respuesta_central.clear()
    try:
        productor.send(TOPIC_PETICIONES, key=id_operario.encode(FORMAT), value=mensaje.encode(FORMAT))
        productor.flush(5)
    except Exception as e:
        print(f"[KAFKA] No se ha podido enviar la petición: {e}")
        return False
    if vigilar:
        threading.Thread(target=vigilar_central, daemon=True).start()
    return True


########## MENSAJES DE LA CENTRAL ##########

def mostrar_respuesta(partes):
    tipo = partes[0]
    ws = partes[2]
    if tipo == "RECIBIDA":
        print(f"[{ws}] La central ha recibido la petición y comprueba la estación...")
    elif tipo == "AUTORIZADO":
        print(f"[{ws}] AUTORIZADO: la central ordena a la estación regar {partes[3]} s")
    elif tipo == "DENEGADO":
        print(f"[{ws}] DENEGADO: {partes[3]}")
    elif tipo == "INICIO":
        print(f"[{ws}] Electroválvula ABIERTA, empieza el riego (límite {partes[3]} s)")
    elif tipo == "TELEMETRIA":
        print(f"[{ws}] Caudal: {float(partes[3]):.2f} l/min | Acumulado: {float(partes[4]):.2f} l")
    elif tipo == "RESUMEN":
        volumen, duracion, motivo = partes[3], partes[4], partes[5]
        print(f"[{ws}] RIEGO TERMINADO ({motivo}) | Volumen total: {volumen} l | Duración: {duracion} s")
    elif tipo == "LISTA":
        print("[ESTACIONES]")
        for estacion in partes[2].split(';'):
            if estacion:
                id_ws, estado = estacion.split('=')
                print(f"   {id_ws:<8} {estado}")
    else:
        print(f"[CENTRAL] {'#'.join(partes)}")


########## PETICIONES ##########

def solicitar_riego(id_ws, segundos, vigilar=False):
    print(f"[PETICIÓN] Riego en {id_ws} durante {segundos} s")
    return enviar(f"SOLICITUD#{id_operario}#{id_ws}#{segundos}", vigilar)


def esperar_fin(id_ws):
    # Espera a que la petición termine (DENEGADO o RESUMEN). Si la central o la
    # estación dejan de dar noticias durante TIMEOUT_RESPUESTA segundos, se da por fallida.
    contesto = False
    while True:
        try:
            partes = cola_fichero.get(timeout=TIMEOUT_RESPUESTA)
        except queue.Empty:
            if contesto:
                print(f"[{id_ws}] Sin noticias en {TIMEOUT_RESPUESTA}s, se da la petición por fallida")
            else:
                print(f"[{id_ws}] {SIN_CENTRAL}, se da la petición por fallida")
            return
        contesto = True
        if partes[2] == id_ws and partes[0] in FINALES:
            return


def leer_fichero(ruta):
    # Formato: una petición por línea, id_estacion#segundos (por ejemplo WS_01#30).
    # Se ignoran las líneas vacías y las que empiezan por //
    peticiones = []
    with open(ruta, encoding=FORMAT) as f:
        for num, linea in enumerate(f, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("//"):
                continue
            partes = linea.split('#')
            if len(partes) == 2 and partes[1].strip().isdigit() and int(partes[1]) > 0:
                peticiones.append((partes[0].strip(), int(partes[1])))
            else:
                print(f"[FICHERO] Línea {num} no válida, se salta: {linea}")
    return peticiones


def procesar_fichero(ruta):
    try:
        peticiones = leer_fichero(ruta)
    except OSError as e:
        print(f"[FICHERO] No se puede abrir {ruta}: {e}")
        return

    print(f"[FICHERO] {len(peticiones)} peticiones en {ruta}")
    consumidor_listo.wait()
    procesando_fichero.set()
    for i, (id_ws, segundos) in enumerate(peticiones):
        # Se vacía la cola por si quedan mensajes de la petición anterior
        while not cola_fichero.empty():
            cola_fichero.get_nowait()
        if solicitar_riego(id_ws, segundos):
            esperar_fin(id_ws)
        if i < len(peticiones) - 1:
            print(f"[FICHERO] Siguiente petición en {ESPERA_FICHERO} s")
            time.sleep(ESPERA_FICHERO)
    procesando_fichero.clear()
    print("[FICHERO] Todas las peticiones del fichero han terminado")


########## MAIN ##########

def mostrar_menu():
    print("Comandos:")
    print("  R <id_estacion> <segundos> = pedir un riego (ejemplo: R WS_01 30)")
    print("  L = ver las estaciones y su estado")
    print("  F <fichero> = lanzar las peticiones de un fichero")
    print("  FIN = salir")


def main():
    global id_operario
    if len(sys.argv) not in (4, 5) or not sys.argv[2].isdigit():
        print("Uso: python WM_FO.py <ip_broker> <puerto_broker> <id_operario> [fichero_peticiones]")
        print("Ejemplo: python WM_FO.py 127.0.0.1 9092 FO_01 peticiones.txt")
        return

    bootstrap = f"{sys.argv[1]}:{sys.argv[2]}"
    id_operario = sys.argv[3]
    if '#' in id_operario:
        print("El ID del operario no puede contener '#' (es el separador del protocolo)")
        return

    threading.Thread(target=conectar_kafka, args=(bootstrap,), daemon=True).start()
    threading.Thread(target=escuchar_respuestas, args=(bootstrap,), daemon=True).start()

    if len(sys.argv) == 5:
        threading.Thread(target=procesar_fichero, args=(sys.argv[4],), daemon=True).start()

    mostrar_menu()
    salir = False
    while not salir:
        try:
            partes = input().strip().split()
        except EOFError:
            partes = [FIN]
        comando = partes[0].upper() if partes else ""

        if comando == "R":
            if len(partes) != 3 or not partes[2].isdigit() or int(partes[2]) <= 0 or '#' in partes[1]:
                print("Uso: R <id_estacion> <segundos>   (ejemplo: R WS_01 30)")
            else:
                solicitar_riego(partes[1], int(partes[2]), vigilar=True)
        elif comando == "L":
            enviar(f"LISTA#{id_operario}", vigilar=True)
        elif comando == "F":
            if len(partes) != 2:
                print("Uso: F <fichero>")
            elif procesando_fichero.is_set():
                print("[FICHERO] Ya se está procesando un fichero")
            else:
                threading.Thread(target=procesar_fichero, args=(partes[1],), daemon=True).start()
        elif comando == FIN:
            print("[DESCONEXIÓN] Cerrando la aplicación del operario.")
            salir = True
        elif comando:
            print("Comando no válido.")
            mostrar_menu()


print("****** WM_FO - APLICACIÓN DEL OPERARIO ****")
main()
