# SIMULA el hardware de la estacióno de riego. Se conecta al monitor y responde a cada HEALTH con el estado de salud.
import socket
import sys
import os
import json
import threading
import time
import random
import logging
from kafka import KafkaProducer, KafkaConsumer   # pip install kafka-python

FORMAT = 'utf-8'
FIN = "FIN"

# Protocolo de sockets recomendado en el enunciado: <STX><DATA><ETX><LRC>
#   - Al conectar, el cliente envía ENQ y el servidor contesta ACK (handshake).
#   - Cada trama recibida se confirma con ACK (LRC correcto) o NACK (corrupta);
#     con NACK el emisor la reenvía, como mucho MAX_REINTENTOS veces.
#   - Al cerrar de forma ordenada se envía EOT.
STX = b'\x02'
ETX = b'\x03'
EOT = b'\x04'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
MAX_REINTENTOS = 3
MAX_TRAMA = 1024        # bytes de DATA como máximo; más largo se considera basura

# Protocolo Monitor <-> Engine (el mismo que en WM_WS_M)
HEALTH = "HEALTH"       # pregunta del monitor cada segundo, a la que el engine responde con OK o KO según su estado de salud
OK = "OK"               # todo funciona
KO = "KO"               # avería simulada (fuga)
REINTENTO_MONITOR = 3   # segundos entre intentos de conexión al monitor
TIMEOUT_MONITOR = 5     # segundos sin recibir nada del monitor = monitor caído (envía HEALTH cada segundo)

# Simulación del caudalímetro (litros/minuto)
CAUDAL_MIN = 15.0 # el caudal simulado es un número aleatorio entre 15 y 25
CAUDAL_MAX = 25.0
OPERARIO_LOCAL = "LOCAL"    # riego activado desde el menú de la propia estación

# Motivos por los que termina un riego
FIN_TIEMPO = "TIEMPO"       # se alcanzó el límite de tiempo
FIN_MANUAL = "MANUAL"       # comando P en el menú de la estación
FIN_FUGA = "FUGA"           # avería durante el riego
FIN_APAGADO = "APAGADO"     # el engine se apaga con FIN
FIN_BLOQUEO = "BLOQUEO"     # orden BLOQUEAR de la central (llega por Kafka)
FIN_SIN_MONITOR = "SIN_MONITOR"     # se ha perdido el monitor: la estación queda averiada
FIN_CAIDA_ENGINE = "CAIDA_ENGINE"   # el engine se cayó regando; se envía al volver a arrancar

# Kafka: el engine publica sus eventos de riego y escucha las órdenes de la central
TOPIC_EVENTOS = "WM_eventos"    # Engine -> Central: RIEGO_INICIO, TELEMETRIA, RIEGO_FIN, RECHAZADO
TOPIC_ORDENES = "WM_ordenes"    # Central -> Engine: REGAR, BLOQUEAR, ACTIVAR
REINTENTO_KAFKA = 3             # segundos entre intentos de conexión con el broker
# Si el engine se reinicia, el broker tarda como mucho esto en dar por muerta la
# sesión anterior del grupo y repartirle la partición a la nueva
SESION_KAFKA_MS = 6000

# Fichero donde se guarda el riego en curso cada segundo (riego_WS_01.json):
# si el engine se cae regando, al volver envía el RIEGO_FIN con lo que se llegó a regar
FICHERO_RIEGO = "riego_{}.json"

# kafka-python escribe sus propios avisos en la consola y taparían el menú
logging.getLogger("kafka").setLevel(logging.CRITICAL)

# Estado de salud que se responde al monitor. Lo cambia el teclado (hilo principal)
# y lo lee el hilo del monitor; es una simple asignación, no hace falta lock.
estado_engine = OK
id_ws = None
id_asignado = threading.Event()     # el consumidor de órdenes necesita el ID para su grupo

# Sin monitor la estación está AVERIADA: no se puede regar (ni a mano) y el riego
# en curso se corta, porque nadie vigila si hay fugas
monitor_conectado = False

# Productor de Kafka (None hasta que el broker responde)
productor = None

# Orden BLOQUEAR de la central: mientras dure no se puede regar, ni siquiera a mano
bloqueada = False

# Riego en curso (None si la electroválvula está cerrada). Lo usan el hilo del
# teclado y el hilo del riego, así que se protege con un lock.
riego = None
lock_riego = threading.Lock()


########## PROTOCOLO <STX><DATA><ETX><LRC> ##########

def calcular_lrc(datos):
    # XOR byte a byte del mensaje
    lrc = 0
    for byte in datos:
        lrc ^= byte
    return lrc


def empaquetar(msg):
    datos = msg.encode(FORMAT)
    if STX in datos or ETX in datos:
        raise ValueError("el mensaje no puede contener los caracteres STX/ETX")
    return STX + datos + ETX + bytes([calcular_lrc(datos)])


def recibir_bytes(sock, size):
    # recv puede devolver menos bytes de los pedidos: se repite hasta tenerlos todos
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            return None
        data.extend(part)
    return bytes(data)


def recibir_trama(sock):
    # Devuelve (datos, lrc_correcto) o None si la conexión se cierra o llega EOT
    byte = recibir_bytes(sock, 1)
    while byte is not None and byte != STX:   # se descarta lo que haya antes de STX
        if byte == EOT:
            return None
        byte = recibir_bytes(sock, 1)
    if byte is None:
        return None

    datos = bytearray()
    byte = recibir_bytes(sock, 1)
    while byte is not None and byte != ETX and len(datos) <= MAX_TRAMA:
        datos.extend(byte)
        byte = recibir_bytes(sock, 1)
    if byte is None:
        return None
    if byte != ETX:
        return bytes(datos), False

    lrc = recibir_bytes(sock, 1)
    if lrc is None:
        return None
    return bytes(datos), lrc[0] == calcular_lrc(datos)


def send(sock, msg):
    # Envía la trama y espera su ACK. Con NACK (o respuesta rara) se reenvía.
    trama = empaquetar(msg)
    for _ in range(MAX_REINTENTOS):
        sock.sendall(trama)
        confirmacion = recibir_bytes(sock, 1)
        if confirmacion is None:
            raise ConnectionError("conexión cerrada esperando ACK")
        if confirmacion == ACK:
            return
    raise ConnectionError(f"trama rechazada {MAX_REINTENTOS} veces")


def receive(sock):
    # Recibe una trama, la valida con el LRC y contesta ACK o NACK.
    # Devuelve el mensaje, o None si el otro extremo ha cerrado.
    for _ in range(MAX_REINTENTOS):
        trama = recibir_trama(sock)
        if trama is None:
            return None
        datos, correcta = trama
        if correcta:
            try:
                msg = datos.decode(FORMAT)
                sock.sendall(ACK)
                return msg
            except UnicodeDecodeError:
                pass
        sock.sendall(NACK)
    raise ConnectionError(f"{MAX_REINTENTOS} tramas corruptas seguidas")


def saludar(sock):
    # Handshake del lado cliente: ENQ -> ACK
    sock.sendall(ENQ)
    if recibir_bytes(sock, 1) != ACK:
        raise ConnectionError("el servidor no aceptó el ENQ")


def despedirse(sock):
    # Cierre ordenado: EOT y close (si el otro ya cerró, da igual)
    try:
        sock.sendall(EOT)
    except OSError:
        pass
    sock.close()


########## KAFKA ##########

def conectar_kafka(bootstrap):
    # El broker puede no estar listo todavía: se reintenta hasta que conteste.
    # Después kafka-python se reconecta solo si el broker se cae un rato.
    global productor
    while productor is None:
        try:
            # max_block_ms: si el broker cae, send no se queda bloqueado 60 s (por defecto)
            productor = KafkaProducer(bootstrap_servers=bootstrap, max_block_ms=2000)
        except Exception:
            print(f"\n[KAFKA] No se puede conectar con el broker {bootstrap}, reintentando en {REINTENTO_KAFKA}s")
            time.sleep(REINTENTO_KAFKA)
    print(f"\n[KAFKA] Conectado al broker {bootstrap}")
    escuchar_ordenes(bootstrap)


def publicar_evento(mensaje, confirmar=False):
    # Publica un evento de riego para WM_Central (que se lo reenvía al operario).
    # Formatos:
    #   RIEGO_INICIO#id_ws#operario#limite_segundos
    #   TELEMETRIA#id_ws#caudal#volumen_acumulado
    #   RIEGO_FIN#id_ws#operario#volumen_total#duracion#motivo
    #   RECHAZADO#id_ws#operario#motivo   (orden REGAR que la estación no puede cumplir)
    # La clave es el ID de la estación: así sus eventos van siempre en orden.
    # Con confirmar=True se espera a que el broker lo guarde (para el RIEGO_FIN,
    # que no se puede perder). Devuelve True si se ha publicado.
    if productor is None:
        print(f"[KAFKA] Sin conexión con el broker, no se publica: {mensaje}")
        return False
    try:
        envio = productor.send(TOPIC_EVENTOS, key=id_ws.encode(FORMAT), value=mensaje.encode(FORMAT))
        if confirmar:
            envio.get(timeout=5)
    except Exception as e:
        print(f"[KAFKA] No se ha podido publicar '{mensaje}': {e}")
        return False
    return True


def escuchar_ordenes(bootstrap):
    # Recibe las órdenes de la central. Todas las estaciones leen el mismo topic
    # y cada engine se queda solo con las que llevan su ID.
    # Cada engine tiene su propio grupo (WM_engine_WS_01) y confirma (commit) cada
    # orden después de atenderla: si el engine está caído, las órdenes le esperan
    # en Kafka y las recibe al volver. "latest" solo se usa la primera vez que
    # arranca ese grupo, para no ejecutar órdenes de otras pruebas.
    id_asignado.wait()
    recuperar_riego_pendiente()

    consumidor = None
    while consumidor is None:
        try:
            consumidor = KafkaConsumer(TOPIC_ORDENES, bootstrap_servers=bootstrap,
                                       group_id=f"WM_engine_{id_ws}", auto_offset_reset="latest",
                                       enable_auto_commit=False, session_timeout_ms=SESION_KAFKA_MS,
                                       heartbeat_interval_ms=SESION_KAFKA_MS // 3)
        except Exception:
            time.sleep(REINTENTO_KAFKA)

    for registro in consumidor:
        partes = registro.value.decode(FORMAT).split('#')
        if len(partes) >= 2 and partes[1] == id_ws:
            atender_orden(partes)
        consumidor.commit()


def atender_orden(partes):
    global bloqueada
    orden = partes[0]
    print(f"\n[CENTRAL] Orden recibida: {'#'.join(partes)}")

    if orden == "REGAR" and len(partes) == 4 and partes[3].isdigit():
        operario = partes[2]
        ok, motivo = iniciar_riego(operario, int(partes[3]))
        if not ok:
            print(f"[RIEGO] No se puede regar: {motivo}")
            publicar_evento(f"RECHAZADO#{id_ws}#{operario}#{motivo}")
    elif orden == "BLOQUEAR":
        bloqueada = True
        print("[BLOQUEO] Estación OUT OF SERVICE (fuera de servicio) por orden de la central")
        if parar_riego(FIN_BLOQUEO):
            print("[BLOQUEO] Riego en curso cortado")
    elif orden == "ACTIVAR":
        bloqueada = False
        print("[ACTIVAR] Estación activada de nuevo por la central")
    else:
        print("[CENTRAL] Orden desconocida, se ignora")


########## RIEGO ##########

def iniciar_riego(operario, limite):
    # Abre la electroválvula si la estación puede regar. Devuelve (ok, motivo).
    # La usan el menú local y la orden REGAR que llega de la central.
    global riego
    with lock_riego:
        if id_ws is None:
            return False, "la estación aún no está autenticada (sin ID del monitor)"
        if not monitor_conectado:
            return False, "la estación está AVERIADA (sin conexión con el monitor)"
        if bloqueada:
            return False, "la estación está fuera de servicio (bloqueada por la central)"
        if estado_engine == KO:
            return False, "hay una avería (fuga) detectada"
        if riego is not None:
            return False, "ya hay un riego en curso"
        # permite cortar el riego sin esperar al segundo
        riego = {"operario": operario,"limite": limite,"inicio": time.time(),"volumen": 0.0,"caudal": 0.0,"motivo_fin": None,"despertar": threading.Event()}  
        # se crea el hilo del riego
        riego["hilo"] = threading.Thread(target=bucle_riego, args=(riego,), daemon=True)

    print(f"\n[RIEGO] Electroválvula ABIERTA. Operario: {operario} | Límite: {limite} s")
    publicar_evento(f"RIEGO_INICIO#{id_ws}#{operario}#{limite}")
    riego["hilo"].start()
    return True, ""


def parar_riego(motivo):
    # Pide al hilo del riego que termine. Devuelve False si no había riego.
    with lock_riego:
        if riego is None or riego["motivo_fin"] is not None:
            return False
        riego["motivo_fin"] = motivo
        riego["despertar"].set()
    return True


def bucle_riego(r): # Solo existe mientras se riega: mie el caudal cada segundo y decide cuándo parar
    # Cada segundo mide el caudal, acumula el volumen y comprueba si hay que parar
    global riego
    terminado = False
    while not terminado:
        r["despertar"].wait(1)

        with lock_riego:
            if r["motivo_fin"] is None and estado_engine == KO:
                r["motivo_fin"] = FIN_FUGA
            if r["motivo_fin"] is None and not monitor_conectado:
                r["motivo_fin"] = FIN_SIN_MONITOR

            if r["motivo_fin"] is None:
                r["caudal"] = round(random.uniform(CAUDAL_MIN, CAUDAL_MAX), 2)
                r["volumen"] += r["caudal"] / 60     # l/min durante 1 segundo
                transcurrido = time.time() - r["inicio"]
                print(f"[RIEGO] Caudal: {r['caudal']:.2f} l/min | Acumulado: {r['volumen']:.2f} l | {int(transcurrido)}/{r['limite']} s")
                publicar_evento(f"TELEMETRIA#{id_ws}#{r['caudal']:.2f}#{r['volumen']:.2f}")
                guardar_riego(r, int(transcurrido))
                if transcurrido >= r["limite"]:
                    r["motivo_fin"] = FIN_TIEMPO

            terminado = r["motivo_fin"] is not None

    duracion = int(time.time() - r["inicio"])
    with lock_riego:
        riego = None

    print(f"\n[RIEGO] Electroválvula CERRADA. Motivo: {r['motivo_fin']}")
    print(f"[RESUMEN] Operario: {r['operario']} | Volumen total: {r['volumen']:.2f} l | Duración: {duracion} s")
    # Solo se borra el fichero si el broker ha recibido el RIEGO_FIN: si no, se
    # reenvía cuando el engine vuelva a arrancar
    if publicar_evento(f"RIEGO_FIN#{id_ws}#{r['operario']}#{r['volumen']:.2f}#{duracion}#{r['motivo_fin']}", confirmar=True):
        borrar_riego()


def guardar_riego(r, duracion):
    # Lo regado hasta ahora, por si el engine se cae a mitad del riego
    try:
        with open(FICHERO_RIEGO.format(id_ws), "w", encoding=FORMAT) as f:
            json.dump({"operario": r["operario"], "volumen": round(r["volumen"], 2), "duracion": duracion}, f)
    except OSError as e:
        print(f"[RIEGO] No se puede guardar el riego en disco: {e}")


def borrar_riego():
    try:
        os.remove(FICHERO_RIEGO.format(id_ws))
    except OSError:
        pass


def recuperar_riego_pendiente():
    # Al arrancar: si hay un riego guardado es que el engine se cayó regando (o que
    # no pudo enviar el RIEGO_FIN). Se envía ahora con lo que se llegó a regar.
    try:
        with open(FICHERO_RIEGO.format(id_ws), encoding=FORMAT) as f:
            r = json.load(f)
    except (OSError, ValueError):
        return
    print(f"\n[RIEGO] Riego interrumpido por una caída del engine: {r['volumen']:.2f} l en {r['duracion']} s. Se informa a la central")
    while not publicar_evento(f"RIEGO_FIN#{id_ws}#{r['operario']}#{r['volumen']:.2f}#{r['duracion']}#{FIN_CAIDA_ENGINE}", confirmar=True):
        time.sleep(REINTENTO_KAFKA)
    borrar_riego()


def atender_monitor(addr_monitor): # habla por sockets con WM_WS_M, recibe el ID y responde a cada HEALTH
    # Se conecta al monitor y responde a cada HEALTH con el estado actual.
    # Si el monitor se cae, vuelve a intentarlo cada REINTENTO_MONITOR segundos.
    global id_ws, monitor_conectado
    while True:
        try:
            # El timeout queda puesto en el socket: si el monitor deja de enviar
            # HEALTH (caída silenciosa, red cortada...) el receive salta y se reconecta
            conn = socket.create_connection(addr_monitor, timeout=TIMEOUT_MONITOR)
            saludar(conn)
        except OSError:
            print(f"\n[MONITOR] No se puede conectar a {addr_monitor}, reintentando en {REINTENTO_MONITOR}s")
            time.sleep(REINTENTO_MONITOR)
            continue

        print(f"\n[MONITOR] Conectado al monitor en {addr_monitor}")
        monitor_conectado = True
        conectado = True
        while conectado:
            try:
                mensaje = receive(conn) # recibe el mensaje del monitor
            except socket.timeout:
                print(f"\n[MONITOR] {TIMEOUT_MONITOR}s sin noticias del monitor")
                mensaje = None
            except OSError:
                mensaje = None

            if mensaje is None:
                conectado = False
            elif mensaje.startswith("ID#"):
                id_ws = mensaje.split('#')[1]
                print(f"[MONITOR] ID asignado a este engine: {id_ws}")
                id_asignado.set()
            elif mensaje == HEALTH:
                try:
                    send(conn, estado_engine) # estado de saludo del engine
                except OSError:
                    conectado = False
            else:
                print(f"\n[MONITOR] Mensaje desconocido: {mensaje}")

        despedirse(conn)
        monitor_conectado = False
        print("\n[ESTADO] Estación AVERIADA: sin monitor no se aceptan riegos")
        if parar_riego(FIN_SIN_MONITOR):
            print("[AVERÍA] Riego en curso cortado: nadie vigila la estación")
        print(f"[MONITOR] Conexión con el monitor perdida, reintentando en {REINTENTO_MONITOR}s")
        time.sleep(REINTENTO_MONITOR)


def mostrar_menu():
    print("Comandos:")
    print("  R <segundos> = iniciar riego manual\n")
    print("  P = parar riego\n")
    print("  K = provocar avería (KO)\n")
    print("  O = resolver avería (OK)\n")
    print("  FIN = salir")


def main():
    global estado_engine
    num_argumentos = len(sys.argv)
    if num_argumentos != 5:
        print("Uso: python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>")
        print("Ejemplo: python WM_WS_E.py 127.0.0.1 9092 127.0.0.1 8000")
    else:
        ip_broker = sys.argv[1]
        puerto_broker = int(sys.argv[2])
        ip_monitor = sys.argv[3]
        puerto_monitor = int(sys.argv[4])
        addr_monitor = (ip_monitor, puerto_monitor)

        # Hilos del monitor y de Kafka. daemon=True para que mueran con FIN.
        hilo_monitor = threading.Thread(target=atender_monitor, args=(addr_monitor,), daemon=True)
        hilo_monitor.start()
        hilo_kafka = threading.Thread(target=conectar_kafka, args=(f"{ip_broker}:{puerto_broker}",), daemon=True)
        hilo_kafka.start()

        # El hilo principal se queda leyendo el teclado
        mostrar_menu()
        salir = False
        while not salir:
            try:
                partes = input().strip().split()
            except EOFError:    # sin teclado (p. ej. docker sin -it): se apaga como con FIN
                partes = [FIN]
            comando = partes[0].upper() if partes else ""

            if comando == "K":
                estado_engine = KO
                print("[AVERÍA] Fuga simulada: se responderá KO al monitor")
                # Una fuga durante el riego lo corta inmediatamente (punto 9)
                if parar_riego(FIN_FUGA):
                    print("[AVERÍA] Riego en curso cortado por la fuga")
            elif comando == "O":
                estado_engine = OK
                print("[AVERÍA] Fuga resuelta: se responderá OK al monitor")
            elif comando == "R":
                if len(partes) != 2 or not partes[1].isdigit() or int(partes[1]) <= 0:
                    print("Uso: R <segundos>   (ejemplo: R 30)")
                else:
                    ok, motivo = iniciar_riego(OPERARIO_LOCAL, int(partes[1]))
                    if not ok:
                        print(f"[RIEGO] No se puede regar: {motivo}")
            elif comando == "P":
                if not parar_riego(FIN_MANUAL):
                    print("[RIEGO] No hay ningún riego en curso")
            elif comando == FIN:
                with lock_riego:
                    r = riego
                if parar_riego(FIN_APAGADO):
                    r["hilo"].join(6)   # deja que se envíe el RIEGO_FIN y se borre el fichero antes de salir
                if productor is not None:
                    productor.flush(5)  # que el RIEGO_FIN llegue a la central antes de salir
                print("[DESCONEXIÓN] Apagando engine.")
                salir = True
            elif comando:
                print("Comando no válido.")
                mostrar_menu()


########## MAIN ##########

print("****** WM_WS_E - ENGINE DE ESTACIÓN DE RIEGO ****")
main()
