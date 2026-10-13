import socket
import sys
import threading
import time
import random

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

# Protocolo Monitor <-> Engine (el mismo que en WM_WS_M)
HEALTH = "HEALTH"       # pregunta del monitor cada segundo
OK = "OK"               # todo funciona
KO = "KO"               # avería simulada (fuga)
REINTENTO_MONITOR = 3   # segundos entre intentos de conexión al monitor

# Simulación del caudalímetro (litros/minuto)
CAUDAL_MIN = 15.0
CAUDAL_MAX = 25.0
OPERARIO_LOCAL = "LOCAL"    # riego activado desde el menú de la propia estación

# Motivos por los que termina un riego
FIN_TIEMPO = "TIEMPO"       # se alcanzó el límite de tiempo
FIN_MANUAL = "MANUAL"       # comando P en el menú de la estación
FIN_FUGA = "FUGA"           # avería durante el riego
FIN_APAGADO = "APAGADO"     # el engine se apaga con FIN
FIN_BLOQUEO = "BLOQUEO"     # orden BLOQUEAR de la central (llegará por Kafka)

# Estado de salud que se responde al monitor. Lo cambia el teclado (hilo principal)
# y lo lee el hilo del monitor; es una simple asignación, no hace falta lock.
estado_engine = OK
id_ws = None

# Riego en curso (None si la electroválvula está cerrada). Lo usan el hilo del
# teclado y el hilo del riego, así que se protege con un lock.
riego = None
lock_riego = threading.Lock()


def send(client, msg):
    message = msg.encode(FORMAT)
    header = f"{len(message):<{HEADER}}".encode(FORMAT)
    client.sendall(header + message)


def recive_msg(sock, size):
    solu = None
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size-len(data))

        if not part:
            return solu
        data.extend(part)

    return bytes(data)

def receive(client):
    solu = None
    header = recive_msg(client, HEADER)
    if header == solu:
        return solu

    msg_length = int(header.decode(FORMAT).strip())
    message = recive_msg(client, msg_length)

    if message == solu:
        return solu

    return message.decode(FORMAT)


def publicar_evento(mensaje):
    # PENDIENTE KAFKA: aquí se publicará el mensaje para WM_Central (y el operario).
    # Formatos ya definidos:
    #   RIEGO_INICIO#id_ws#operario#limite_segundos
    #   TELEMETRIA#id_ws#caudal#volumen_acumulado
    #   RIEGO_FIN#id_ws#operario#volumen_total#duracion#motivo
    # De momento el riego solo se muestra en la pantalla del engine.
    pass


def iniciar_riego(operario, limite):
    # Abre la electroválvula si la estación puede regar. Devuelve (ok, motivo).
    # La usa el menú local y la usará la orden REGAR que llegue de la central.
    global riego
    with lock_riego:
        if id_ws is None:
            return False, "la estación aún no está autenticada (sin ID del monitor)"
        if estado_engine == KO:
            return False, "hay una avería (fuga) detectada"
        if riego is not None:
            return False, "ya hay un riego en curso"

        riego = {
            "operario": operario,
            "limite": limite,
            "inicio": time.time(),
            "volumen": 0.0,
            "caudal": 0.0,
            "motivo_fin": None,
            "despertar": threading.Event()  # permite cortar el riego sin esperar al segundo
        }
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


def bucle_riego(r):
    # Cada segundo mide el caudal, acumula el volumen y comprueba si hay que parar
    global riego
    terminado = False
    while not terminado:
        r["despertar"].wait(1)

        with lock_riego:
            if r["motivo_fin"] is None and estado_engine == KO:
                r["motivo_fin"] = FIN_FUGA

            if r["motivo_fin"] is None:
                r["caudal"] = round(random.uniform(CAUDAL_MIN, CAUDAL_MAX), 2)
                r["volumen"] += r["caudal"] / 60     # l/min durante 1 segundo
                transcurrido = time.time() - r["inicio"]
                print(f"[RIEGO] Caudal: {r['caudal']:.2f} l/min | Acumulado: {r['volumen']:.2f} l | {int(transcurrido)}/{r['limite']} s")
                publicar_evento(f"TELEMETRIA#{id_ws}#{r['caudal']:.2f}#{r['volumen']:.2f}")
                if transcurrido >= r["limite"]:
                    r["motivo_fin"] = FIN_TIEMPO

            terminado = r["motivo_fin"] is not None

    duracion = int(time.time() - r["inicio"])
    with lock_riego:
        riego = None

    print(f"\n[RIEGO] Electroválvula CERRADA. Motivo: {r['motivo_fin']}")
    print(f"[RESUMEN] Operario: {r['operario']} | Volumen total: {r['volumen']:.2f} l | Duración: {duracion} s")
    publicar_evento(f"RIEGO_FIN#{id_ws}#{r['operario']}#{r['volumen']:.2f}#{duracion}#{r['motivo_fin']}")


def atender_monitor(addr_monitor):
    # Se conecta al monitor y responde a cada HEALTH con el estado actual.
    # Si el monitor se cae, vuelve a intentarlo cada REINTENTO_MONITOR segundos.
    global id_ws
    while True:
        try:
            conn = socket.create_connection(addr_monitor)
        except OSError:
            print(f"\n[MONITOR] No se puede conectar a {addr_monitor}, reintentando en {REINTENTO_MONITOR}s")
            time.sleep(REINTENTO_MONITOR)
            continue

        print(f"\n[MONITOR] Conectado al monitor en {addr_monitor}")
        conectado = True
        while conectado:
            try:
                mensaje = receive(conn)
            except OSError:
                mensaje = None

            if mensaje is None:
                conectado = False
            elif mensaje.startswith("ID#"):
                id_ws = mensaje.split('#')[1]
                print(f"[MONITOR] ID asignado a este engine: {id_ws}")
            elif mensaje == HEALTH:
                try:
                    send(conn, estado_engine)
                except OSError:
                    conectado = False
            else:
                print(f"\n[MONITOR] Mensaje desconocido: {mensaje}")

        conn.close()
        print(f"\n[MONITOR] Conexión con el monitor perdida, reintentando en {REINTENTO_MONITOR}s")
        time.sleep(REINTENTO_MONITOR)


def mostrar_menu():
    print("Comandos:")
    print("  R <segundos> = iniciar riego manual   P = parar riego")
    print("  K = provocar avería (KO)   O = resolver avería (OK)   FIN = salir")


def main():
    global estado_engine
    num_argumentos = len(sys.argv)
    if num_argumentos != 5:
        print("Uso: python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>")
        print("Ejemplo: python WM_WS_E.py 127.0.0.1 9092 127.0.0.1 8000")
    else:
        # Los datos del broker se usarán cuando se añada Kafka
        ip_broker = sys.argv[1]
        puerto_broker = int(sys.argv[2])
        ip_monitor = sys.argv[3]
        puerto_monitor = int(sys.argv[4])
        addr_monitor = (ip_monitor, puerto_monitor)

        # Hilo que habla con el monitor. daemon=True para que muera con FIN.
        hilo_monitor = threading.Thread(target=atender_monitor, args=(addr_monitor,), daemon=True)
        hilo_monitor.start()

        # El hilo principal se queda leyendo el teclado
        mostrar_menu()
        salir = False
        while not salir:
            partes = input().strip().split()
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
                    r["hilo"].join(2)   # deja que se muestre el resumen antes de salir
                print("[DESCONEXIÓN] Apagando engine.")
                salir = True
            elif comando:
                print("Comando no válido.")
                mostrar_menu()


########## MAIN ##########

print("****** WM_WS_E - ENGINE DE ESTACIÓN DE RIEGO ****")
main()
