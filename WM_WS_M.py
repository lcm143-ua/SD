import socket
import sys
import threading
import time

FORMAT = 'utf-8'
FIN = "FIN"

# Protocolo de sockets recomendado en el enunciado: <STX><DATA><ETX><LRC>
#   - Al conectar, el cliente envía ENQ y el servidor contesta ACK (handshake).
#   - Cada trama recibida se confirma con ACK (LRC correcto) o NACK (corrupta);
#     con NACK el emisor la reenvía, como mucho MAX_REINTENTOS veces.
#   - Al cerrar de forma ordenada se envía EOT.
# Mensajería de los frofes
STX = b'\x02'
ETX = b'\x03'
EOT = b'\x04'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'
MAX_REINTENTOS = 3
MAX_TRAMA = 1024        # bytes de DATA como máximo; más largo se considera basura

# Protocolo Monitor <-> Engine
HEALTH = "HEALTH"       # el monitor lo envía cada segundo
OK = "OK"               # respuesta del engine si todo va bien
KO = "KO"               # respuesta del engine si detecta avería (fuga)
INTERVALO_HEALTH = 1    # segundos entre HEALTH
TIMEOUT_ENGINE = 3      # segundos sin respuesta (o sin conexión) del engine = avería

# Conexión Monitor <-> Central
INTERVALO_LATIDO = 2    # cada cuánto se envía PING a la central si no hay nada que contar
TIMEOUT_CENTRAL = 5     # segundos esperando una respuesta de la central
REINTENTO_CENTRAL = 3   # segundos entre intentos de reconexión

# Estados que el monitor comunica a la central (mismos nombres que en WM_Central)
AVAILABLE = "AVAILABLE"
LEAK = "LEAK"

# Socket con la central (None mientras está caída). Lo usan el hilo del menú y el
# hilo que mantiene la conexión; el lock garantiza que cada send va seguido de SU
# receive sin mezclarse.
central = None
lock_central = threading.Lock()

# estado_ws lo escribe el hilo del engine; el hilo de la central lo comunica.
# Así el bucle de HEALTH nunca se queda bloqueado esperando a la central.
estado_ws = AVAILABLE
cambio_estado = threading.Event()
saliendo = threading.Event()


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


def aceptar_saludo(sock):
    # Handshake del lado servidor: espera ENQ y contesta ACK
    if recibir_bytes(sock, 1) == ENQ:
        sock.sendall(ACK)
        return True
    return False


def despedirse(sock):
    # Cierre ordenado: EOT y close (si el otro ya cerró, da igual)
    try:
        sock.sendall(EOT)
    except OSError:
        pass
    sock.close()


########## CONEXIÓN CON LA CENTRAL ##########

def registrarse(addr, id_ws, ubicacion):
    # Conecta, hace el handshake y envía REGISTER.
    # Devuelve (socket, respuesta) si la central acepta o (None, respuesta) si rechaza.
    # Si la central no está accesible lanza OSError.
    sock = socket.create_connection(addr, timeout=TIMEOUT_CENTRAL)
    try:
        saludar(sock)
        send(sock, f"REGISTER#{id_ws}#{ubicacion}")
        respuesta = receive(sock)
    except OSError:
        sock.close()
        raise

    if respuesta is None or not respuesta.startswith("ACK#REGISTER_OK"):
        despedirse(sock)
        return None, respuesta
    return sock, respuesta


def hablar_central(msg):
    # Envía un mensaje a la central y devuelve su respuesta (None si no hay conexión).
    # Si la conexión falla se da por perdida y el hilo de la central la recupera.
    global central
    with lock_central:
        if central is None:
            return None
        try:
            send(central, msg)
            respuesta = receive(central)
        except OSError:  # incluye socket.timeout y conexión reseteada
            respuesta = None
        if respuesta is None:
            central.close()
            central = None
        return respuesta


def mantener_central(addr, id_ws, ubicacion):
    # Hilo que se ocupa de la conexión con la central:
    #   - comunica los cambios de estado (solo transiciones)
    #   - si no hay nada que contar envía un PING para que la central sepa que seguimos vivos
    #   - si la conexión se pierde, se reconecta y vuelve a registrarse
    global central
    estado_notificado = AVAILABLE   # la central nos deja en AVAILABLE al registrarnos
    caida_avisada = False

    while not saliendo.is_set():
        cambio_estado.wait(INTERVALO_LATIDO)
        if saliendo.is_set():
            break

        if central is None:
            if not caida_avisada:
                print(f"\n[CENTRAL] Conexión perdida, reintentando cada {REINTENTO_CENTRAL}s")
                caida_avisada = True
            try:
                sock, respuesta = registrarse(addr, id_ws, ubicacion)
            except OSError:
                sock, respuesta = None, None
            if sock is None:
                if respuesta is not None:
                    print(f"[CENTRAL] Registro rechazado: {respuesta}")
                time.sleep(REINTENTO_CENTRAL)
            else:
                with lock_central:
                    central = sock
                print(f"\n[CENTRAL] Reconectado. [RESPUESTA CENTRAL]: {respuesta}")
                estado_notificado = AVAILABLE
                caida_avisada = False
            continue

        cambio_estado.clear()
        estado = estado_ws
        if estado != estado_notificado:
            respuesta = hablar_central(f"ESTADO#{id_ws}#{estado}")
            if respuesta is not None:
                estado_notificado = estado
                print(f"[RESPUESTA CENTRAL]: {respuesta}")
        else:
            hablar_central(f"PING#{id_ws}")


def cambiar_estado(id_ws, nuevo_estado):
    # Solo se avisa a la central cuando el estado cambia de verdad (transición).
    # El aviso lo envía el hilo de la central, para no frenar el bucle de HEALTH.
    global estado_ws
    if nuevo_estado != estado_ws:
        estado_ws = nuevo_estado
        print(f"\n[ESTADO] {id_ws} pasa a {nuevo_estado}")
        cambio_estado.set()


########## CONEXIÓN CON EL ENGINE ##########

def vigilar_engine(conn_engine, id_ws): # vigila la salud del engine (KO o silencio = fuga)
    conn_engine.settimeout(TIMEOUT_ENGINE)
    conectado = True

    # Lo primero: handshake y decirle al engine qué ID tiene (ya validado por la central)
    try:
        if aceptar_saludo(conn_engine):
            send(conn_engine, f"ID#{id_ws}")
        else:
            print("\n[ENGINE] La conexión no empezó con ENQ, se descarta")
            conectado = False
    except OSError:
        conectado = False

    while conectado:
        try:
            send(conn_engine, HEALTH)
            respuesta = receive(conn_engine)
        except OSError:  # incluye socket.timeout y conexión reseteada
            respuesta = None

        if respuesta == OK:
            cambiar_estado(id_ws, AVAILABLE)
        elif respuesta is None:
            print("\n[ENGINE] El engine no responde o se ha desconectado")
            cambiar_estado(id_ws, LEAK)
            conectado = False
        else:
            print(f"\n[ENGINE] Respuesta de avería recibida: {respuesta}")
            cambiar_estado(id_ws, LEAK)

        if conectado:
            time.sleep(INTERVALO_HEALTH)

    despedirse(conn_engine)


def servidor_engine(server, puerto_engine, id_ws):
    # Servidor de sockets que el monitor ofrece a WM_WS_E (un engine a la vez).
    # Si el engine no llega en TIMEOUT_ENGINE segundos también se considera avería.
    server.settimeout(TIMEOUT_ENGINE)
    print(f"[ENGINE] Esperando conexión del engine en el puerto {puerto_engine}")

    while True:
        try:
            conn_engine, addr_engine = server.accept()
        except socket.timeout:
            if estado_ws != LEAK:
                print(f"\n[ENGINE] Ningún engine conectado en {TIMEOUT_ENGINE}s")
                cambiar_estado(id_ws, LEAK)
            continue

        print(f"\n[ENGINE] Engine conectado desde {addr_engine}")
        vigilar_engine(conn_engine, id_ws)
        print(f"[ENGINE] Esperando reconexión del engine en el puerto {puerto_engine}")


########## MAIN ##########

def main():
    global central
    if len(sys.argv) != 6 or not sys.argv[1].isdigit() or not sys.argv[3].isdigit():
        print("Uso: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> <ubicacion>")
        print('Ejemplo: python WM_WS_M.py 8000 127.0.0.1 9999 WS_01 "Bosque_Dioses"')
        return

    puerto_engine = int(sys.argv[1])
    addr = (sys.argv[2], int(sys.argv[3]))
    id_ws = sys.argv[4]
    ubicacion = sys.argv[5]

    if '#' in id_ws or '#' in ubicacion:
        print("El ID y la ubicación no pueden contener '#' (es el separador del protocolo)")
        return

    # Se abre antes el puerto del engine: si está ocupado no tiene sentido registrarse
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(("0.0.0.0", puerto_engine))
    except OSError as e:
        print(f"No se puede abrir el puerto {puerto_engine} para el engine: {e}")
        return
    server.listen(1)

    # La central puede no estar arrancada todavía: se reintenta hasta que conteste.
    # Un rechazo explícito (p. ej. ID ya conectado) sí termina el monitor.
    sock = None
    while sock is None:
        try:
            sock, respuesta = registrarse(addr, id_ws, ubicacion)
        except OSError:
            print(f"No se pudo conectar con la central en {addr}, reintentando en {REINTENTO_CENTRAL}s")
            time.sleep(REINTENTO_CENTRAL)
            continue
        if sock is None:
            print(f"Registro rechazado o conexión cerrada por la central: {respuesta}")
            server.close()
            return

    central = sock
    print(f"Conexión establecida con {addr}")
    print(f"[RESPUESTA CENTRAL]: {respuesta}")

    # Hilos del engine y de la central. daemon=True para que mueran con FIN.
    threading.Thread(target=servidor_engine, args=(server, puerto_engine, id_ws), daemon=True).start()
    threading.Thread(target=mantener_central, args=(addr, id_ws, ubicacion), daemon=True).start()

    salir = False
    while not salir:
        try:
            msg = input("Introduce comando/estado o 'FIN' para salir: ").strip()
        except EOFError:
            msg = FIN

        if msg == FIN:
            saliendo.set()
            cambio_estado.set()     # despierta al hilo de la central para que termine
            with lock_central:
                if central is not None:
                    try:
                        send(central, FIN)
                    except OSError:
                        pass
                    despedirse(central)
                    central = None
            print("[DESCONEXIÓN] Cerrando conexión.")
            salir = True
        elif msg:
            respuesta = hablar_central(msg)
            if respuesta is None:
                print("[CENTRAL] Sin conexión con la central (se está reintentando en segundo plano)")
            else:
                print(f"[RESPUESTA CENTRAL]: {respuesta}")

    server.close()


print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2026/2027 SOCKET CLIENT ****")
main()
