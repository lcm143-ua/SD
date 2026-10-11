import socket
import sys
import threading
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

# Protocolo Monitor <-> Engine
HEALTH = "HEALTH"       # el monitor lo envía cada segundo
OK = "OK"               # respuesta del engine si todo va bien
KO = "KO"               # respuesta del engine si detecta avería (fuga)
TIMEOUT_ENGINE = 3      # segundos sin respuesta del engine = avería

# Estados que el monitor comunica a la central (mismos nombres que en WM_Central)
AVAILABLE = "AVAILABLE"
LEAK = "LEAK"

# El socket con la central lo usan dos hilos (menú y vigilancia del engine).
# El lock garantiza que cada send va seguido de SU receive sin mezclarse.
lock_central = threading.Lock()
estado_ws = AVAILABLE


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


def hablar_central(client, msg):
    # Envía un mensaje a la central y devuelve su respuesta (None si se ha caído)
    with lock_central:
        try:
            send(client, msg)
            return receive(client)
        except OSError:
            return None


def cambiar_estado(client, id_ws, nuevo_estado):
    # Solo se avisa a la central cuando el estado cambia de verdad (transición)
    global estado_ws
    if nuevo_estado != estado_ws:
        estado_ws = nuevo_estado
        print(f"\n[ESTADO] {id_ws} pasa a {nuevo_estado}")
        respuesta = hablar_central(client, f"ESTADO#{id_ws}#{nuevo_estado}")
        if respuesta is None:
            print("[ERROR] No se pudo notificar el estado a la central")
        else:
            print(f"[RESPUESTA CENTRAL]: {respuesta}")


def vigilar_engine(conn_engine, client, id_ws):
    # Cada segundo pregunta al engine por su salud. KO o silencio = fuga.
    conn_engine.settimeout(TIMEOUT_ENGINE)
    conectado = True

    # Lo primero es decirle al engine qué ID tiene (ya validado por la central)
    try:
        send(conn_engine, f"ID#{id_ws}")
    except OSError:
        conectado = False

    while conectado:
        try:
            send(conn_engine, HEALTH)
            respuesta = receive(conn_engine)
        except OSError:  # incluye socket.timeout y conexión reseteada
            respuesta = None

        if respuesta == OK:
            cambiar_estado(client, id_ws, AVAILABLE)
        elif respuesta is None:
            print("\n[ENGINE] El engine no responde o se ha desconectado")
            cambiar_estado(client, id_ws, LEAK)
            conectado = False
        else:
            print(f"\n[ENGINE] Respuesta de avería recibida: {respuesta}")
            cambiar_estado(client, id_ws, LEAK)

        if conectado:
            time.sleep(1)

    conn_engine.close()


def servidor_engine(puerto_engine, client, id_ws):
    # Servidor de sockets que el monitor ofrece a WM_WS_E (un engine a la vez)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", puerto_engine))
    server.listen(1)
    print(f"[ENGINE] Esperando conexión del engine en el puerto {puerto_engine}")

    while True:
        conn_engine, addr_engine = server.accept()
        print(f"\n[ENGINE] Engine conectado desde {addr_engine}")
        vigilar_engine(conn_engine, client, id_ws)
        print(f"[ENGINE] Esperando reconexión del engine en el puerto {puerto_engine}")


def main():
    num_argumentos = len(sys.argv)
    solu = None
    solu2 = True
    if num_argumentos != 6:
        print("Uso: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> <ubicacion>")
        print('Ejemplo: python WM_WS_M.py 8000 127.0.0.1 9999 WS_01 "Bosque de los Dioses"')
    else:
        puerto_engine = int(sys.argv[1])
        server_central = sys.argv[2]
        puerto_central = int(sys.argv[3])
        id_ws = sys.argv[4]
        ubicacion = sys.argv[5]
        addr = (server_central, puerto_central)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            client.connect(addr)
        except OSError:
            print(f"No se pudo conectar con la central en {addr}")
            return
        print(f"Conexión establecida con {addr}")

        msg_registro = f"REGISTER#{id_ws}#{ubicacion}"
        send(client, msg_registro)

        confirmacion = receive(client)

        if confirmacion == solu or not confirmacion.startswith("ACK#REGISTER_OK"):
            print(f"Registro rechazado o conexión cerrada por la central: {confirmacion}")
            client.close()
            return

        print(f"[RESPUESTA CENTRAL]: {confirmacion}")

        # Hilo que atiende al engine y vigila su salud. daemon=True para que
        # muera solo cuando el monitor termine con FIN.
        hilo_engine = threading.Thread(target=servidor_engine, args=(puerto_engine, client, id_ws), daemon=True)
        hilo_engine.start()

        while solu2:
            msg = input("Introduce comando/estado o 'FIN' para salir: ").strip()

            if not msg:
                continue

            if msg == FIN:
                with lock_central:
                    send(client, msg)
                print("[DESCONEXIÓN] Cerrando conexión.")
                break

            respuesta = hablar_central(client, msg)
            if respuesta is solu:
                print("[DESCONEXIÓN] La central cerró la conexión")
                break

            print(f"[RESPUESTA CENTRAL]: {respuesta}")

        client.close()


########## MAIN ##########

print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2026/2027 SOCKET CLIENT ****")
main()
