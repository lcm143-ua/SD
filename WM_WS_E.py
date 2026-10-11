import socket
import sys
import threading
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

# Protocolo Monitor <-> Engine (el mismo que en WM_WS_M)
HEALTH = "HEALTH"       # pregunta del monitor cada segundo
OK = "OK"               # todo funciona
KO = "KO"               # avería simulada (fuga)
REINTENTO_MONITOR = 3   # segundos entre intentos de conexión al monitor

# Estado de salud que se responde al monitor. Lo cambia el teclado (hilo principal)
# y lo lee el hilo del monitor; es una simple asignación, no hace falta lock.
estado_engine = OK
id_ws = None


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

        # El hilo principal se queda leyendo el teclado para simular averías
        print("Comandos: K = provocar avería (KO) | O = resolver avería (OK) | FIN = salir")
        salir = False
        while not salir:
            tecla = input().strip().upper()

            if tecla == "K":
                estado_engine = KO
                print("[AVERÍA] Fuga simulada: se responderá KO al monitor")
            elif tecla == "O":
                estado_engine = OK
                print("[AVERÍA] Fuga resuelta: se responderá OK al monitor")
            elif tecla == FIN:
                print("[DESCONEXIÓN] Apagando engine.")
                salir = True
            elif tecla:
                print("Comando no válido. Usa K, O o FIN")


########## MAIN ##########

print("****** WM_WS_E - ENGINE DE ESTACIÓN DE RIEGO ****")
main()
