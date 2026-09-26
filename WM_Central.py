import socket 
import threading
import sys


HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

estaciones = {}

def send_msg(conn, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    conn.send(send_length)
    conn.send(message)

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    id_estacion_actual = None
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            else:

                partes = msg.split('#')
                comando = partes[0]

                if comando == "REGISTER" and len(partes) >= 3:
                    id_estacion = partes[1]
                    ubicacion = partes[2]
                    id_estacion_actual = id_estacion

                    estaciones[id_estacion] = {
                        "id": id_estacion,
                        "ubicacion": ubicacion,
                        "estado": "AVAILABLE",
                        "addr": addr,
                        "conn": conn
                    }
                    print(f"\n[REGISTRO OK] Estación '{id_estacion}' ({ubicacion}) registrada con éxito desde {addr}")
                    print(f"[ESTACIONES REGISTRADAS EN MEMORIA]: {list(estaciones.keys())}\n")

                    respuesta = f"ACK#REGISTER_OK#{id_estacion}"
                    send_msg(conn, respuesta)

                else:
                    print(f"[CENTRAL] Recibido de [{id_estacion_actual or addr}]: {msg}")
                    respuesta = f"ACK#OK#Recibido: {msg}"
                    send_msg(conn, respuesta)

    print(f"[DESCONEXION] Cierre de socket {addr}")
    if id_estacion_actual and id_estacion_actual in estaciones:
        print(f"[CENTRAL] Cambiando estado de '{id_estacion_actual}' a DESCONECTADA")
        estaciones[id_estacion_actual]["estado"] = "DESCONECTADA"
    
    conn.close()
    
        

def start(server, ip, port):
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 1}")
        

######################### MAIN ##########################

if len(sys.argv) == 2:
    PORT = int(sys.argv[1])
    SERVER = "0.0.0.0"
    ADDR = (SERVER, PORT)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

    print(f"[STARTING] Iniciando WM_Central en {SERVER}:{PORT}...")
    start(server, SERVER, PORT)
else:
    print("Error")

