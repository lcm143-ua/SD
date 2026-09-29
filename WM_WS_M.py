import socket
import sys

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

def send(client, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

def receive(client):
    msg_length = client.recv(HEADER).decode(FORMAT)
    if msg_length:
        msg_length = int(msg_length)
        return client.recv(msg_length).decode(FORMAT)
    return None
    
########## MAIN ##########


print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2020/2021 SOCKET CLIENT ****")

if  (len(sys.argv) >= 5):
    PUERTO_ENGINE = int(sys.argv[1])
    SERVER_CENTRAL = sys.argv[2]
    PORT_CENTRAL = int(sys.argv[3])
    ID_WS = sys.argv[4]
    UBICACION = sys.argv[5]

    ADDR = (SERVER_CENTRAL, PORT_CENTRAL)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

    msg_registro = f"REGISTER#{ID_WS}#{UBICACION}"
    print(f"[REGISTRO] Enviando solicitud de registro: {msg_registro}")
    send(client, msg_registro)

    confirmacion = receive(client)
    print(f"[RESPUESTA CENTRAL]: {confirmacion}\n")

    msg = ""
    while msg != FIN :
        msg = input("Introduce comando/estado o 'FIN' para salir: ")
        if msg.strip():
            send(client, msg)
            if msg != FIN:
                resp = receive(client)
                print(f"[RESPUESTA CENTRAL]: {resp}\n")

    print("[DESCONEXION] Enviando cierre a WM_Central...")
    client.close()

else:
    print("Uso correcto: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <id_ws> <ubicacion>")
    print("Ejemplo: python WM_WS_M.py 127.0.0.1 9999 WS_01#Parque_Central")