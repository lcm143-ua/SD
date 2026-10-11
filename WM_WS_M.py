import socket
import sys

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

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

########## MAIN ##########


print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2026/2027 SOCKET CLIENT ****")
'''
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
    '''
    
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
        
        client.connect(addr)
        print(f"Conexión establecida con {addr}")
        
        msg_registro = f"REGISTER#{id_ws}#{ubicacion}"
        send(client, msg_registro)
        
        confirmacion = receive(client)
        
        if confirmacion == solu:
            print("La central cerró la conexión durante el registro.")
        
        
        print(f"[RESPUESTA CENTRAL]: {confirmacion}")
        
        while solu2:
            msg = input("Introduce comando/estado o 'FIN' para salir: ").strip()
            
            if not msg:
                continue 
            
            send(client, msg)
            
            if msg == FIN:
                print("[DESCONEXIÓN] Cerrando conexión.")
                break 
            
            respuesta = receive(client)
            if respuesta is solu:
                print("[DESCONEXIÓ] La central cerró la conexión")
                break 
            
            print(f"[RESPUESTA CENTRAL]: {respuesta}")
            
        client.close()