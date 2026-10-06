import socket 
import threading
import sys
import sqlite3
import tkinter as tk
from tkinter import ttk


HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

conexion = sqlite3.connect("database.db")
cursor = conexion.cursor()

#Creación de todas las tablas
cursor.execute("""
    CREATE TABLE IF NOT EXISTS estaciones(
        ID TEXT PRIMARY KEY,
        Location TEXT NOT NULL,
        Status TEXT NOT NULL,
        Flow REAL,
        Accum INTERGER,
        Operator TEXT
    )
""")

#Guardar
conexion.commit()

estaciones = {}

def vaciar_bd():
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()

    cursor.execute("DELETE FROM estaciones")

    conexion.commit()
    conexion.close()

def guardar_estacion_bd(estaciones, id):
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()

    estacion = estaciones[id]

    id_estacion = estacion["id"]
    ubicacion = estacion ["ubicacion"]
    estado = estacion["estado"]

    cursor.execute("SELECT * FROM estaciones WHERE ID = ?", (id_estacion,))

    if cursor.fetchone() is not None:
        cursor.execute("""
            UPDATE estaciones
            SET Status = ?
            WHERE ID = ?
        """, (estado, id_estacion))
    else: 
        cursor.execute("""
            INSERT INTO estaciones (ID, Location, Status)
            VALUES (?,?,?)
            """, (id_estacion,ubicacion, estado)
        )
    conexion.commit()
    conexion.close()

def mostrar_bd():
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()

    cursor.execute("SELECT * FROM estaciones")

    estaciones_bd = cursor.fetchall()

    for estacion in estaciones_bd:
        print(estacion)

    conexion.close()

def modificar_estado(id, estado):
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()

    cursor.execute("""
        UPDATE estaciones
        SET Status = ?
        WHERE ID = ?
    """, (estado, id))

    conexion.commit()
    conexion.close()

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
            if not msg_length:
                connected = False
            else:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == FIN:
                    connected = False
                    print(f"[FIN] La estación '{id_estacion_actual}' ha enviado FIN")
                    modificar_estado(id_estacion_actual, "OFFLINE")
                    mostrar_bd()
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

                        guardar_estacion_bd(estaciones, id_estacion)

                        print(f"\n[REGISTRO OK] Estación '{id_estacion}' ({ubicacion}) registrada con éxito desde {addr}")
    
                        respuesta = f"ACK#REGISTER_OK#{id_estacion}"
                        send_msg(conn, respuesta)

                        mostrar_bd()
    
                    else:
                        print(f"[CENTRAL] Recibido de [{id_estacion_actual or addr}]: {msg}")
                        respuesta = f"ACK#OK#Recibido: {msg}"
                        send_msg(conn, respuesta)
    
    conn.close()
    
        

def start(server, ip, port):
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 1}")
        
def Pantalla_Monitor():
    ventana = tk.Tk()
    ventana.title("WM_Central - Monitor de Estaciones de Riego")
    ventana.geometry("1100x600")

    # 1. Frame Izquierdo: Tabla de Datos
    frame_izq = tk.Frame(ventana)
    frame_izq.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

    lbl_tabla = tk.Label(frame_izq, text="Estado de las Estaciones", font=("Arial", 14, "bold"))
    lbl_tabla.pack(pady=5)

    columnas = ("ID", "Location", "Status", "Flow", "Accum", "Operator")
    tabla = ttk.Treeview(frame_izq, columns=columnas, show="headings")

    for col in columnas:
        tabla.heading(col, text=col)
        tabla.column(col, width=90, anchor=tk.CENTER)

    scrollbar = ttk.Scrollbar(frame_izq, orient=tk.VERTICAL, command=tabla.yview)
    tabla.configure(yscroll=scrollbar.set)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    tabla.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Función interna para refrescar los datos automáticamente
    def cargar_datos_bd():
        for fila in tabla.get_children():
            tabla.delete(fila)

        try:
            conn = sqlite3.connect("database.db")
            cur = conn.cursor()
            cur.execute("SELECT ID, Location, Status, Flow, Accum, Operator FROM estaciones")
            filas = cur.fetchall()
            for fila in filas:
                fila_limpia = [valor if valor is not None else "-" for valor in fila]
                tabla.insert("", tk.END, values=fila_limpia)
            conn.close()
        except Exception as e:
            print(f"[ERROR BD] No se pudieron refrescar los datos: {e}")

        # Re-ejecutar cada 1000 milisegundos (1 segundo)
        ventana.after(1000, cargar_datos_bd)

    # 2. Frame Derecho: Imagen del Mapa
    frame_der = tk.Frame(ventana, bg="white", relief=tk.SUNKEN, bd=2)
    frame_der.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10, pady=10)

    lbl_mapa = tk.Label(frame_der, text="Mapa de Zonas", font=("Arial", 14, "bold"), bg="white")
    lbl_mapa.pack(pady=5)

    try:
        imagen_mapa = tk.PhotoImage(file="InvernaliaZonas.png")
        lbl_imagen = tk.Label(frame_der, image=imagen_mapa, bg="white")
        lbl_imagen.image = imagen_mapa
        lbl_imagen.pack(expand=True)
    except tk.TclError:
        lbl_error = tk.Label(frame_der, text="[Imagen no encontrada]\nAsegúrate de que 'InvernaliaZonas.png'\nestá en el mismo directorio.", bg="white", fg="red")
        lbl_error.pack(expand=True)

    # Iniciar la carga de datos y el bucle principal de la interfaz
    cargar_datos_bd()
    ventana.mainloop()

######################### MAIN ##########################

vaciar_bd()
print(f"Vaciado de la base de datos")

if len(sys.argv) == 2:
    PORT = int(sys.argv[1])
    SERVER = "0.0.0.0"
    ADDR = (SERVER, PORT)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

    print(f"[STARTING] Iniciando WM_Central en {SERVER}:{PORT}...")
    hilo_servidor = threading.Thread(target=start, args=(server, SERVER, PORT), daemon=True)
    hilo_servidor.start()
    Pantalla_Monitor()
else:
    print("Error")

conexion.close()