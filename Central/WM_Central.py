import socket
import threading
import sys
import sqlite3
import tkinter as tk
from tkinter import ttk


HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

# Disponible, Regando, Fuga, Fuera de servicio, Desconectada
ESTADOS_WS = ("AVAILABLE", "WATERING", "LEAK", "STOPPED", "OFFLINE")

# Texto y color con que se muestra cada estado en el panel
TEXTO_ESTADO = {
    "AVAILABLE": "Disponible",
    "WATERING": "Regando",
    "LEAK": "Fuga",
    "STOPPED": "Fuera de Servicio",
    "OFFLINE": "Desconectada"
}
COLOR_ESTADO = {
    "AVAILABLE": "#4caf50",     # verde
    "WATERING": "#4caf50",      # verde (parpadea alternando con WATERING_OFF)
    "WATERING_OFF": "#c8e6c9",  # verde claro
    "LEAK": "#e53935",          # rojo
    "STOPPED": "#fb8c00",       # naranja
    "OFFLINE": "#9e9e9e"        # gris
}

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

# Diccionario en memoria con todas las estaciones. Lo usan varios hilos a la vez
# (uno por cada monitor conectado), así que se protege con un lock.
estaciones = {}
lock_estaciones = threading.Lock()

def cargar_estaciones_bd():
    # Al arrancar no sabemos el estado real de ninguna estación hasta que se
    # conecte, así que todas las registradas pasan a OFFLINE (punto 1 de la mecánica)
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()

    cursor.execute("""
        UPDATE estaciones
        SET Status = 'OFFLINE', Flow = NULL, Accum = NULL, Operator = NULL
    """)
    conexion.commit()

    cursor.execute("SELECT ID, Location FROM estaciones")
    for id_estacion, ubicacion in cursor.fetchall():
        estaciones[id_estacion] = {
            "id": id_estacion,
            "ubicacion": ubicacion,
            "estado": "OFFLINE",
            "addr": None,
            "conn": None
        }

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
            SET Status = ?, Location = ?
            WHERE ID = ?
        """, (estado, ubicacion, id_estacion))
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

def registrar_estacion(id_estacion, ubicacion, conn, addr):
    # Autenticación: se rechaza un ID que ya tiene un monitor conectado.
    # Comprobar y dar de alta va dentro del lock para que dos monitores con el
    # mismo ID que lleguen a la vez no puedan pasar los dos.
    with lock_estaciones:
        if id_estacion in estaciones and estaciones[id_estacion]["conn"] is not None:
            return False

        estaciones[id_estacion] = {
            "id": id_estacion,
            "ubicacion": ubicacion,
            "estado": "AVAILABLE",
            "addr": addr,
            "conn": conn
        }
        guardar_estacion_bd(estaciones, id_estacion)
    return True

def desconectar_estacion(id_estacion):
    with lock_estaciones:
        estaciones[id_estacion]["estado"] = "OFFLINE"
        estaciones[id_estacion]["conn"] = None
        estaciones[id_estacion]["addr"] = None
        modificar_estado(id_estacion, "OFFLINE")

def send_msg(conn, msg):
    message = msg.encode(FORMAT)
    header = f"{len(message):<{HEADER}}".encode(FORMAT)
    conn.sendall(header + message)

def recive_msg(sock, size):
    # recv puede devolver menos bytes de los pedidos: se repite hasta tenerlos todos
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            return None
        data.extend(part)
    return bytes(data)

def receive_msg(conn):
    header = recive_msg(conn, HEADER)
    if header is None:
        return None

    msg_length = int(header.decode(FORMAT).strip())
    message = recive_msg(conn, msg_length)
    if message is None:
        return None

    return message.decode(FORMAT)

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    id_estacion_actual = None
    connected = True
    while connected:
            try:
                msg = receive_msg(conn)
            except (OSError, ValueError):  # conexión cortada o cabecera corrupta
                msg = None

            if msg is None:
                connected = False
            elif msg == FIN:
                connected = False
                print(f"[FIN] La estación '{id_estacion_actual}' ha enviado FIN")
            else:
                partes = msg.split('#')
                comando = partes[0]

                if comando == "REGISTER" and len(partes) >= 3 and id_estacion_actual is None:
                    id_estacion = partes[1]
                    ubicacion = partes[2]

                    if registrar_estacion(id_estacion, ubicacion, conn, addr):
                        id_estacion_actual = id_estacion
                        print(f"\n[REGISTRO OK] Estación '{id_estacion}' ({ubicacion}) registrada con éxito desde {addr}")
                        send_msg(conn, f"ACK#REGISTER_OK#{id_estacion}")
                        mostrar_bd()
                    else:
                        print(f"\n[REGISTRO KO] '{id_estacion}' ya está conectada, se rechaza {addr}")
                        send_msg(conn, f"NACK#REGISTER_KO#{id_estacion}#ID ya conectado")
                        connected = False

                elif comando == "ESTADO" and len(partes) >= 3:
                    id_estacion = partes[1]
                    nuevo_estado = partes[2]

                    # Un monitor solo puede cambiar el estado de su propia estación
                    if id_estacion == id_estacion_actual and nuevo_estado in ESTADOS_WS:
                        with lock_estaciones:
                            estaciones[id_estacion]["estado"] = nuevo_estado
                            modificar_estado(id_estacion, nuevo_estado)
                        print(f"[ESTADO] Estación '{id_estacion}' -> {nuevo_estado}")
                        send_msg(conn, f"ACK#ESTADO_OK#{id_estacion}#{nuevo_estado}")
                        mostrar_bd()
                    else:
                        print(f"[ESTADO] Mensaje de estado no válido: {msg}")
                        send_msg(conn, f"NACK#ESTADO_KO#{id_estacion}")

                else:
                    print(f"[CENTRAL] Recibido de [{id_estacion_actual or addr}]: {msg}")
                    respuesta = f"ACK#OK#Recibido: {msg}"
                    send_msg(conn, respuesta)

    # Tanto si llega FIN como si se corta la conexión, la estación queda desconectada
    if id_estacion_actual is not None:
        desconectar_estacion(id_estacion_actual)
        print(f"[DESCONEXION] Estación '{id_estacion_actual}' marcada como OFFLINE")
        mostrar_bd()

    conn.close()



def start(server, ip, port):
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 2}")

def Pantalla_Monitor():
    ventana = tk.Tk()
    ventana.title("WM_Central - Monitor de Estaciones de Riego")
    ventana.geometry("1100x600")

    # El tema por defecto de Windows ignora el color de fondo de las filas; "clam" sí lo respeta
    estilo = ttk.Style()
    estilo.theme_use("clam")

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

    # Una etiqueta (tag) por estado con su color de fondo
    for estado, color in COLOR_ESTADO.items():
        tabla.tag_configure(estado, background=color)

    scrollbar = ttk.Scrollbar(frame_izq, orient=tk.VERTICAL, command=tabla.yview)
    tabla.configure(yscroll=scrollbar.set)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    tabla.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Alterna en cada refresco para que las estaciones regando parpadeen
    parpadeo = {"encendido": True}

    # Función interna para refrescar los datos automáticamente
    def cargar_datos_bd():
        for fila in tabla.get_children():
            tabla.delete(fila)

        parpadeo["encendido"] = not parpadeo["encendido"]

        try:
            conn = sqlite3.connect("database.db")
            cur = conn.cursor()
            cur.execute("SELECT ID, Location, Status, Flow, Accum, Operator FROM estaciones")
            filas = cur.fetchall()
            for fila in filas:
                estado = fila[2]
                fila_limpia = [valor if valor is not None else "-" for valor in fila]
                fila_limpia[2] = TEXTO_ESTADO.get(estado, estado)

                tag = estado
                if estado == "WATERING" and not parpadeo["encendido"]:
                    tag = "WATERING_OFF"

                tabla.insert("", tk.END, values=fila_limpia, tags=(tag,))
            conn.close()
        except Exception as e:
            print(f"[ERROR BD] No se pudieron refrescar los datos: {e}")

        # Re-ejecutar cada 500 milisegundos (también marca el ritmo del parpadeo)
        ventana.after(500, cargar_datos_bd)

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

if len(sys.argv) == 2:
    cargar_estaciones_bd()
    print(f"[BD] {len(estaciones)} estaciones cargadas de la base de datos (todas en OFFLINE)")
    mostrar_bd()

    PORT = int(sys.argv[1])
    SERVER = "0.0.0.0"
    ADDR = (SERVER, PORT)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)

    print(f"[STARTING] Iniciando WM_Central en {SERVER}:{PORT}...")
    hilo_servidor = threading.Thread(target=start, args=(server, SERVER, PORT), daemon=True)
    hilo_servidor.start()
    Pantalla_Monitor()
else:
    print("Uso: python WM_Central.py <puerto_escucha>")
    print("Ejemplo: python WM_Central.py 9999")

conexion.close()
