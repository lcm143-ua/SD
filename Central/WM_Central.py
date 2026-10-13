import socket
import threading
import sys
import sqlite3
import queue
import time
import tkinter as tk
from tkinter import ttk


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

TIMEOUT_MONITOR = 10    # segundos sin recibir nada de un monitor (envía PING cada 2 s) = desconectado

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

# Operario con el que la central lanza sus propios riegos (punto 11)
OPERARIO_CENTRAL = "CENTRAL"
TODAS = "TODAS"

# Operarios dados de alta al crear la BD (los FO arrancan con uno de estos ID)
OPERARIOS_INICIALES = [
    (OPERARIO_CENTRAL, "Central de control"),
    ("FO_01", "Operario 1"),
    ("FO_02", "Operario 2"),
    ("FO_03", "Operario 3")
]

conexion = sqlite3.connect("database.db")
cursor = conexion.cursor()

#Creación de todas las tablas
cursor.execute("""
    CREATE TABLE IF NOT EXISTS estaciones(
        ID TEXT PRIMARY KEY,
        Location TEXT NOT NULL,
        Status TEXT NOT NULL,
        Flow REAL,
        Accum INTEGER,
        Operator TEXT
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS operarios(
        ID TEXT PRIMARY KEY,
        Nombre TEXT NOT NULL
    )
""")

# Un registro por cada riego: sirve para el resumen final que se envía al operario
cursor.execute("""
    CREATE TABLE IF NOT EXISTS riegos(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Estacion TEXT NOT NULL,
        Operario TEXT NOT NULL,
        Inicio TEXT NOT NULL,
        Limite INTEGER NOT NULL,
        Fin TEXT,
        Volumen REAL,
        Duracion INTEGER,
        Motivo TEXT
    )
""")

cursor.executemany("INSERT OR IGNORE INTO operarios (ID, Nombre) VALUES (?, ?)", OPERARIOS_INICIALES)

#Guardar
conexion.commit()

# Diccionario en memoria con todas las estaciones. Lo usan varios hilos a la vez
# (uno por cada monitor conectado y el del panel), así que se protege con un lock.
estaciones = {}
lock_estaciones = threading.Lock()

# Mensajes para el registro de eventos del panel. Los hilos de los sockets no pueden
# tocar la ventana de Tkinter, así que dejan los mensajes en esta cola y el panel los recoge.
cola_log = queue.Queue()

def log(mensaje):
    print(mensaje)
    cola_log.put(f"{time.strftime('%H:%M:%S')}  {mensaje}")

def ahora():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def cargar_estaciones_bd():
    # Al arrancar no sabemos el estado real de ninguna estación hasta que se
    # conecte, así que todas las registradas pasan a OFFLINE (punto 1 de la mecánica)
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()

    cursor.execute("""
        UPDATE estaciones
        SET Status = 'OFFLINE', Flow = NULL, Accum = NULL, Operator = NULL
    """)

    # Los riegos que quedaron abiertos cuando se apagó la central se cierran
    cursor.execute("""
        UPDATE riegos
        SET Fin = ?, Motivo = 'REINICIO_CENTRAL'
        WHERE Fin IS NULL
    """, (ahora(),))
    conexion.commit()

    cursor.execute("SELECT ID, Location FROM estaciones")
    for id_estacion, ubicacion in cursor.fetchall():
        estaciones[id_estacion] = {
            "id": id_estacion,
            "ubicacion": ubicacion,
            "estado": "OFFLINE",
            "bloqueada": False,
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

########## OPERARIOS Y RIEGOS (BD) ##########

def operario_existe(id_operario):
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()
    cursor.execute("SELECT ID FROM operarios WHERE ID = ?", (id_operario,))
    existe = cursor.fetchone() is not None
    conexion.close()
    return existe

def iniciar_riego_bd(id_estacion, id_operario, limite):
    # Se llama cuando el engine confirma que ha abierto la electroválvula (RIEGO_INICIO)
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()
    cursor.execute("""
        INSERT INTO riegos (Estacion, Operario, Inicio, Limite)
        VALUES (?, ?, ?, ?)
    """, (id_estacion, id_operario, ahora(), limite))
    cursor.execute("""
        UPDATE estaciones
        SET Status = 'WATERING', Flow = 0, Accum = 0, Operator = ?
        WHERE ID = ?
    """, (id_operario, id_estacion))
    conexion.commit()
    conexion.close()

def actualizar_telemetria_bd(id_estacion, caudal, volumen):
    # Se llama cada segundo durante el riego (TELEMETRIA)
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()
    cursor.execute("""
        UPDATE estaciones
        SET Flow = ?, Accum = ?
        WHERE ID = ?
    """, (caudal, volumen, id_estacion))
    conexion.commit()
    conexion.close()

def finalizar_riego_bd(id_estacion, volumen, duracion, motivo):
    # Cierra el riego abierto de la estación y devuelve su resumen (o None si no había)
    conexion = sqlite3.connect("database.db")
    cursor = conexion.cursor()
    cursor.execute("""
        SELECT ID, Operario, Inicio, Limite FROM riegos
        WHERE Estacion = ? AND Fin IS NULL
        ORDER BY ID DESC
    """, (id_estacion,))
    fila = cursor.fetchone()

    resumen = None
    if fila is not None:
        id_riego, id_operario, inicio, limite = fila
        cursor.execute("""
            UPDATE riegos
            SET Fin = ?, Volumen = ?, Duracion = ?, Motivo = ?
            WHERE ID = ?
        """, (ahora(), volumen, duracion, motivo, id_riego))
        resumen = {
            "riego": id_riego,
            "estacion": id_estacion,
            "operario": id_operario,
            "inicio": inicio,
            "limite": limite,
            "volumen": volumen,
            "duracion": duracion,
            "motivo": motivo
        }

    cursor.execute("""
        UPDATE estaciones
        SET Flow = NULL, Accum = NULL, Operator = NULL
        WHERE ID = ?
    """, (id_estacion,))
    conexion.commit()
    conexion.close()
    return resumen

########## EVENTOS DE RIEGO QUE ENVIARÁ EL ENGINE (por Kafka) ##########
# Estas funciones las llamará el consumidor de Kafka al recibir los mensajes
# RIEGO_INICIO, TELEMETRIA y RIEGO_FIN que ya publica (de momento en vacío) WM_WS_E.

def riego_iniciado(id_estacion, id_operario, limite):
    with lock_estaciones:
        estaciones[id_estacion]["estado"] = "WATERING"
        iniciar_riego_bd(id_estacion, id_operario, limite)
    log(f"[RIEGO] '{id_estacion}' empieza a regar. Operario: {id_operario}, límite {limite} s")

def telemetria_recibida(id_estacion, caudal, volumen):
    actualizar_telemetria_bd(id_estacion, caudal, volumen)

def riego_finalizado(id_estacion, volumen, duracion, motivo):
    with lock_estaciones:
        estacion = estaciones[id_estacion]
        resumen = finalizar_riego_bd(id_estacion, volumen, duracion, motivo)
        # Vuelve a reposo salvo que la haya cambiado otro evento (fuga, bloqueo, desconexión)
        if estacion["estado"] == "WATERING":
            estacion["estado"] = "STOPPED" if estacion["bloqueada"] else "AVAILABLE"
            modificar_estado(id_estacion, estacion["estado"])
    log(f"[RIEGO] '{id_estacion}' termina ({motivo}): {volumen} l en {duracion} s")
    # PENDIENTE KAFKA: enviar el resumen al operario que pidió el riego
    return resumen

########## ÓRDENES DE LA CENTRAL (punto 11) ##########

def enviar_orden(id_estacion, orden):
    # PENDIENTE KAFKA: aquí se publicará la orden para el WM_WS_E de esa estación.
    # Formatos ya definidos:
    #   REGAR#id_ws#operario#limite_segundos
    #   BLOQUEAR#id_ws
    #   ACTIVAR#id_ws
    log(f"[ORDEN -> {id_estacion}] {orden}  (envío por Kafka pendiente)")

def autorizar_riego(id_estacion, id_operario):
    # Comprobaciones antes de autorizar un riego (punto 4 de la mecánica).
    # Devuelve (True, "") o (False, motivo). La usarán también las peticiones de los FO.
    if not operario_existe(id_operario):
        return False, f"el operario '{id_operario}' no está dado de alta"

    with lock_estaciones:
        if id_estacion not in estaciones:
            return False, f"la estación '{id_estacion}' no existe"

        estado = estaciones[id_estacion]["estado"]
        if estado == "OFFLINE":
            return False, "la estación está desconectada"
        if estado == "LEAK":
            return False, "la estación tiene una fuga"
        if estado == "STOPPED" or estaciones[id_estacion]["bloqueada"]:
            return False, "la estación está fuera de servicio"
        if estado == "WATERING":
            return False, "la estación ya está regando"

    return True, ""

def orden_iniciar_riego(id_estacion, limite, id_operario=OPERARIO_CENTRAL):
    ok, motivo = autorizar_riego(id_estacion, id_operario)
    if ok:
        log(f"[RIEGO] Autorizado en '{id_estacion}' para {id_operario} ({limite} s)")
        enviar_orden(id_estacion, f"REGAR#{id_estacion}#{id_operario}#{limite}")
    else:
        log(f"[RIEGO] Denegado en '{id_estacion}': {motivo}")
    return ok

def orden_bloquear(id_estacion):
    # La WS termina cualquier riego y pasa a Fuera de Servicio (naranja)
    with lock_estaciones:
        estacion = estaciones[id_estacion]
        if estacion["estado"] == "OFFLINE":
            log(f"[BLOQUEO] '{id_estacion}' está desconectada, no se puede bloquear")
            return False
        estacion["bloqueada"] = True
        # Con fuga sigue en rojo: la avería se muestra antes que el bloqueo
        if estacion["estado"] in ("AVAILABLE", "WATERING"):
            estacion["estado"] = "STOPPED"
            modificar_estado(id_estacion, "STOPPED")
    log(f"[BLOQUEO] '{id_estacion}' bloqueada (Fuera de Servicio)")
    enviar_orden(id_estacion, f"BLOQUEAR#{id_estacion}")
    return True

def orden_activar(id_estacion):
    # La WS vuelve a estar ACTIVADA (verde)
    with lock_estaciones:
        estacion = estaciones[id_estacion]
        if estacion["estado"] == "OFFLINE":
            log(f"[ACTIVAR] '{id_estacion}' está desconectada, no se puede activar")
            return False
        estacion["bloqueada"] = False
        if estacion["estado"] == "STOPPED":
            estacion["estado"] = "AVAILABLE"
            modificar_estado(id_estacion, "AVAILABLE")
    log(f"[ACTIVAR] '{id_estacion}' activada")
    enviar_orden(id_estacion, f"ACTIVAR#{id_estacion}")
    return True

def aplicar_orden(id_estacion, funcion, *args):
    # Ejecuta la orden sobre una estación o sobre todas las conectadas
    if id_estacion == TODAS:
        with lock_estaciones:
            destino = [id for id, e in estaciones.items() if e["estado"] != "OFFLINE"]
        if not destino:
            log("[ORDEN] No hay ninguna estación conectada")
        for id in destino:
            funcion(id, *args)
    else:
        funcion(id_estacion, *args)

########## SOCKETS ##########

def registrar_estacion(id_estacion, ubicacion, conn, addr):
    # Autenticación: se rechaza un ID que ya tiene un monitor conectado.
    # Comprobar y dar de alta va dentro del lock para que dos monitores con el
    # mismo ID que lleguen a la vez no puedan pasar los dos.
    with lock_estaciones:
        if id_estacion in estaciones and estaciones[id_estacion]["conn"] is not None:
            return False

        # Si la central la había bloqueado, sigue bloqueada al reconectarse
        bloqueada = id_estacion in estaciones and estaciones[id_estacion]["bloqueada"]
        estaciones[id_estacion] = {
            "id": id_estacion,
            "ubicacion": ubicacion,
            "estado": "STOPPED" if bloqueada else "AVAILABLE",
            "bloqueada": bloqueada,
            "addr": addr,
            "conn": conn
        }
        guardar_estacion_bd(estaciones, id_estacion)
    return True

def cambiar_estado_monitor(id_estacion, nuevo_estado):
    # Aplica un ESTADO enviado por el monitor. Devuelve el estado con que queda.
    with lock_estaciones:
        estacion = estaciones[id_estacion]
        # Al resolverse una fuga, una estación bloqueada vuelve a Fuera de Servicio
        if nuevo_estado == "AVAILABLE" and estacion["bloqueada"]:
            nuevo_estado = "STOPPED"
        estado_anterior = estacion["estado"]
        estacion["estado"] = nuevo_estado
        modificar_estado(id_estacion, nuevo_estado)

    if estado_anterior == "WATERING" and nuevo_estado == "LEAK":
        log(f"[AVERÍA] Fuga en '{id_estacion}' durante un riego: el engine lo corta")
    return nuevo_estado

def desconectar_estacion(id_estacion):
    with lock_estaciones:
        estacion = estaciones[id_estacion]
        estado_anterior = estacion["estado"]
        estacion["estado"] = "OFFLINE"
        estacion["conn"] = None
        estacion["addr"] = None
        modificar_estado(id_estacion, "OFFLINE")

    # Si estaba regando, el riego queda cerrado: ya no se puede monitorizar
    if estado_anterior == "WATERING":
        finalizar_riego_bd(id_estacion, None, None, "DESCONEXION")

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


def send_msg(sock, msg):
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


def receive_msg(sock):
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


def atender_mensaje(conn, addr, msg, id_estacion_actual):
    # Procesa un mensaje del monitor y le responde.
    # Devuelve (id de la estación de esta conexión, si la conexión sigue abierta)
    partes = msg.split('#')
    comando = partes[0]

    if comando == "REGISTER" and len(partes) >= 3 and id_estacion_actual is None:
        id_estacion = partes[1]
        ubicacion = partes[2]

        if registrar_estacion(id_estacion, ubicacion, conn, addr):
            log(f"[REGISTRO OK] Estación '{id_estacion}' ({ubicacion}) registrada desde {addr}")
            send_msg(conn, f"ACK#REGISTER_OK#{id_estacion}")
            mostrar_bd()
            return id_estacion, True

        log(f"[REGISTRO KO] '{id_estacion}' ya está conectada, se rechaza {addr}")
        send_msg(conn, f"NACK#REGISTER_KO#{id_estacion}#ID ya conectado")
        return None, False

    # Autenticación: hasta registrarse solo se acepta REGISTER
    if id_estacion_actual is None:
        print(f"[CENTRAL] {addr} envía '{msg}' sin estar registrado")
        send_msg(conn, "NACK#NO_REGISTRADO#Primero hay que enviar REGISTER#id#ubicacion")
        return None, True

    if comando == "PING":
        # Latido del monitor: solo sirve para saber que la conexión sigue viva
        send_msg(conn, f"ACK#PONG#{id_estacion_actual}")

    elif comando == "ESTADO" and len(partes) >= 3:
        id_estacion = partes[1]
        nuevo_estado = partes[2]

        # Un monitor solo puede cambiar el estado de su propia estación
        if id_estacion == id_estacion_actual and nuevo_estado in ESTADOS_WS:
            estado_final = cambiar_estado_monitor(id_estacion, nuevo_estado)
            log(f"[ESTADO] Estación '{id_estacion}' -> {estado_final}")
            send_msg(conn, f"ACK#ESTADO_OK#{id_estacion}#{estado_final}")
            mostrar_bd()
        else:
            print(f"[ESTADO] Mensaje de estado no válido: {msg}")
            send_msg(conn, f"NACK#ESTADO_KO#{id_estacion}")

    elif comando == "REGISTER":
        send_msg(conn, f"NACK#REGISTER_KO#{id_estacion_actual}#Esta conexión ya está registrada")

    else:
        print(f"[CENTRAL] Recibido de [{id_estacion_actual}]: {msg}")
        send_msg(conn, f"ACK#OK#Recibido: {msg}")

    return id_estacion_actual, True

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    # Con el timeout, una estación que deja de enviar PING (red caída, PC apagado
    # sin cerrar el socket...) se detecta y pasa a OFFLINE
    conn.settimeout(TIMEOUT_MONITOR)
    id_estacion_actual = None
    connected = True

    # try/finally: pase lo que pase (FIN, corte, excepción) la estación queda OFFLINE
    try:
        if not aceptar_saludo(conn):
            print(f"[HANDSHAKE] {addr} no ha empezado con ENQ, se cierra la conexión")
            connected = False

        while connected:
            try:
                msg = receive_msg(conn)
            except socket.timeout:
                log(f"[LATIDO] Sin noticias de '{id_estacion_actual or addr}' en {TIMEOUT_MONITOR}s")
                msg = None

            if msg is None:
                connected = False
            elif msg == FIN:
                connected = False
                print(f"[FIN] La estación '{id_estacion_actual}' ha enviado FIN")
            else:
                id_estacion_actual, connected = atender_mensaje(conn, addr, msg, id_estacion_actual)
    except OSError as e:  # conexión cortada, reseteada o demasiadas tramas corruptas
        print(f"[CONEXION] Error con {id_estacion_actual or addr}: {e}")
    finally:
        if id_estacion_actual is not None:
            desconectar_estacion(id_estacion_actual)
            log(f"[DESCONEXION] Estación '{id_estacion_actual}' marcada como OFFLINE")
            mostrar_bd()
        despedirse(conn)


def start(server, ip, port):
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 2}")

########## PANEL ##########

def Pantalla_Monitor():
    ventana = tk.Tk()
    ventana.title("WM_Central - Monitor de Estaciones de Riego")
    ventana.geometry("1280x720")

    # El tema por defecto de Windows ignora el color de fondo de las filas; "clam" sí lo respeta
    estilo = ttk.Style()
    estilo.theme_use("clam")

    # 1. Frame Izquierdo: Tabla de Datos, órdenes y registro de eventos
    frame_izq = tk.Frame(ventana)
    frame_izq.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

    lbl_tabla = tk.Label(frame_izq, text="Estado de las Estaciones", font=("Arial", 14, "bold"))
    lbl_tabla.pack(pady=5)

    frame_tabla = tk.Frame(frame_izq)
    frame_tabla.pack(fill=tk.BOTH, expand=True)

    columnas = ("ID", "Location", "Status", "Flow", "Accum", "Operator")
    tabla = ttk.Treeview(frame_tabla, columns=columnas, show="headings", height=8)

    anchos = {"ID": 70, "Location": 150, "Status": 125, "Flow": 95, "Accum": 80, "Operator": 85}
    for col in columnas:
        tabla.heading(col, text=col)
        tabla.column(col, width=anchos[col], anchor=tk.CENTER)

    # Una etiqueta (tag) por estado con su color de fondo
    for estado, color in COLOR_ESTADO.items():
        tabla.tag_configure(estado, background=color)

    scrollbar = ttk.Scrollbar(frame_tabla, orient=tk.VERTICAL, command=tabla.yview)
    tabla.configure(yscroll=scrollbar.set)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    tabla.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Órdenes de la central sobre una o todas las estaciones (punto 11)
    frame_ordenes = tk.LabelFrame(frame_izq, text="Órdenes de la central", font=("Arial", 10, "bold"))
    frame_ordenes.pack(fill=tk.X, pady=8)

    tk.Label(frame_ordenes, text="Estación:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
    combo_estacion = ttk.Combobox(frame_ordenes, values=[TODAS], state="readonly", width=12)
    combo_estacion.set(TODAS)
    combo_estacion.grid(row=0, column=1, padx=5, pady=5)

    tk.Label(frame_ordenes, text="Duración (s):").grid(row=0, column=2, padx=5, pady=5, sticky="e")
    spin_duracion = tk.Spinbox(frame_ordenes, from_=5, to=3600, increment=5, width=6)
    spin_duracion.delete(0, tk.END)
    spin_duracion.insert(0, "30")
    spin_duracion.grid(row=0, column=3, padx=5, pady=5)

    def pulsar_regar():
        duracion = spin_duracion.get().strip()
        if not duracion.isdigit() or int(duracion) <= 0:
            log("[RIEGO] La duración debe ser un número de segundos mayor que 0")
        else:
            aplicar_orden(combo_estacion.get(), orden_iniciar_riego, int(duracion))

    tk.Button(frame_ordenes, text="Iniciar riego", bg="#c8e6c9", width=12,
              command=pulsar_regar).grid(row=0, column=4, padx=5, pady=5)
    tk.Button(frame_ordenes, text="Bloquear", bg="#ffe0b2", width=12,
              command=lambda: aplicar_orden(combo_estacion.get(), orden_bloquear)).grid(row=0, column=5, padx=5, pady=5)
    tk.Button(frame_ordenes, text="Activar", bg="#c8e6c9", width=12,
              command=lambda: aplicar_orden(combo_estacion.get(), orden_activar)).grid(row=0, column=6, padx=5, pady=5)

    # Al pinchar una fila se selecciona esa estación en el desplegable. Después se
    # quita la selección para que el color de selección no tape el color del estado.
    def seleccionar_fila(evento):
        seleccion = tabla.selection()
        if seleccion:
            combo_estacion.set(seleccion[0])
            tabla.selection_remove(seleccion)
    tabla.bind("<<TreeviewSelect>>", seleccionar_fila)

    # Registro de eventos: mensajes de autorización, denegación, órdenes, estados...
    tk.Label(frame_izq, text="Registro de eventos", font=("Arial", 10, "bold")).pack(anchor="w")
    texto_log = tk.Text(frame_izq, height=10, state=tk.DISABLED, font=("Consolas", 9), bg="#1e2329", fg="#d8dee9")
    texto_log.pack(fill=tk.BOTH, expand=True)

    # Alterna en cada refresco para que las estaciones regando parpadeen
    parpadeo = {"encendido": True}

    # Función interna para refrescar los datos automáticamente
    def cargar_datos_bd():
        parpadeo["encendido"] = not parpadeo["encendido"]

        try:
            conn = sqlite3.connect("database.db")
            cur = conn.cursor()
            cur.execute("SELECT ID, Location, Status, Flow, Accum, Operator FROM estaciones")
            filas = cur.fetchall()
            conn.close()

            # Las filas se actualizan en su sitio (iid = ID) para no perder la selección
            for fila in filas:
                estado = fila[2]
                fila_limpia = [valor if valor is not None else "-" for valor in fila]
                fila_limpia[2] = TEXTO_ESTADO.get(estado, estado)
                if fila[3] is not None:
                    fila_limpia[3] = f"{fila[3]:.2f} l/min"
                if fila[4] is not None:
                    fila_limpia[4] = f"{fila[4]:.2f} l"

                tag = estado
                if estado == "WATERING" and not parpadeo["encendido"]:
                    tag = "WATERING_OFF"

                if tabla.exists(fila[0]):
                    tabla.item(fila[0], values=fila_limpia, tags=(tag,))
                else:
                    tabla.insert("", tk.END, iid=fila[0], values=fila_limpia, tags=(tag,))

            ids = [TODAS] + [fila[0] for fila in filas]
            if list(combo_estacion["values"]) != ids:
                combo_estacion["values"] = ids
        except Exception as e:
            print(f"[ERROR BD] No se pudieron refrescar los datos: {e}")

        # Mensajes nuevos del registro de eventos
        while not cola_log.empty():
            texto_log.configure(state=tk.NORMAL)
            texto_log.insert(tk.END, cola_log.get() + "\n")
            texto_log.see(tk.END)
            texto_log.configure(state=tk.DISABLED)

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

if __name__ == "__main__":
    if len(sys.argv) == 2:
        cargar_estaciones_bd()
        log(f"[BD] {len(estaciones)} estaciones cargadas de la base de datos (todas en OFFLINE)")
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
