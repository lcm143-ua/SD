import socket
import threading
import sys
import sqlite3
import queue
import time
import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk   # Tkinter no abre JPG por sí solo (pip install pillow)

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
    "AVAILABLE": "#c8e6c9",     # verde
    "WATERING": "#43a047",      # verde intenso (parpadea alternando con WATERING_OFF)
    "WATERING_OFF": "#e8f5e9",  # verde muy claro
    "LEAK": "#e53935",          # rojo
    "STOPPED": "#ffcc80",       # naranja
    "OFFLINE": "#eceff1"        # gris
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
        Sector TEXT,
        Status TEXT NOT NULL,
        Flow REAL,
        Accum INTEGER,
        Operator TEXT
    )
""")

try:
    cursor.execute("ALTER TABLE estaciones ADD COLUMN Sector TEXT")
except sqlite3.OperationalError:
    pass

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

ZONAS_SECTORES = {
    "Invernadero": [
        (100, 50), (150, 50)
    ],
    "Cripta": [
        (250, 50), (300, 50), (300, 100), (350, 100)
    ],
    "Bosque_Dioses": [
        (100, 100), (150, 100), (100, 150), (150, 150), 
        (200, 150), (250, 150), (100, 200), (150, 200), (200, 200)
    ],
    "Patio_Este": [
        (300, 150), (350, 150), (400, 100), (400, 150)
    ],
    "Patio_Armas": [
        (250, 200), (300, 200), (250, 250), (300, 250)
    ],
    "Establos": [
        (150, 300), (200, 300), (250, 300), (300, 300), 
        (250, 350), (300, 350)
    ],
    "Septo": [
        (350, 250), (400, 250), (350, 300), (400, 300), (350, 350)
    ]
}

def log(mensaje):
    print(mensaje)
    cola_log.put(f"{time.strftime('%H:%M:%S')}  {mensaje}")

def ahora():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def obtener_sectores_ocupados():
    ocupados = set()
    for e in estaciones.values():
        if e.get("sector") is not None and e.get("estado") != "OFFLINE":
            ocupados.add(e["sector"])
    return ocupados

def asignar_sector(id_estacion, ubicacion, sector_idx=None):
    if ubicacion not in ZONAS_SECTORES:
        return None
    lista = ZONAS_SECTORES[ubicacion]
    ocupados = obtener_sectores_ocupados()
    if id_estacion in estaciones and estaciones[id_estacion].get("sector") in lista:
        ocupados.discard(estaciones[id_estacion]["sector"])
    if sector_idx is not None and isinstance(sector_idx, int) and 0 <= sector_idx < len(lista):
        cand = lista[sector_idx]
        if cand not in ocupados:
            return cand
        return None
    for cand in lista:
        if cand not in ocupados:
            return cand
    return None

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

    cursor.execute("SELECT ID, Location, Sector FROM estaciones")
    for id_estacion, ubicacion, sector_raw in cursor.fetchall():
        sector = None
        if sector_raw:
            try:
                p = sector_raw.split(',')
                sector = (int(p[0]), int(p[1]))
            except Exception:
                sector = None
        estaciones[id_estacion] = {
            "id": id_estacion,
            "ubicacion": ubicacion,
            "sector": sector,
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
    ubicacion = estacion["ubicacion"]
    sector = estacion.get("sector")
    sector_str = f"{sector[0]},{sector[1]}" if sector else None
    estado = estacion["estado"]

    cursor.execute("SELECT * FROM estaciones WHERE ID = ?", (id_estacion,))

    if cursor.fetchone() is not None:
        cursor.execute("""
            UPDATE estaciones
            SET Status = ?, Location = ?, Sector = ?
            WHERE ID = ?
        """, (estado, ubicacion, sector_str, id_estacion))
    else:
        cursor.execute("""
            INSERT INTO estaciones (ID, Location, Sector, Status)
            VALUES (?,?,?,?)
            """, (id_estacion, ubicacion, sector_str, estado)
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

def registrar_estacion(id_estacion, ubicacion, conn, addr, sector_idx=None):
    # Autenticación: se rechaza un ID que ya tiene un monitor conectado.
    # Comprobar y dar de alta va dentro del lock para que dos monitores con el
    # mismo ID que lleguen a la vez no puedan pasar los dos.
    with lock_estaciones:
        if id_estacion in estaciones and estaciones[id_estacion]["conn"] is not None:
            return False

        sec = asignar_sector(id_estacion, ubicacion, sector_idx)
        if sec is None:
            return False

        # Si la central la había bloqueado, sigue bloqueada al reconectarse
        bloqueada = id_estacion in estaciones and estaciones[id_estacion]["bloqueada"]
        estaciones[id_estacion] = {
            "id": id_estacion,
            "ubicacion": ubicacion,
            "sector": sec,
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
        sector_idx = int(partes[3]) if len(partes) > 3 and partes[3].isdigit() else None

        if registrar_estacion(id_estacion, ubicacion, conn, addr, sector_idx):
            log(f"[REGISTRO OK] Estación '{id_estacion}' ({ubicacion}) registrada desde {addr}")
            send_msg(conn, f"ACK#REGISTER_OK#{id_estacion}")
            mostrar_bd()
            return id_estacion, True

        log(f"[REGISTRO KO] '{id_estacion}' ya está conectada o sector ocupado, se rechaza {addr}")
        send_msg(conn, f"NACK#REGISTER_KO#{id_estacion}#ID o Sector no disponible")
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
    print(f"[LISTENING] Servidor a la escucha en {ip}:{port}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        thread.start()
        print(f"[CONEXIONES ACTIVAS] {threading.active_count() - 2}")

########## PANEL ##########

# Paleta del panel
C_FONDO = "#eef1f5"         # fondo de la ventana
C_TARJETA = "#ffffff"       # fondo de las tarjetas
C_BORDE = "#d9dee5"
C_CABECERA = "#1b2a3a"      # barra superior
C_TEXTO = "#1f2933"
C_TEXTO_SUAVE = "#6b7785"
C_ACENTO = "#1e88e5"
FUENTE = "Segoe UI"

# Color del texto de cada estado en la tabla (el fondo sale de COLOR_ESTADO)
TEXTO_COLOR_ESTADO = {
    "AVAILABLE": "#1b5e20",
    "WATERING": "#ffffff",
    "WATERING_OFF": "#1b5e20",
    "LEAK": "#ffffff",
    "STOPPED": "#4e2a00",
    "OFFLINE": "#5f6b76"
}

# Color de cada línea del registro según la etiqueta con la que empieza el mensaje
COLOR_LOG = {
    "error": "#ff6b6b",
    "aviso": "#ffb454",
    "ok": "#7ed491",
    "info": "#7fb8ff",
    "normal": "#d8dee9",
    "hora": "#6c7a89"
}


def tipo_log(linea):
    # Decide el color de una línea del registro mirando sus palabras clave
    texto = linea.upper()
    if texto.startswith("[BD]"):
        return "info"
    if any(p in texto for p in ("KO]", "AVERÍA", "FUGA", "ERROR", "DENEG", "NO SE PUEDE", "NO AUTORIZ")):
        return "error"
    if any(p in texto for p in ("[BLOQUEO]", "[DESCONEXION]", "[LATIDO]", "OFFLINE")):
        return "aviso"
    if any(p in texto for p in ("[RIEGO]", "[ACTIVAR]", "REGISTRO OK")):
        return "ok"
    if any(p in texto for p in ("[ORDEN", "[ESTADO]")):
        return "info"
    return "normal"


def crear_tarjeta(padre, titulo=None):
    # Marco blanco con borde fino y título opcional. Devuelve el frame interior.
    borde = tk.Frame(padre, bg=C_BORDE)
    interior = tk.Frame(borde, bg=C_TARJETA)
    interior.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)
    if titulo:
        tk.Label(interior, text=titulo.upper(), font=(FUENTE, 9, "bold"), fg=C_TEXTO_SUAVE,
                 bg=C_TARJETA, anchor="w").pack(fill=tk.X, padx=14, pady=(12, 6))
    return borde, interior


def configurar_estilos(ventana):
    estilo = ttk.Style(ventana)
    # El tema por defecto de Windows ignora el color de fondo de las filas; "clam" sí lo respeta
    estilo.theme_use("clam")

    estilo.configure(".", font=(FUENTE, 10), background=C_TARJETA, foreground=C_TEXTO)

    estilo.configure("Treeview", rowheight=30, font=(FUENTE, 10), background=C_TARJETA,
                     fieldbackground=C_TARJETA, borderwidth=0, relief="flat")
    estilo.configure("Treeview.Heading", font=(FUENTE, 9, "bold"), background="#f5f7fa",
                     foreground=C_TEXTO_SUAVE, relief="flat", borderwidth=0, padding=(6, 8))
    estilo.map("Treeview.Heading", background=[("active", "#e8ecf1")])
    estilo.layout("Treeview", [("Treeview.treearea", {"sticky": "nswe"})])

    estilo.configure("Vertical.TScrollbar", background="#d5dbe3", troughcolor=C_TARJETA,
                     bordercolor=C_TARJETA, arrowcolor=C_TEXTO_SUAVE, relief="flat")

    estilo.configure("TCombobox", padding=5, arrowcolor=C_TEXTO_SUAVE, bordercolor=C_BORDE,
                     lightcolor=C_BORDE, darkcolor=C_BORDE)
    estilo.map("TCombobox", fieldbackground=[("readonly", C_TARJETA)],
               bordercolor=[("focus", C_ACENTO)])
    estilo.configure("TSpinbox", padding=5, arrowcolor=C_TEXTO_SUAVE, bordercolor=C_BORDE,
                     lightcolor=C_BORDE, darkcolor=C_BORDE)
    estilo.map("TSpinbox", bordercolor=[("focus", C_ACENTO)])

    # Botones: (fondo, fondo al pasar el ratón, texto)
    botones = {
        "Regar.TButton": ("#2e7d32", "#256b29", "#ffffff"),
        "Bloquear.TButton": ("#ef6c00", "#d45f00", "#ffffff"),
        "Activar.TButton": ("#1e88e5", "#1873c4", "#ffffff")
    }
    for nombre, (fondo, hover, texto) in botones.items():
        estilo.configure(nombre, font=(FUENTE, 10, "bold"), background=fondo, foreground=texto,
                         borderwidth=0, focusthickness=0, padding=(14, 7), relief="flat")
        estilo.map(nombre, background=[("pressed", hover), ("active", hover)])


def Pantalla_Monitor():
    ventana = tk.Tk()
    ventana.title("WM_Central - Monitor de Estaciones de Riego")
    ventana.geometry("1280x720")
    ventana.minsize(1100, 640)
    try:
        ventana.state("zoomed")     # maximizada en Windows
    except tk.TclError:
        pass
    ventana.configure(bg=C_FONDO)
    configurar_estilos(ventana)

    # ---------- Barra superior ----------
    cabecera = tk.Frame(ventana, bg=C_CABECERA, height=64)
    cabecera.pack(side=tk.TOP, fill=tk.X)
    cabecera.pack_propagate(False)

    tk.Label(cabecera, text="💧", font=(FUENTE, 20), bg=C_CABECERA, fg="#4fc3f7").pack(side=tk.LEFT, padx=(20, 8))
    titulos = tk.Frame(cabecera, bg=C_CABECERA)
    titulos.pack(side=tk.LEFT)
    tk.Label(titulos, text="WM Central", font=(FUENTE, 15, "bold"), bg=C_CABECERA, fg="white").pack(anchor="w")
    tk.Label(titulos, text="Monitor de Estaciones de Riego · Invernalia", font=(FUENTE, 9),
             bg=C_CABECERA, fg="#9fb3c8").pack(anchor="w")

    lbl_reloj = tk.Label(cabecera, font=(FUENTE, 13, "bold"), bg=C_CABECERA, fg="white")
    lbl_reloj.pack(side=tk.RIGHT, padx=20)
    puerto = globals().get("PORT", 5050)
    tk.Label(cabecera, text=f"●  En servicio · puerto {puerto}", font=(FUENTE, 9, "bold"),
             bg=C_CABECERA, fg="#7ed491").pack(side=tk.RIGHT, padx=10)

    # ---------- Barra de estado inferior ----------
    pie = tk.Frame(ventana, bg="#dfe4ea", height=26)
    pie.pack(side=tk.BOTTOM, fill=tk.X)
    pie.pack_propagate(False)
    lbl_pie = tk.Label(pie, font=(FUENTE, 8), bg="#dfe4ea", fg=C_TEXTO_SUAVE, anchor="w")
    lbl_pie.pack(side=tk.LEFT, padx=12)
    tk.Label(pie, text="Sistemas Distribuidos 2026-27 · WaterManagement", font=(FUENTE, 8),
             bg="#dfe4ea", fg=C_TEXTO_SUAVE).pack(side=tk.RIGHT, padx=12)

    # ---------- Cuerpo ----------
    cuerpo = tk.Frame(ventana, bg=C_FONDO)
    cuerpo.pack(fill=tk.BOTH, expand=True, padx=16, pady=12)
    cuerpo.columnconfigure(0, weight=1, uniform="col")
    cuerpo.columnconfigure(1, weight=1, uniform="col")
    cuerpo.rowconfigure(1, weight=1)

    # Fila de indicadores: número de estaciones en cada estado
    frame_kpis = tk.Frame(cuerpo, bg=C_FONDO)
    frame_kpis.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 14))
    indicadores = [
        ("TOTAL", "Estaciones", C_ACENTO),
        ("AVAILABLE", TEXTO_ESTADO["AVAILABLE"], "#43a047"),
        ("WATERING", TEXTO_ESTADO["WATERING"], "#2e7d32"),
        ("LEAK", TEXTO_ESTADO["LEAK"], "#e53935"),
        ("STOPPED", TEXTO_ESTADO["STOPPED"], "#fb8c00"),
        ("OFFLINE", TEXTO_ESTADO["OFFLINE"], "#9e9e9e")
    ]
    lbl_kpis = {}
    for i, (clave, texto, color) in enumerate(indicadores):
        frame_kpis.columnconfigure(i, weight=1, uniform="kpi")
        borde, tarjeta = crear_tarjeta(frame_kpis)
        borde.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 10, 0))
        tk.Frame(tarjeta, bg=color, width=4).pack(side=tk.LEFT, fill=tk.Y)
        contenido = tk.Frame(tarjeta, bg=C_TARJETA)
        contenido.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=12, pady=8)
        lbl_kpis[clave] = tk.Label(contenido, text="0", font=(FUENTE, 20, "bold"), bg=C_TARJETA, fg=C_TEXTO)
        lbl_kpis[clave].pack(anchor="w")
        tk.Label(contenido, text=texto, font=(FUENTE, 9), bg=C_TARJETA, fg=C_TEXTO_SUAVE).pack(anchor="w")

    # ---------- Columna izquierda: tabla, órdenes y registro ----------
    frame_izq = tk.Frame(cuerpo, bg=C_FONDO)
    frame_izq.grid(row=1, column=0, sticky="nsew", padx=(0, 14))
    frame_izq.rowconfigure(0, weight=3)
    frame_izq.rowconfigure(1, weight=0)
    frame_izq.rowconfigure(2, weight=2)
    frame_izq.columnconfigure(0, weight=1)

    borde_tabla, tarjeta_tabla = crear_tarjeta(frame_izq, "Estado de las estaciones")
    borde_tabla.grid(row=0, column=0, sticky="nsew", pady=(0, 10))

    frame_tabla = tk.Frame(tarjeta_tabla, bg=C_TARJETA)
    frame_tabla.pack(fill=tk.BOTH, expand=True, padx=14, pady=(0, 14))

    columnas = ("ID", "Location", "Sector", "Status", "Flow", "Accum", "Operator")
    cabeceras = {"ID": "ID", "Location": "Ubicación", "Sector": "Sector", "Status": "Estado", "Flow": "Caudal",
                 "Accum": "Acumulado", "Operator": "Operario"}
    anchos = {"ID": 60, "Location": 110, "Sector": 90, "Status": 120, "Flow": 80, "Accum": 80, "Operator": 80}
    tabla = ttk.Treeview(frame_tabla, columns=columnas, show="headings", height=8)
    for col in columnas:
        tabla.heading(col, text=cabeceras[col].upper())
        tabla.column(col, width=anchos[col], anchor=tk.CENTER)
    tabla.column("Location", anchor=tk.W)
    tabla.heading("Location", anchor=tk.W)

    for estado, color in COLOR_ESTADO.items():
        tabla.tag_configure(estado, background=color, foreground=TEXTO_COLOR_ESTADO.get(estado, C_TEXTO))

    scrollbar = ttk.Scrollbar(frame_tabla, orient=tk.VERTICAL, command=tabla.yview)
    tabla.configure(yscrollcommand=scrollbar.set)
    tabla.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    borde_ordenes, tarjeta_ordenes = crear_tarjeta(frame_izq, "Acciones de control")
    borde_ordenes.grid(row=1, column=0, sticky="ew", pady=(0, 10))

    f_ctrl = tk.Frame(tarjeta_ordenes, bg=C_TARJETA)
    f_ctrl.pack(fill=tk.X, padx=14, pady=(0, 14))

    tk.Label(f_ctrl, text="Estación:", font=(FUENTE, 9, "bold"), bg=C_TARJETA).grid(row=0, column=0, sticky="w", padx=(0, 5))
    combo_estaciones = ttk.Combobox(f_ctrl, values=[TODAS], state="readonly", width=12)
    combo_estaciones.set(TODAS)
    combo_estaciones.grid(row=0, column=1, sticky="w", padx=(0, 15))

    tk.Label(f_ctrl, text="Límite (s):", font=(FUENTE, 9, "bold"), bg=C_TARJETA).grid(row=0, column=2, sticky="w", padx=(0, 5))
    spin_limite = ttk.Spinbox(f_ctrl, from_=10, to=3600, increment=10, width=6)
    spin_limite.set(60)
    spin_limite.grid(row=0, column=3, sticky="w", padx=(0, 15))

    def cmd_regar():
        target = combo_estaciones.get()
        lim = int(spin_limite.get())
        aplicar_orden(target, orden_iniciar_riego, lim)

    def cmd_bloquear():
        target = combo_estaciones.get()
        aplicar_orden(target, orden_bloquear)

    def cmd_activar():
        target = combo_estaciones.get()
        aplicar_orden(target, orden_activar)

    btn_regar = ttk.Button(f_ctrl, text="REGAR", style="Regar.TButton", command=cmd_regar)
    btn_regar.grid(row=0, column=4, padx=5)

    btn_bloquear = ttk.Button(f_ctrl, text="BLOQUEAR", style="Bloquear.TButton", command=cmd_bloquear)
    btn_bloquear.grid(row=0, column=5, padx=5)

    btn_activar = ttk.Button(f_ctrl, text="ACTIVAR", style="Activar.TButton", command=cmd_activar)
    btn_activar.grid(row=0, column=6, padx=5)

    borde_log, tarjeta_log = crear_tarjeta(frame_izq, "Registro de eventos")
    borde_log.grid(row=2, column=0, sticky="nsew")

    f_log = tk.Frame(tarjeta_log, bg=C_TARJETA)
    f_log.pack(fill=tk.BOTH, expand=True, padx=14, pady=(0, 14))

    txt_log = tk.Text(f_log, bg="#1e2836", fg="#d8dee9", font=("Consolas", 9), relief="flat", wrap="word")
    scroll_log = ttk.Scrollbar(f_log, orient=tk.VERTICAL, command=txt_log.yview)
    txt_log.configure(yscrollcommand=scroll_log.set)
    txt_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scroll_log.pack(side=tk.RIGHT, fill=tk.Y)

    for k, v in COLOR_LOG.items():
        txt_log.tag_config(k, foreground=v)

    frame_der = tk.Frame(cuerpo, bg=C_FONDO)
    frame_der.grid(row=1, column=1, sticky="nsew")
    frame_der.rowconfigure(0, weight=1)
    frame_der.columnconfigure(0, weight=1)

    borde_mapa, tarjeta_mapa = crear_tarjeta(frame_der, "Mapa de Sectores")
    borde_mapa.grid(row=0, column=0, sticky="nsew")

    f_mapa = tk.Frame(tarjeta_mapa, bg=C_TARJETA)
    f_mapa.pack(fill=tk.BOTH, expand=True, padx=14, pady=(0, 14))

    canvas_mapa = tk.Canvas(f_mapa, width=500, height=400, bg="#f5f7fa", highlightthickness=1, highlightbackground=C_BORDE)
    canvas_mapa.pack()

    mapa_img_tk = None
    for _nmap in ["InvernaliaZonas.jpg", "InvernaliaZonas.png", "mapa.jpg", "mapa.png", "Invernalia.jpg", "Invernalia.png", "mapa.jpeg"]:
        try:
            _pil_m = Image.open(_nmap)
            _pil_m = _pil_m.resize((500, 400))
            mapa_img_tk = ImageTk.PhotoImage(_pil_m)
            break
        except Exception:
            pass

    def redibujar_mapa():
        canvas_mapa.delete("all")
        if mapa_img_tk:
            canvas_mapa.create_image(0, 0, image=mapa_img_tk, anchor="nw")

        with lock_estaciones:
            for id_est, data in estaciones.items():
                sec = data.get("sector")
                est = data.get("estado", "OFFLINE")
                if sec:
                    cx, cy = sec
                    bg_col = COLOR_ESTADO.get(est, "#eceff1")
                    fg_col = TEXTO_COLOR_ESTADO.get(est, "#1f2933")
                    canvas_mapa.create_oval(cx - 9, cy - 9, cx + 9, cy + 9, fill=bg_col, outline="#1b2a3a", width=1)
                    canvas_mapa.create_text(cx, cy, text=id_est, fill=fg_col, font=(FUENTE, 6, "bold"))

    def refrescar_panel():
        lbl_reloj.config(text=time.strftime("%H:%M:%S"))

        while not cola_log.empty():
            linea = cola_log.get_nowait()
            tag = tipo_log(linea)
            txt_log.insert(tk.END, linea + "\n", tag)
            txt_log.see(tk.END)

        counts = {"TOTAL": 0, "AVAILABLE": 0, "WATERING": 0, "LEAK": 0, "STOPPED": 0, "OFFLINE": 0}
        with lock_estaciones:
            items_est = list(estaciones.values())

        for e in items_est:
            counts["TOTAL"] += 1
            st = e["estado"]
            if st in counts:
                counts[st] += 1

        for k, v in counts.items():
            if k in lbl_kpis:
                lbl_kpis[k].config(text=str(v))

        for item in tabla.get_children():
            tabla.delete(item)

        ids_conectados = []
        for e in items_est:
            sec_str = f"({e['sector'][0]},{e['sector'][1]})" if e.get("sector") else "-"
            fl = f"{e.get('flow'):.1f} L/min" if e.get('flow') is not None else "-"
            ac = f"{e.get('accum')} L" if e.get('accum') is not None else "-"
            op = e.get('operator') or "-"
            st = e["estado"]
            tabla.insert("", tk.END, values=(e["id"], e["ubicacion"], sec_str, TEXTO_ESTADO.get(st, st), fl, ac, op), tags=(st,))
            if st != "OFFLINE":
                ids_conectados.append(e["id"])

        combo_estaciones["values"] = [TODAS] + sorted(ids_conectados)

        redibujar_mapa()
        ventana.after(500, refrescar_panel)

    lbl_pie.config(text="Sistema iniciado correctamente.")
    refrescar_panel()
    ventana.mainloop()


if __name__ == "__main__":
    SERVER = socket.gethostbyname(socket.gethostname())
    PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5050
    cargar_estaciones_bd()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((SERVER, PORT))
    t_server = threading.Thread(target=start, args=(server_sock, SERVER, PORT), daemon=True)
    t_server.start()
    Pantalla_Monitor()