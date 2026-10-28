import socket
import threading
import sys
import os
import json
import sqlite3
import queue
import time
import logging
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from kafka import KafkaProducer, KafkaConsumer, TopicPartition   # pip install kafka-python
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# El panel de escritorio (Tkinter) solo se abre si hay pantalla. En Railway/Docker
# no la hay: allí se usa únicamente el panel web.
try:
    import tkinter as tk
    from tkinter import ttk
    from PIL import Image, ImageTk   # Tkinter no abre JPG por sí solo (pip install pillow)
except ImportError:
    tk = None

FORMAT = 'utf-8'
FIN = "FIN"

# Carpeta del programa: la imagen del mapa está aquí aunque se lance desde otro sitio
CARPETA = os.path.dirname(os.path.abspath(__file__))

# Ruta de la BD. En Railway se apunta a un volumen (WM_DB=/data/database.db)
# para que no se borre en cada despliegue.
DB_PATH = os.environ.get("WM_DB", "database.db")

# Kafka (ver los formatos de cada mensaje en las funciones que los envían)
TOPIC_EVENTOS = "WM_eventos"        # Engine -> Central
TOPIC_ORDENES = "WM_ordenes"        # Central -> Engine
TOPIC_PETICIONES = "WM_peticiones"  # FO -> Central
TOPIC_RESPUESTAS = "WM_respuestas"  # Central -> FO
TOPICS = (TOPIC_EVENTOS, TOPIC_ORDENES, TOPIC_PETICIONES, TOPIC_RESPUESTAS)
REINTENTO_KAFKA = 3     # segundos entre intentos de conexión con el broker
# Si la central se reinicia, el broker tarda como mucho esto en dar por muerta la
# sesión anterior del grupo y repartirle las particiones a la nueva
SESION_KAFKA_MS = 6000
OPERARIO_LOCAL = "LOCAL"    # riego lanzado desde el menú del engine (no hay FO al que avisar)

# Si una WS pasa a fuga o se desconecta regando, se espera este tiempo al RIEGO_FIN
# del engine. Si no llega (el engine ha muerto) la central cierra el riego con lo
# último que sabe y avisa al operario. Si el RIEGO_FIN llega después, lo corrige.
ESPERA_RIEGO_FIN = 5
CIERRES_PROVISIONALES = ("AVERIA", "DESCONEXION")
logging.getLogger("kafka").setLevel(logging.CRITICAL)

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

conexion = sqlite3.connect(DB_PATH)
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
# Solo se llena si hay panel de escritorio (si no, crecería sin que nadie la vacíe).
cola_log = queue.Queue()
hay_gui = False

# Últimas líneas del registro para el panel web
historial_log = deque(maxlen=300)

# Productor de Kafka (None hasta que el broker responde)
productor = None

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
    linea = f"{time.strftime('%H:%M:%S')}  {mensaje}"
    historial_log.append(linea)
    if hay_gui:
        cola_log.put(linea)

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
    conexion = sqlite3.connect(DB_PATH)
    cursor = conexion.cursor()

    cursor.execute("""
        UPDATE estaciones
        SET Status = 'OFFLINE', Flow = NULL, Accum = NULL, Operator = NULL
    """)
    # Los riegos que quedaron abiertos NO se cierran aquí: su RIEGO_FIN sigue en
    # Kafka esperando a la central (grupo WM_Central) y los cerrará con los datos reales
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
    conexion = sqlite3.connect(DB_PATH)
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
    conexion = sqlite3.connect(DB_PATH)
    cursor = conexion.cursor()

    cursor.execute("SELECT * FROM estaciones")

    estaciones_bd = cursor.fetchall()

    for estacion in estaciones_bd:
        print(estacion)

    conexion.close()

def modificar_estado(id, estado):
    conexion = sqlite3.connect(DB_PATH)
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
    conexion = sqlite3.connect(DB_PATH)
    cursor = conexion.cursor()
    cursor.execute("SELECT ID FROM operarios WHERE ID = ?", (id_operario,))
    existe = cursor.fetchone() is not None
    conexion.close()
    return existe

def iniciar_riego_bd(id_estacion, id_operario, limite):
    # Se llama cuando el engine confirma que ha abierto la electroválvula (RIEGO_INICIO)
    conexion = sqlite3.connect(DB_PATH)
    cursor = conexion.cursor()
    # Un riego anterior de esta estación que nunca recibió su fin ya no va a llegar
    cursor.execute("""
        UPDATE riegos
        SET Fin = ?, Motivo = 'SIN_CIERRE'
        WHERE Estacion = ? AND Fin IS NULL
    """, (ahora(), id_estacion))
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
    conexion = sqlite3.connect(DB_PATH)
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
    conexion = sqlite3.connect(DB_PATH)
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

def corregir_riego_bd(id_estacion, volumen, duracion, motivo):
    # El último riego de la estación lo cerró la central sin datos del engine
    # (AVERIA o DESCONEXION) y ahora llega el RIEGO_FIN real: se guardan sus datos.
    # Devuelve True si había un riego que corregir.
    conexion = sqlite3.connect(DB_PATH)
    cursor = conexion.cursor()
    cursor.execute("SELECT ID, Motivo FROM riegos WHERE Estacion = ? ORDER BY ID DESC", (id_estacion,))
    fila = cursor.fetchone()
    corregido = fila is not None and fila[1] in CIERRES_PROVISIONALES
    if corregido:
        cursor.execute("""
            UPDATE riegos
            SET Volumen = ?, Duracion = ?, Motivo = ?
            WHERE ID = ?
        """, (volumen, duracion, f"{fila[1]}/{motivo}", fila[0]))
        conexion.commit()
    conexion.close()
    return corregido

def operario_riego_abierto(id_estacion):
    # Operario del riego que la BD tiene abierto en la estación (o None)
    conexion = sqlite3.connect(DB_PATH)
    cursor = conexion.cursor()
    cursor.execute("SELECT Operario FROM riegos WHERE Estacion = ? AND Fin IS NULL ORDER BY ID DESC", (id_estacion,))
    fila = cursor.fetchone()
    conexion.close()
    return fila[0] if fila else None

########## EVENTOS DE RIEGO QUE ENVÍA EL ENGINE POR KAFKA ##########
# Las llama escuchar_eventos al recibir RIEGO_INICIO, TELEMETRIA y RIEGO_FIN de WM_WS_E.
# Además de la BD actualizan el diccionario, que es lo que pinta el panel.

def riego_iniciado(id_estacion, id_operario, limite):
    with lock_estaciones:
        if id_estacion not in estaciones:
            return
        estacion = estaciones[id_estacion]
        # Si el monitor no está conectado (evento atrasado tras reiniciar la central)
        # se guarda el riego pero la estación sigue Desconectada en el panel
        if estacion["estado"] != "OFFLINE":
            estacion["estado"] = "WATERING"
        estacion["flow"] = 0.0
        estacion["accum"] = 0.0
        estacion["operator"] = id_operario
        estacion["inicio"] = time.time()
        bloqueada = estacion["bloqueada"]
        iniciar_riego_bd(id_estacion, id_operario, limite)
    log(f"[RIEGO] '{id_estacion}' empieza a regar. Operario: {id_operario}, límite {limite} s")
    notificar_operario(id_operario, f"INICIO#{id_operario}#{id_estacion}#{limite}")
    # Un engine reiniciado no sabe que estaba bloqueado y puede regar a mano: se le recuerda
    if bloqueada:
        log(f"[BLOQUEO] '{id_estacion}' está bloqueada: se repite la orden")
        enviar_orden(id_estacion, f"BLOQUEAR#{id_estacion}")

def telemetria_recibida(id_estacion, caudal, volumen):
    with lock_estaciones:
        if id_estacion not in estaciones:
            return
        estacion = estaciones[id_estacion]
        estacion["flow"] = caudal
        estacion["accum"] = volumen
        # El monitor se ha vuelto a registrar (AVAILABLE) mientras el engine seguía
        # regando, o la central se reinició a mitad del riego: el estado real es WATERING
        if estacion["estado"] == "AVAILABLE":
            estacion["estado"] = "WATERING"
            modificar_estado(id_estacion, "WATERING")
        if not estacion.get("operator"):
            estacion["operator"] = operario_riego_abierto(id_estacion)
        id_operario = estacion.get("operator")
    actualizar_telemetria_bd(id_estacion, caudal, volumen)
    # Cada operario solo recibe los datos de la estación que le está regando (punto 10)
    if id_operario:
        notificar_operario(id_operario, f"TELEMETRIA#{id_operario}#{id_estacion}#{caudal}#{volumen}")

def riego_finalizado(id_estacion, id_operario, volumen, duracion, motivo):
    with lock_estaciones:
        if id_estacion not in estaciones:
            return None
        estacion = estaciones[id_estacion]
        resumen = finalizar_riego_bd(id_estacion, volumen, duracion, motivo)
        if resumen is None and corregir_riego_bd(id_estacion, volumen, duracion, motivo):
            log(f"[RIEGO] '{id_estacion}': llega el fin real de un riego que la central había cerrado sin datos")
        estacion["flow"] = estacion["accum"] = estacion["operator"] = None
        # Vuelve a reposo salvo que la haya cambiado otro evento (fuga, bloqueo, desconexión)
        if estacion["estado"] == "WATERING":
            estacion["estado"] = "STOPPED" if estacion["bloqueada"] else "AVAILABLE"
            modificar_estado(id_estacion, estacion["estado"])
    log(f"[RIEGO] '{id_estacion}' termina ({motivo}): {volumen} l en {duracion} s")
    # Resumen final para el operario que pidió el riego (punto 8)
    notificar_operario(id_operario, f"RESUMEN#{id_operario}#{id_estacion}#{volumen}#{duracion}#{motivo}")
    return resumen

def riego_interrumpido(estacion, motivo):
    # La estación deja de regar sin que lo haya dicho el engine (fuga o desconexión).
    # Se llama con lock_estaciones cogido. Se limpia la telemetría del panel y se
    # programa el cierre provisional por si el engine no llega a mandar su RIEGO_FIN.
    inicio = estacion.get("inicio")
    duracion = int(time.time() - inicio) if inicio else None    # hasta ahora, no hasta el cierre
    datos = (estacion["id"], estacion.get("operator"), estacion.get("accum"), duracion, motivo)
    estacion["flow"] = estacion["accum"] = estacion["operator"] = None
    threading.Timer(ESPERA_RIEGO_FIN, cierre_provisional, args=datos).start()

def cierre_provisional(id_estacion, id_operario, volumen, duracion, motivo):
    resumen = finalizar_riego_bd(id_estacion, volumen, duracion, motivo)
    if resumen is None:
        return      # el RIEGO_FIN del engine ya ha cerrado el riego
    volumen_txt = f"{volumen:.2f}" if volumen is not None else "-"
    log(f"[RIEGO] '{id_estacion}' sin noticias del engine: riego cerrado por la central ({motivo}), {volumen_txt} l")
    id_operario = id_operario or resumen["operario"]
    notificar_operario(id_operario, f"RESUMEN#{id_operario}#{id_estacion}#{volumen_txt}#{duracion if duracion is not None else '-'}#{motivo}")

########## KAFKA ##########

def crear_topics(bootstrap):
    # Se crean al arrancar para que los consumidores no esperen a que alguien
    # publique por primera vez (y por si el broker no los crea solo)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    for topic in TOPICS:
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    admin.close()

def conectar_kafka(bootstrap):
    # La central arranca aunque Kafka no esté: los sockets y el panel funcionan igual
    # y se reintenta hasta que el broker conteste.
    global productor
    while productor is None:
        try:
            crear_topics(bootstrap)
            productor = KafkaProducer(bootstrap_servers=bootstrap, max_block_ms=2000)
        except Exception:
            log(f"[KAFKA] ERROR: no se puede conectar con el broker {bootstrap}, reintentando en {REINTENTO_KAFKA}s")
            time.sleep(REINTENTO_KAFKA)
    log(f"[KAFKA] Conectado al broker {bootstrap}")
    threading.Thread(target=escuchar_eventos, args=(bootstrap,), daemon=True).start()
    threading.Thread(target=escuchar_peticiones, args=(bootstrap,), daemon=True).start()

def publicar(topic, clave, mensaje):
    if productor is None:
        log(f"[KAFKA] ERROR: sin conexión con el broker, no se envía {mensaje}")
        return
    try:
        productor.send(topic, key=clave.encode(FORMAT), value=mensaje.encode(FORMAT))
    except Exception as e:
        log(f"[KAFKA] ERROR al enviar {mensaje}: {e}")

def notificar_operario(id_operario, mensaje):
    # Mensajes para WM_FO. Los riegos de la central o del menú del engine no tienen FO.
    if id_operario not in (OPERARIO_CENTRAL, OPERARIO_LOCAL):
        publicar(TOPIC_RESPUESTAS, id_operario, mensaje)

def crear_consumidor(topic, bootstrap):
    # Consumidor del grupo de la central (WM_Central_<topic>). Cada mensaje se
    # confirma (commit) después de procesarlo: lo que se publique con la central
    # caída se queda en Kafka y se procesa al volver, no se pierde ninguna transacción.
    # "latest" solo se usa la primera vez que arranca el grupo.
    return KafkaConsumer(topic, bootstrap_servers=bootstrap, group_id=f"WM_Central_{topic}",
                         auto_offset_reset="latest", enable_auto_commit=False,
                         session_timeout_ms=SESION_KAFKA_MS, heartbeat_interval_ms=SESION_KAFKA_MS // 3)

def escuchar_eventos(bootstrap):
    # Eventos de los engines. Los que llegaron con la central caída (RIEGO_FIN de
    # riegos que estaban en marcha) se procesan ahora y cierran el riego con los datos reales.
    consumidor = crear_consumidor(TOPIC_EVENTOS, bootstrap)
    for registro in consumidor:
        procesar_evento(registro)
        consumidor.commit()

def procesar_evento(registro):
    msg = registro.value.decode(FORMAT)
    partes = msg.split('#')
    try:
        if partes[0] == "RIEGO_INICIO" and len(partes) == 4:
            riego_iniciado(partes[1], partes[2], int(partes[3]))
        elif partes[0] == "TELEMETRIA" and len(partes) == 4:
            telemetria_recibida(partes[1], float(partes[2]), float(partes[3]))
        elif partes[0] == "RIEGO_FIN" and len(partes) == 6:
            riego_finalizado(partes[1], partes[2], float(partes[3]), int(partes[4]), partes[5])
        elif partes[0] == "RECHAZADO" and len(partes) == 4:
            id_estacion, id_operario, motivo = partes[1], partes[2], partes[3]
            log(f"[RIEGO] '{id_estacion}' NO AUTORIZA el riego de {id_operario}: {motivo}")
            notificar_operario(id_operario, f"DENEGADO#{id_operario}#{id_estacion}#la estación lo rechaza: {motivo}")
        else:
            log(f"[KAFKA] Evento no válido: {msg}")
    except ValueError:
        log(f"[KAFKA] Evento con datos no válidos: {msg}")

def escuchar_peticiones(bootstrap):
    # Peticiones de los operarios:
    #   SOLICITUD#id_operario#id_ws#limite_segundos -> RECIBIDA y luego AUTORIZADO o DENEGADO
    #   LISTA#id_operario                            -> LISTA#id_operario#WS_01=Disponible;...
    consumidor = crear_consumidor(TOPIC_PETICIONES, bootstrap)

    # Las peticiones que ya estaban en el topic al arrancar se hicieron con la central
    # caída: el FO ya mostró "Imposible conectar con WM_Central" y no espera ese riego.
    # No se pierden: se contestan con DENEGADO para que el operario sepa qué pasó.
    particiones = consumidor.partitions_for_topic(TOPIC_PETICIONES) or {0}
    fin_atrasadas = consumidor.end_offsets([TopicPartition(TOPIC_PETICIONES, p) for p in particiones])

    for registro in consumidor:
        msg = registro.value.decode(FORMAT)
        partes = msg.split('#')
        atrasada = registro.offset < fin_atrasadas.get(TopicPartition(registro.topic, registro.partition), 0)
        if partes[0] == "SOLICITUD" and len(partes) == 4 and partes[3].isdigit() and int(partes[3]) > 0:
            if atrasada:
                log(f"[PETICIÓN] {partes[1]} pidió regar '{partes[2]}' con la central caída: se deniega")
                notificar_operario(partes[1], f"DENEGADO#{partes[1]}#{partes[2]}#la central estaba caída cuando se pidió, hay que volver a pedirlo")
            else:
                atender_solicitud(partes[1], partes[2], int(partes[3]))
        elif partes[0] == "LISTA" and len(partes) == 2:
            notificar_operario(partes[1], f"LISTA#{partes[1]}#{lista_estaciones()}")
        else:
            log(f"[KAFKA] Petición no válida: {msg}")
        consumidor.commit()

def atender_solicitud(id_operario, id_estacion, limite):
    # Cada paso se notifica al operario y se muestra en el panel (punto 4)
    log(f"[PETICIÓN] {id_operario} pide regar '{id_estacion}' durante {limite} s")
    notificar_operario(id_operario, f"RECIBIDA#{id_operario}#{id_estacion}")
    ok, motivo = autorizar_riego(id_estacion, id_operario)
    if ok:
        log(f"[RIEGO] Autorizado en '{id_estacion}' para {id_operario} ({limite} s)")
        notificar_operario(id_operario, f"AUTORIZADO#{id_operario}#{id_estacion}#{limite}")
        enviar_orden(id_estacion, f"REGAR#{id_estacion}#{id_operario}#{limite}")
    else:
        log(f"[RIEGO] Denegado en '{id_estacion}' a {id_operario}: {motivo}")
        notificar_operario(id_operario, f"DENEGADO#{id_operario}#{id_estacion}#{motivo}")

def lista_estaciones():
    # Para que los operarios vean qué estaciones hay y cuáles están disponibles
    with lock_estaciones:
        return ";".join(f"{e['id']}={TEXTO_ESTADO[e['estado']]}" for e in sorted(estaciones.values(), key=lambda e: e["id"]))

########## ÓRDENES DE LA CENTRAL (punto 11) ##########

def enviar_orden(id_estacion, orden):
    # Orden para el WM_WS_E de esa estación. Formatos:
    #   REGAR#id_ws#operario#limite_segundos
    #   BLOQUEAR#id_ws
    #   ACTIVAR#id_ws
    log(f"[ORDEN -> {id_estacion}] {orden}")
    publicar(TOPIC_ORDENES, id_estacion, orden)

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
            riego_interrumpido(estacion, "AVERIA")

    if estado_anterior == "WATERING" and nuevo_estado == "LEAK":
        log(f"[AVERÍA] Fuga o engine caído en '{id_estacion}' durante un riego: se espera su RIEGO_FIN")
    # Fuga resuelta en una estación bloqueada: el engine puede haberse reiniciado
    # (se pierde el bloqueo que tenía en memoria), así que se le repite la orden
    if estado_anterior == "LEAK" and nuevo_estado == "STOPPED":
        enviar_orden(id_estacion, f"BLOQUEAR#{id_estacion}")
    return nuevo_estado

def desconectar_estacion(id_estacion):
    with lock_estaciones:
        estacion = estaciones[id_estacion]
        estado_anterior = estacion["estado"]
        estacion["estado"] = "OFFLINE"
        estacion["conn"] = None
        estacion["addr"] = None
        modificar_estado(id_estacion, "OFFLINE")
        # Si estaba regando ya no se puede monitorizar: el engine, al perder el
        # monitor, corta el riego y manda su RIEGO_FIN; si no llega, lo cierra la central
        if estado_anterior == "WATERING":
            riego_interrumpido(estacion, "DESCONEXION")
        else:
            estacion["flow"] = estacion["accum"] = estacion["operator"] = None

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
            # Si sigue bloqueada, el engine (quizá reiniciado) tiene que saberlo
            if estaciones[id_estacion]["bloqueada"]:
                enviar_orden(id_estacion, f"BLOQUEAR#{id_estacion}")
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
            _pil_m = Image.open(os.path.join(CARPETA, _nmap))
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
            ac = f"{e.get('accum'):.2f} L" if e.get('accum') is not None else "-"
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


########## PANEL WEB ##########
# En Railway no hay pantalla, así que el panel se sirve también por HTTP.
# La página pide /api/estado cada segundo y manda las órdenes a /api/orden.

PAGINA_WEB = """<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WM Central</title>
<style>
  * { box-sizing: border-box; }
  body { margin: 0; font-family: "Segoe UI", system-ui, sans-serif; background: #eef1f5; color: #1f2933; }
  header { background: #1b2a3a; color: #fff; padding: 12px 20px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  header h1 { font-size: 18px; margin: 0; }
  header small { color: #9fb3c8; display: block; font-size: 12px; }
  header .estado { margin-left: auto; font-size: 13px; font-weight: 600; }
  main { padding: 14px; display: grid; gap: 14px; grid-template-columns: minmax(0, 1fr) minmax(0, 520px); }
  @media (max-width: 1000px) { main { grid-template-columns: minmax(0, 1fr); } }
  .kpis { grid-column: 1 / -1; display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; }
  .kpi, .tarjeta { background: #fff; border: 1px solid #d9dee5; border-radius: 6px; }
  .kpi { padding: 8px 12px; border-left: 4px solid var(--c); }
  .kpi b { font-size: 24px; display: block; }
  .kpi span { font-size: 12px; color: #6b7785; }
  .tarjeta { padding: 12px 14px; overflow: hidden; }
  .tarjeta h2 { font-size: 12px; color: #6b7785; margin: 0 0 8px; letter-spacing: .05em; }
  .columna { display: grid; gap: 14px; align-content: start; min-width: 0; }
  .tabla { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th { text-align: left; font-size: 11px; color: #6b7785; padding: 8px; background: #f5f7fa; }
  td { padding: 8px; border-top: 1px solid #fff; white-space: nowrap; }
  .AVAILABLE { background: #c8e6c9; color: #1b5e20; }
  .WATERING { background: #43a047; color: #fff; animation: parpadeo 1s steps(1) infinite; }
  .LEAK { background: #e53935; color: #fff; }
  .STOPPED { background: #ffcc80; color: #4e2a00; }
  .OFFLINE { background: #eceff1; color: #5f6b76; }
  @keyframes parpadeo { 50% { background: #e8f5e9; color: #1b5e20; } }
  .controles { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  select, input, button { font: inherit; padding: 6px 10px; border: 1px solid #d9dee5; border-radius: 4px; }
  input { width: 90px; }
  button { color: #fff; border: 0; font-weight: 600; cursor: pointer; }
  .regar { background: #2e7d32; } .bloquear { background: #ef6c00; } .activar { background: #1e88e5; }
  #log { background: #1e2836; color: #d8dee9; font: 12px Consolas, monospace; height: 260px; overflow-y: auto; padding: 8px; white-space: pre-wrap; margin: 0; }
  .error { color: #ff6b6b; } .aviso { color: #ffb454; } .ok { color: #7ed491; } .info { color: #7fb8ff; }
  #mapa { position: relative; width: 500px; max-width: 100%; aspect-ratio: 5 / 4; background: #f5f7fa url(/mapa) 0 0 / 100% 100%; border: 1px solid #d9dee5; }
  .punto { position: absolute; width: 20px; height: 20px; margin: -10px 0 0 -10px; border-radius: 50%; border: 1px solid #1b2a3a; font: bold 6px sans-serif; display: flex; align-items: center; justify-content: center; }
</style></head>
<body>
<header><span style="font-size:24px">&#128167;</span>
  <div><h1>WM Central</h1><small>Monitor de Estaciones de Riego &middot; Invernalia</small></div>
  <span class="estado" id="estado">Conectando...</span></header>
<main>
  <section class="kpis" id="kpis"></section>
  <div class="columna">
    <div class="tarjeta"><h2>ESTADO DE LAS ESTACIONES</h2><div class="tabla"><table>
      <thead><tr><th>ID</th><th>UBICACIÓN</th><th>SECTOR</th><th>ESTADO</th><th>CAUDAL</th><th>ACUMULADO</th><th>OPERARIO</th></tr></thead>
      <tbody id="filas"></tbody></table></div></div>
    <div class="tarjeta"><h2>ACCIONES DE CONTROL</h2><div class="controles">
      <label>Estación <select id="destino"><option>TODAS</option></select></label>
      <label>Límite (s) <input id="limite" type="number" min="1" value="60"></label>
      <button class="regar" onclick="orden('REGAR')">REGAR</button>
      <button class="bloquear" onclick="orden('BLOQUEAR')">BLOQUEAR</button>
      <button class="activar" onclick="orden('ACTIVAR')">ACTIVAR</button></div></div>
    <div class="tarjeta"><h2>REGISTRO DE EVENTOS</h2><pre id="log"></pre></div>
  </div>
  <div class="columna"><div class="tarjeta"><h2>MAPA DE SECTORES</h2><div id="mapa"></div></div></div>
</main>
<script>
const KPIS = [["TOTAL", "Estaciones", "#1e88e5"], ["AVAILABLE", "Disponible", "#43a047"], ["WATERING", "Regando", "#2e7d32"],
              ["LEAK", "Fuga", "#e53935"], ["STOPPED", "Fuera de Servicio", "#fb8c00"], ["OFFLINE", "Desconectada", "#9e9e9e"]];

function celda(fila, texto) { const td = document.createElement("td"); td.textContent = texto; fila.appendChild(td); }

function tipoLog(l) {
  const t = l.toUpperCase();
  if (["KO]", "AVERÍA", "FUGA", "ERROR", "DENEG", "NO SE PUEDE", "NO AUTORIZ"].some(p => t.includes(p))) return "error";
  if (["[BLOQUEO]", "[DESCONEXION]", "[LATIDO]", "OFFLINE"].some(p => t.includes(p))) return "aviso";
  if (["[RIEGO]", "[ACTIVAR]", "REGISTRO OK"].some(p => t.includes(p))) return "ok";
  if (["[ORDEN", "[ESTADO]", "[KAFKA]", "[PETICIÓN]"].some(p => t.includes(p))) return "info";
  return "";
}

let ultimoLog = "";
async function refrescar() {
  try {
    const d = await (await fetch("/api/estado")).json();
    document.getElementById("estado").textContent = "● En servicio · sockets " + d.puerto + (d.kafka ? " · Kafka OK" : " · Kafka sin conexión");
    const cuenta = {TOTAL: d.estaciones.length};
    d.estaciones.forEach(e => cuenta[e.estado] = (cuenta[e.estado] || 0) + 1);
    document.getElementById("kpis").innerHTML = KPIS.map(([k, t, c]) =>
      `<div class="kpi" style="--c:${c}"><b>${cuenta[k] || 0}</b><span>${t}</span></div>`).join("");

    const filas = document.getElementById("filas"), mapa = document.getElementById("mapa");
    filas.textContent = ""; mapa.textContent = "";
    const destino = document.getElementById("destino"), elegido = destino.value;
    destino.length = 1;
    d.estaciones.forEach(e => {
      const tr = document.createElement("tr"); tr.className = e.estado;
      celda(tr, e.id); celda(tr, e.ubicacion); celda(tr, e.sector ? "(" + e.sector + ")" : "-"); celda(tr, e.texto);
      celda(tr, e.flow == null ? "-" : e.flow.toFixed(1) + " L/min");
      celda(tr, e.accum == null ? "-" : e.accum.toFixed(2) + " L");
      celda(tr, e.operator || "-");
      filas.appendChild(tr);
      if (e.estado != "OFFLINE") destino.add(new Option(e.id));
      if (e.sector) {
        const p = document.createElement("div"); p.className = "punto " + e.estado; p.textContent = e.id;
        p.style.left = e.sector[0] / 5 + "%"; p.style.top = e.sector[1] / 4 + "%";
        mapa.appendChild(p);
      }
    });
    destino.value = [...destino.options].some(o => o.value == elegido) ? elegido : "TODAS";

    const log = document.getElementById("log"), texto = d.log.join("\\n");
    if (texto != ultimoLog) {
      ultimoLog = texto; log.textContent = "";
      d.log.forEach(l => { const s = document.createElement("span"); s.className = tipoLog(l); s.textContent = l + "\\n"; log.appendChild(s); });
      log.scrollTop = log.scrollHeight;
    }
  } catch (err) {
    document.getElementById("estado").textContent = "Sin conexión con la central";
  }
}

async function orden(accion) {
  await fetch("/api/orden", {method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({accion: accion, estacion: document.getElementById("destino").value,
                          limite: parseInt(document.getElementById("limite").value)})});
  refrescar();
}

refrescar();
setInterval(refrescar, 1000);
</script>
</body></html>
"""

def buscar_mapa():
    # Mismos nombres que el panel de escritorio; el PNG va primero porque pesa menos
    for nombre in ("InvernaliaZonas.png", "InvernaliaZonas.jpg"):
        ruta = os.path.join(CARPETA, nombre)
        if os.path.exists(ruta):
            return ruta
    return None

class PanelWeb(BaseHTTPRequestHandler):
    def responder(self, codigo, tipo, cuerpo):
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(cuerpo)))
        self.end_headers()
        self.wfile.write(cuerpo)

    def do_GET(self):
        if self.path == "/":
            self.responder(200, "text/html; charset=utf-8", PAGINA_WEB.encode(FORMAT))
        elif self.path == "/api/estado":
            with lock_estaciones:
                lista = [{
                    "id": e["id"], "ubicacion": e["ubicacion"], "sector": e.get("sector"),
                    "estado": e["estado"], "texto": TEXTO_ESTADO.get(e["estado"], e["estado"]),
                    "flow": e.get("flow"), "accum": e.get("accum"), "operator": e.get("operator")
                } for e in sorted(estaciones.values(), key=lambda e: e["id"])]
            datos = {"puerto": PORT, "kafka": productor is not None, "estaciones": lista, "log": list(historial_log)}
            self.responder(200, "application/json", json.dumps(datos).encode(FORMAT))
        elif self.path == "/mapa" and buscar_mapa():
            ruta = buscar_mapa()
            with open(ruta, "rb") as f:
                tipo = "image/png" if ruta.endswith(".png") else "image/jpeg"
                self.responder(200, tipo, f.read())
        else:
            self.responder(404, "text/plain", b"No encontrado")

    def do_POST(self):
        if self.path != "/api/orden":
            self.responder(404, "text/plain", b"No encontrado")
            return
        try:
            datos = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            accion, destino = datos["accion"], datos["estacion"]
            with lock_estaciones:
                existe = destino == TODAS or destino in estaciones
            if not existe:
                raise ValueError(f"estación desconocida {destino}")
            if accion == "REGAR":
                limite = int(datos["limite"])
                if limite <= 0:
                    raise ValueError("el límite tiene que ser positivo")
                aplicar_orden(destino, orden_iniciar_riego, limite)
            elif accion == "BLOQUEAR":
                aplicar_orden(destino, orden_bloquear)
            elif accion == "ACTIVAR":
                aplicar_orden(destino, orden_activar)
            else:
                raise ValueError(f"acción desconocida {accion}")
            self.responder(200, "application/json", b'{"ok": true}')
        except (ValueError, KeyError, TypeError) as e:
            self.responder(400, "application/json", json.dumps({"ok": False, "error": str(e)}).encode(FORMAT))

    def log_message(self, *args):
        pass    # sin una línea por cada petición (la página refresca cada segundo)


if __name__ == "__main__":
    if len(sys.argv) != 4 or not sys.argv[1].isdigit() or not sys.argv[3].isdigit():
        print("Uso: python WM_Central.py <puerto_sockets> <ip_broker> <puerto_broker>")
        print("Ejemplo: python WM_Central.py 5050 127.0.0.1 9092")
        print("El panel web escucha en la variable de entorno PORT (8080 si no existe)")
        sys.exit(1)

    # 0.0.0.0: se aceptan conexiones por cualquier interfaz (necesario en Docker/Railway)
    SERVER = "0.0.0.0"
    PORT = int(sys.argv[1])
    bootstrap = f"{sys.argv[2]}:{sys.argv[3]}"
    PUERTO_WEB = int(os.environ.get("PORT", 8080))

    # Panel de escritorio solo si hay Tkinter y pantalla (WM_GUI=0 lo desactiva)
    if tk is not None and os.environ.get("WM_GUI", "1") != "0":
        try:
            tk.Tk().destroy()
            hay_gui = True
        except tk.TclError:
            pass

    cargar_estaciones_bd()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((SERVER, PORT))
    t_server = threading.Thread(target=start, args=(server_sock, SERVER, PORT), daemon=True)
    t_server.start()
    threading.Thread(target=conectar_kafka, args=(bootstrap,), daemon=True).start()

    panel_web = ThreadingHTTPServer(("0.0.0.0", PUERTO_WEB), PanelWeb)
    log(f"[PANEL] Panel web en http://localhost:{PUERTO_WEB}")
    if hay_gui:
        threading.Thread(target=panel_web.serve_forever, daemon=True).start()
        Pantalla_Monitor()
    else:
        panel_web.serve_forever()