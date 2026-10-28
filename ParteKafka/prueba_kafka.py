# Comprueba desde cualquier PC del aula que llega al broker de Kafka antes de
# arrancar la práctica: conecta, publica un mensaje y lo vuelve a leer.
#   Uso:     python prueba_kafka.py <ip_broker> <puerto_broker>
#   Ejemplo: python prueba_kafka.py 172.20.43.15 9092
import sys
import time
import logging
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient   # pip install kafka-python

TOPIC_PRUEBA = "WM_prueba"
logging.getLogger("kafka").setLevel(logging.CRITICAL)

if len(sys.argv) != 3 or not sys.argv[2].isdigit():
    print("Uso: python prueba_kafka.py <ip_broker> <puerto_broker>")
    sys.exit(1)

bootstrap = f"{sys.argv[1]}:{sys.argv[2]}"
try:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    brokers = admin.describe_cluster()["brokers"]
    print(f"[OK] Conectado a {bootstrap}. Topics: {sorted(t for t in admin.list_topics() if not t.startswith('__'))}")
    admin.close()
except Exception as e:
    print(f"[ERROR] No se llega a {bootstrap}: {e}")
    print("        ¿Está arrancado Kafka? ¿Firewall de Windows abierto en el puerto 9092?")
    sys.exit(1)

# La dirección que anuncia el broker es la que usan los clientes después del primer contacto
for b in brokers:
    print(f"[OK] El broker se anuncia como {b['host']}:{b['port']}")
    if b["host"] in ("localhost", "127.0.0.1") and sys.argv[1] not in ("localhost", "127.0.0.1"):
        print("[ERROR] Anuncia localhost: desde otro PC fallará. Arráncalo con arrancar_kafka.bat <IP>")
        sys.exit(1)

marca = f"prueba {time.strftime('%H:%M:%S')}"
try:
    productor = KafkaProducer(bootstrap_servers=bootstrap)
    productor.send(TOPIC_PRUEBA, marca.encode()).get(timeout=10)
    productor.close()
    consumidor = KafkaConsumer(TOPIC_PRUEBA, bootstrap_servers=bootstrap, group_id=None,
                               auto_offset_reset="earliest", consumer_timeout_ms=5000)
    leidos = [r.value.decode() for r in consumidor]
    consumidor.close()
except Exception as e:
    print(f"[ERROR] Conecta pero no se puede publicar o leer: {e}")
    sys.exit(1)

if marca in leidos:
    print(f"[OK] Publicado y leído '{marca}'. Kafka funciona desde este PC.")
else:
    print("[ERROR] Se ha publicado pero no se ha podido leer el mensaje")
    sys.exit(1)
