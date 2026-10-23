# Guía de despliegue – WaterManagement

Escenario de la corrección:

| Dónde | Qué |
|---|---|
| Railway (cloud) | Kafka, WM_Central y su BD SQLite |
| PC de laboratorio 1 | Varios WM_FO (Docker) |
| PC de laboratorio 2 | Varias WS: WM_WS_M + WM_WS_E (Docker) |

## Comunicaciones

| De → A | Tecnología | Mensajes |
|---|---|---|
| WM_WS_E ↔ WM_WS_M | Sockets `<STX><DATA><ETX><LRC>` | `ID#id_ws`, `HEALTH` → `OK` / `KO` |
| WM_WS_M → WM_Central | Sockets `<STX><DATA><ETX><LRC>` | `REGISTER#id#ubicacion`, `ESTADO#id#estado`, `PING#id`, `FIN` |
| WM_WS_E → WM_Central | Kafka `WM_eventos` | `RIEGO_INICIO#ws#op#limite`, `TELEMETRIA#ws#caudal#volumen`, `RIEGO_FIN#ws#op#volumen#duracion#motivo`, `RECHAZADO#ws#op#motivo` |
| WM_Central → WM_WS_E | Kafka `WM_ordenes` | `REGAR#ws#op#limite`, `BLOQUEAR#ws`, `ACTIVAR#ws` |
| WM_FO → WM_Central | Kafka `WM_peticiones` | `SOLICITUD#op#ws#limite`, `LISTA#op` |
| WM_Central → WM_FO | Kafka `WM_respuestas` | `RECIBIDA`, `AUTORIZADO`, `DENEGADO`, `INICIO`, `TELEMETRIA`, `RESUMEN`, `LISTA` (todos con `#op#ws#...`) |

Cada mensaje de Kafka lleva como clave el ID de la estación o del operario; cada
engine y cada FO lee el topic completo y se queda solo con los mensajes que llevan su ID.

## 1. Railway

Todo en el mismo proyecto de Railway.

### 1.1 Kafka
1. **+ New → Template → Kafka** (apache/kafka, KRaft) → Deploy.
2. Servicio Kafka → **Settings → Networking → TCP Proxy** → puerto `9092`.
   Apuntar el host y puerto: `KAFKA_HOST` = `xxxxx.proxy.rlwy.net`, `KAFKA_PORT` = `NNNNN`.
3. En **Variables**, comprobar que `KAFKA_ADVERTISED_LISTENERS` anuncia ese host:puerto público
   (si anuncia `*.railway.internal`, los clientes de fuera conectan pero luego dan timeout).
4. Prueba desde el PC: `python test_kafka.py KAFKA_HOST:KAFKA_PORT` (repo railway-hello-world).

### 1.2 WM_Central
1. **+ New → GitHub Repo →** este repositorio. Railway usa el `Dockerfile` de la raíz.
   En **Settings → Source** elegir la rama que se despliega (main).
2. **Variables** del servicio:

   | Variable | Valor |
   |---|---|
   | `KAFKA_HOST` | host del TCP Proxy de Kafka |
   | `KAFKA_PORT` | puerto del TCP Proxy de Kafka |
   | `PUERTO_SOCKETS` | `5050` |
   | `WM_DB` | `/data/database.db` |

3. **Volumen** para que la BD sobreviva a los despliegues: clic derecho en el servicio
   (o Settings) → **Attach Volume** → mount path `/data`.
4. **Settings → Networking**:
   - **TCP Proxy** → puerto `5050`. Es la dirección que usarán los monitores: `CENTRAL_HOST:CENTRAL_PORT`.
   - **Generate Domain** → puerto `8080` (o el que salga en el log `[PANEL] Panel web en ...`).
     Es la URL del panel web de la central.
5. En los logs del despliegue debe aparecer `[KAFKA] Conectado al broker ...`.

Cada `git push` a la rama configurada vuelve a desplegar la central (CI/CD).

## 2. Laboratorio (Docker)

En cada PC, desde la carpeta del repositorio:

```
docker build -f Dockerfile.ws -t wm-ws .     # PC de las estaciones
docker build -f Dockerfile.fo -t wm-fo .     # PC de los operarios
```

### 2.1 Estaciones (PC 2)
Monitor y engine van en contenedores distintos dentro de una red de Docker, para que el
engine encuentre al monitor por su nombre. Un terminal por contenedor:

```
docker network create wm

# WS_01
docker run -it --rm --network wm --name ws01-m wm-ws python WM_WS_M.py 8000 CENTRAL_HOST CENTRAL_PORT WS_01 Bosque_Dioses
docker run -it --rm --network wm --name ws01-e wm-ws python WM_WS_E.py KAFKA_HOST KAFKA_PORT ws01-m 8000

# WS_02
docker run -it --rm --network wm --name ws02-m wm-ws python WM_WS_M.py 8000 CENTRAL_HOST CENTRAL_PORT WS_02 Patio_Armas
docker run -it --rm --network wm --name ws02-e wm-ws python WM_WS_E.py KAFKA_HOST KAFKA_PORT ws02-m 8000
```

Ubicaciones válidas: `Invernadero`, `Cripta`, `Bosque_Dioses`, `Patio_Este`, `Patio_Armas`, `Establos`, `Septo`.

Menú del engine: `R <segundos>` riego manual, `P` parar, `K` provocar fuga, `O` resolver fuga, `FIN`.

### 2.2 Operarios (PC 1)
```
docker run -it --rm wm-fo python WM_FO.py KAFKA_HOST KAFKA_PORT FO_01 peticiones.txt
docker run -it --rm wm-fo python WM_FO.py KAFKA_HOST KAFKA_PORT FO_02
```

Operarios dados de alta: `FO_01`, `FO_02`, `FO_03`.
Menú: `R <estacion> <segundos>`, `L` (estado de las estaciones), `F <fichero>`, `FIN`.

Fichero de peticiones (`peticiones.txt`): una por línea con el formato `id_estacion#segundos`;
las líneas que empiezan por `//` se ignoran. Tras cada petición (con éxito o no) espera 4 s.

## 3. Pruebas en local (sin Railway ni Docker)

Kafka local en `localhost:9092` (ver "ARRANCAR KAFKA") y un terminal por módulo:

```
python Central/WM_Central.py 5050 localhost 9092
python WM_WS_M.py 8000 127.0.0.1 5050 WS_01 Bosque_Dioses
python WM_WS_E.py localhost 9092 127.0.0.1 8000
python WM_FO.py localhost 9092 FO_01 peticiones.txt
```

En local se abre el panel de escritorio (Tkinter) y además el panel web en http://localhost:8080.
