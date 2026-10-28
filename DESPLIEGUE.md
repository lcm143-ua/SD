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

## 3. Prueba en el aula (sin Railway): 1 PC central + 2 PCs

| PC | Qué arranca |
|---|---|
| PC A (el nuestro) | Kafka + WM_Central (panel Tkinter y web) |
| PC B | WS_01 y WS_02 (monitor + engine de cada una) y FO_01 |
| PC C | WS_03 y WS_04 y FO_02 (con `peticiones.txt`) |

En todos: `pip install -r requirements.txt` y el repositorio actualizado (`git pull`).
`IP_A` es la IPv4 del PC A en la red del aula (`ipconfig`).

### 3.1 PC A
1. Abrir el firewall de Windows (PowerShell como administrador, una sola vez):
   ```
   netsh advfirewall firewall add rule name="WM Kafka" dir=in action=allow protocol=TCP localport=9092
   netsh advfirewall firewall add rule name="WM Central" dir=in action=allow protocol=TCP localport=5050,8080
   ```
2. Kafka (deja la ventana abierta):
   ```
   ParteKafka\arrancar_kafka.bat IP_A
   ```
   El script anuncia `IP_A:9092` a los clientes. Con el `localhost` de `server.properties` los otros
   PCs conectan pero luego dan timeout. Además evita los dos fallos de Kafka en Windows 11
   (el `wmic` que ya no existe y el cierre del broker al borrar segmentos viejos).
   **No borrar topics** con Kafka en Windows: también tumba el broker.
3. Central: `python Central/WM_Central.py 5050 IP_A 9092`

### 3.2 PCs B y C
1. Comprobar Kafka antes de nada: `python ParteKafka/prueba_kafka.py IP_A 9092` (tiene que terminar en `[OK]`).
2. Un terminal por proceso. El engine va en el mismo PC que su monitor (`127.0.0.1`) y cada WS usa
   un puerto de engine distinto:
   ```
   python WM_WS_M.py 8001 IP_A 5050 WS_01 Bosque_Dioses
   python WM_WS_E.py IP_A 9092 127.0.0.1 8001
   python WM_WS_M.py 8002 IP_A 5050 WS_02 Patio_Armas
   python WM_WS_E.py IP_A 9092 127.0.0.1 8002
   python WM_FO.py IP_A 9092 FO_01
   ```
   Cada engine guarda su riego en curso en `riego_WS_xx.json` en la carpeta desde la que se lanza:
   para probar la caída del engine hay que volver a lanzarlo desde la misma carpeta.

## 4. Pruebas en local (sin Railway ni Docker)

Kafka local con `ParteKafka\arrancar_kafka.bat localhost` y un terminal por módulo:

```
python Central/WM_Central.py 5050 localhost 9092
python WM_WS_M.py 8000 127.0.0.1 5050 WS_01 Bosque_Dioses
python WM_WS_E.py localhost 9092 127.0.0.1 8000
python WM_FO.py localhost 9092 FO_01 peticiones.txt
```

En local se abre el panel de escritorio (Tkinter) y además el panel web en http://localhost:8080.
