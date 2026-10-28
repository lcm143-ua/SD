@echo off
rem Arranca el broker de Kafka (KRaft, sin ZooKeeper) en este PC para que lo usen
rem los demas PCs del aula.
rem   Uso:     arrancar_kafka.bat <IP_de_este_PC>
rem   Ejemplo: arrancar_kafka.bat 172.20.43.15
rem La IP sale de "ipconfig" (Direccion IPv4 de la red del aula).
rem
rem Por que hace falta la IP: el broker le dice a cada cliente a que direccion
rem tiene que conectarse despues del primer contacto (advertised.listeners).
rem Con el "localhost" que trae server.properties los PCs del aula conectan,
rem pero luego intentan hablar con su propio localhost y dan timeout.
rem
rem Busca Kafka en KAFKA_HOME (vale C:\kafka o C:\kafka\kafka_2.13-4.1.0) o en C:\kafka.

setlocal
if "%~1"=="" (
    echo Uso: arrancar_kafka.bat ^<IP_de_este_PC^>
    echo Ejemplo: arrancar_kafka.bat 172.20.43.15
    exit /b 1
)

set KAFKA_DIR=
if not "%KAFKA_HOME%"=="" if exist "%KAFKA_HOME%\bin\windows" set KAFKA_DIR=%KAFKA_HOME%
if "%KAFKA_DIR%"=="" if not "%KAFKA_HOME%"=="" for /d %%d in ("%KAFKA_HOME%\kafka_*") do set KAFKA_DIR=%%d
if "%KAFKA_DIR%"=="" for /d %%d in ("C:\kafka\kafka_*") do set KAFKA_DIR=%%d
if "%KAFKA_DIR%"=="" (
    echo No encuentro Kafka. Indica la carpeta: set KAFKA_HOME=C:\ruta\kafka_2.13-4.1.0
    exit /b 1
)
echo Kafka en %KAFKA_DIR%
cd /d "%KAFKA_DIR%"

rem kafka-server-start.bat usa "wmic" para elegir la memoria y en Windows 11 ya no
rem existe: el script termina sin decir nada. Fijando la memoria se salta esa parte.
if "%KAFKA_HEAP_OPTS%"=="" set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G

rem Kafka recien descomprimido: hay que formatear la carpeta de logs (log.dirs) una vez.
rem Si ya tiene meta.properties no se toca: formatearla otra vez borraria los topics.
for /f "tokens=1,* delims==" %%a in ('findstr /b "log.dirs=" config\server.properties') do set LOG_DIRS=%%b
if exist "%LOG_DIRS%\meta.properties" goto arrancar
echo Formateando %LOG_DIRS% por primera vez
for /f %%i in ('bin\windows\kafka-storage.bat random-uuid') do set CLUSTER_ID=%%i
call bin\windows\kafka-storage.bat format --standalone -t %CLUSTER_ID% -c config\server.properties

:arrancar
rem Kafka en Windows nativo se apaga solo cuando intenta borrar o compactar un
rem segmento viejo (Windows no deja renombrar un fichero abierto: "El proceso no
rem tiene acceso al archivo..."). Para la practica no hace falta borrar nada:
rem se desactivan la retencion por tiempo y el compactador.
echo Broker anunciado como %~1:9092
call bin\windows\kafka-server-start.bat config\server.properties ^
    --override "advertised.listeners=PLAINTEXT://%~1:9092,CONTROLLER://localhost:9093" ^
    --override log.retention.hours=-1 ^
    --override log.cleaner.enable=false
