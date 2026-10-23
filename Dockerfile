# WM_Central (es el que despliega Railway: usa el Dockerfile de la raíz del repo)
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY Central/WM_Central.py Central/InvernaliaZonas.png ./

# Sin pantalla: solo panel web. La salida sin buffer para verla en los logs de Railway.
ENV PYTHONUNBUFFERED=1 WM_GUI=0

# 5050: sockets para los WM_WS_M (TCP Proxy en Railway)
# 8080: panel web (dominio público en Railway; Railway lo pasa en la variable PORT)
EXPOSE 5050 8080

# Forma shell para que se sustituyan las variables que se definen en Railway
CMD python WM_Central.py ${PUERTO_SOCKETS:-5050} ${KAFKA_HOST:-localhost} ${KAFKA_PORT:-9092}
