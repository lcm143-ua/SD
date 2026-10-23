# Watering Station: una imagen con WM_WS_M y WM_WS_E; cada uno se lanza en su contenedor
#   docker build -f Dockerfile.ws -t wm-ws .
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY WM_WS_M.py WM_WS_E.py ./
ENV PYTHONUNBUFFERED=1

# Sin argumentos muestra el uso del monitor
CMD ["python", "WM_WS_M.py"]
