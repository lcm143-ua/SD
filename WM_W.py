import os, requests

URL = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = os.environ["f50e2ea4b005ab7e46d86f52928d158a"]

def consultar_clima(ciudad):
    r = requests.get(URL, params={"q": ciudad, "units": "metric", "appid": API_KEY}, timeout=3)
    r.raise_for_status()        
    datos = r.json()
    temp = datos["main"]["temp"]
    precipita = any(200 <= w["id"] < 700 for w in datos["weather"])
    return {"ciudad": ciudad, "temp": temp,"descripcion": datos["weather"][0]["description"],"alerta": temp < 0 or precipita}

print(consultar_clima("Alicante,ES"))