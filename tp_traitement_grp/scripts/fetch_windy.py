import requests
import json
import time
from datetime import datetime

API_KEY = "TON_API_KEY_WINDY"
URL = "https://api.windy.com/api/point-forecast/v2"

payload = {
    "lat": 48.8566,
    "lon": 2.3522,
    "model": "gfs",
    "parameters": ["temp", "wind"],
    "key": API_KEY
}

OUTPUT_DIR = "data/stream/windy/"

while True:
    response = requests.post(URL, json=payload)
    data = response.json()

    data["timestamp"] = datetime.utcnow().isoformat()

    filename = f"{OUTPUT_DIR}/windy_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(data, f)

    print("Nouvelle donnée météo écrite")
    time.sleep(30)  # toutes les 30 secondes
