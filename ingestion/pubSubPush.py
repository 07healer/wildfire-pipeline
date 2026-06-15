import requests
import pandas as pd
import json
from datetime import datetime, timezone
UTC = timezone.utc
from google.cloud import pubsub_v1
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/g100004569/gcp-credentials.json"

PROJECT_ID = "fire-pipeline"

publisher = pubsub_v1.PublisherClient()
fire_topic_path = publisher.topic_path(PROJECT_ID, "fireTopic")
weather_topic_path = publisher.topic_path(PROJECT_ID, "weatherTopic")

def fetch_nasa_fires():
    MAP_KEY = os.environ["NASA_FIRMS_KEY"]
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/world/2"
    df = pd.read_csv(url)
    return df

def fetch_weather(lat, lon, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    return requests.get(url).json()

fires_df = fetch_nasa_fires()
print("Fetched", len(fires_df), "fires")

API_KEY = os.environ["OPENWEATHER_KEY"]

for idx, row in fires_df.head(20).iterrows():
    fire_msg = {
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "brightness": row['bright_ti4'],
        "confidence": row['confidence'],
        "frp": row['frp'],
        "daynight": row['daynight'],
        "acq_date": row['acq_date'],
        "ingestion_time": datetime.now(UTC).isoformat()
    }
    publisher.publish(fire_topic_path, json.dumps(fire_msg).encode("utf-8"))

    weather = fetch_weather(row['latitude'], row['longitude'], API_KEY)
    weather_msg = {
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "temperature": weather['main']['temp'],
        "humidity": weather['main']['humidity'],
        "wind_speed": weather['wind']['speed'],
        "pressure": weather['main']['pressure'],
        "ingestion_time": datetime.now(UTC).isoformat()
    }
    publisher.publish(weather_topic_path, json.dumps(weather_msg).encode("utf-8"))

print("Published 20 fire + weather messages to Pub/Sub")