import requests
import pandas as pd
from datetime import datetime, timezone
UTC = timezone.utc
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/g100004569/gcp-credentials.json"

BUCKET = "fire-pipeline-bucket"

def fetch_nasa_fires():
    MAP_KEY = os.environ["NASA_FIRMS_KEY"]
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/world/2"
    df = pd.read_csv(url)
    df['ingestion_time'] = datetime.now(UTC)
    return df

def fetch_weather_for_fires(fires_df, api_key, limit=200):
    weather_data = []
    for idx, row in fires_df.head(limit).iterrows():
        lat, lon = row['latitude'], row['longitude']
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        response = requests.get(url).json()
        weather_data.append({
            'latitude': lat,
            'longitude': lon,
            'temperature': response['main']['temp'],
            'humidity': response['main']['humidity'],
            'wind_speed': response['wind']['speed'],
            'pressure': response['main']['pressure'],
            'ingestion_time': datetime.now(UTC)
        })
    return pd.DataFrame(weather_data)

def upload_df_to_gcs(df, bucket_name, destination_blob):
    local_file = "temp.parquet"
    df.to_parquet(local_file, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_file, timeout=600)
    os.remove(local_file)
    print(f"Uploaded to gs://{bucket_name}/{destination_blob}")

# fetch
fires_df = fetch_nasa_fires()
print("Fetched", len(fires_df), "fires")

API_KEY = os.environ["OPENWEATHER_KEY"]
weather_df = fetch_weather_for_fires(fires_df, API_KEY, limit=200)
print("Fetched weather for", len(weather_df), "locations")

# timestamp for unique filenames
ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

# upload
upload_df_to_gcs(fires_df, BUCKET, f"raw/fires/fires_{ts}.parquet")
upload_df_to_gcs(weather_df, BUCKET, f"raw/weather/weather_{ts}.parquet")