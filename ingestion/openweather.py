import os
import requests
import pandas as pd
from datetime import datetime, timezone
UTC = timezone.utc

def fetch_nasa_fires():
    MAP_KEY = os.environ["NASA_FIRMS_KEY"]
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/world/2"
    df = pd.read_csv(url)
    return df

def fetch_weather_for_fires(fires_df, api_key, limit=10):
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

# main
fires_df = fetch_nasa_fires()
print("Fetched", len(fires_df), "fires")

API_KEY = os.environ["OPENWEATHER_KEY"]
weather_df = fetch_weather_for_fires(fires_df, API_KEY, limit=10)
print(weather_df)