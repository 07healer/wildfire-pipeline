import os
import requests
import pandas as pd
from datetime import datetime, timezone
UTC = timezone.utc

def fetch_nasa_fires():
    MAP_KEY = os.environ["NASA_FIRMS_KEY"]
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/world/2"
    
    df = pd.read_csv(url)
    df['ingestion_time'] = datetime.now(UTC)
    
    print("Fetched", len(df), "fire records")
    print(df.head())
    
    return df

df = fetch_nasa_fires()