import requests
import pandas as pd
from datetime import UTC, datetime

def fetch_nasa_fires():
    MAP_KEY = "6c4dff0540d9ea0f5f782ac62f4e6228"
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_SNPP_NRT/world/1"
    
    df = pd.read_csv(url)
    df['ingestion_time'] = datetime.now(UTC)
    
    print("Fetched", len(df), "fire records")
    print(df.head())
    
    return df

df = fetch_nasa_fires()