import sqlite3
import pandas as pd
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\SRH\DM2\Project\wildfire_pipeline\credentials\fire-pipeline-7949a4caa0c6.json"

def load_kaggle_wildfires():
    conn = sqlite3.connect(r"D:\SRH\DM2\Project\FPA_FOD_20170508.sqlite") 
    
    # read fires table, drop Shape column (binary geometry, not needed)
    df = pd.read_sql("SELECT * FROM Fires", conn)
    df = df.drop(columns=['Shape'])
    
    print("Loaded", len(df), "historical fire records")
    print(df.shape)
    
    # save locally as parquet
    df.to_parquet("historical_fires.parquet", index=False)
    
    return df

df = load_kaggle_wildfires()