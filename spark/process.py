import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]



import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, expr
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/g100004569/gcp-credentials.json"

# download files from GCS locally first
client = storage.Client()
bucket = client.bucket("fire-pipeline-bucket")

bucket.blob("raw/fires/fires_20260612_135118.parquet").download_to_filename("local_fires.parquet")
bucket.blob("raw/weather/weather_20260612_135118.parquet").download_to_filename("local_weather.parquet")
bucket.blob("raw/historical_fires.parquet").download_to_filename("local_historical.parquet")

print("Downloaded all 3 files locally")

# start spark
spark = SparkSession.builder.appName("WildfirePipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# read parquet files
fires_df = spark.read.parquet("local_fires.parquet")
weather_df = spark.read.parquet("local_weather.parquet")
historical_df = spark.read.parquet("local_historical.parquet")

print("Fires count:", fires_df.count())
print("Weather count:", weather_df.count())
print("Historical count:", historical_df.count())

# ---- CLEANING + JOINING ----

# clean fires data
fires_clean = fires_df.select(
    spark_round(col("latitude"), 2).alias("lat_round"),
    spark_round(col("longitude"), 2).alias("lon_round"),
    "latitude", "longitude",
    "bright_ti4", "confidence", "frp", "daynight",
    "acq_date", "acq_time", "ingestion_time"
).dropDuplicates()

# clean weather data
weather_clean = weather_df.select(
    spark_round(col("latitude"), 2).alias("lat_round"),
    spark_round(col("longitude"), 2).alias("lon_round"),
    "temperature", "humidity", "wind_speed", "pressure"
).dropDuplicates()

# join fires with weather
fire_weather = fires_clean.join(
    weather_clean,
    on=["lat_round", "lon_round"],
    how="left"
)

print("Joined fire-weather count:", fire_weather.count())
fire_weather.show(5)

# clean historical data + convert Julian date to normal date
historical_clean = historical_df.select(
    "FOD_ID", "FIRE_YEAR", "DISCOVERY_DATE", "STAT_CAUSE_DESCR",
    "FIRE_SIZE", "FIRE_SIZE_CLASS", "LATITUDE", "LONGITUDE", "STATE"
).na.drop(subset=["LATITUDE", "LONGITUDE", "FIRE_SIZE"])

historical_clean = historical_clean.withColumn(
    "DISCOVERY_DATE_CONVERTED",
    expr("date_add('1858-11-16', cast(DISCOVERY_DATE - 2400000.5 as int))")
)

print("Historical clean count:", historical_clean.count())
historical_clean.select("FOD_ID", "DISCOVERY_DATE", "DISCOVERY_DATE_CONVERTED", "FIRE_SIZE", "STATE").show(5)

# ---- SAVE PROCESSED DATA (via pandas, avoids Windows Hadoop write issues) ----
fire_weather_pd = fire_weather.toPandas()
fire_weather_pd.to_parquet("processed_fire_weather.parquet", index=False)
print("Saved fire_weather:", fire_weather_pd.shape)

historical_clean_pd = historical_clean.toPandas()
historical_clean_pd.to_parquet("processed_historical.parquet", index=False)
print("Saved historical:", historical_clean_pd.shape)