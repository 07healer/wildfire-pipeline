import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, expr, lower, trim, initcap
from google.cloud import storage, bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/g100004569/gcp-credentials.json"

PROJECT = "fire-pipeline"
BQ_DATASET = "wildfire_data"
BUCKET = "fire-pipeline-bucket"

client = storage.Client()
bucket = client.bucket(BUCKET)
bq_client = bigquery.Client(project=PROJECT)


def latest_blob(prefix):
    blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]
    if not blobs:
        raise FileNotFoundError(f"no parquet under {prefix}")
    return sorted(blobs, key=lambda b: b.name)[-1]


def load_to_bq(pdf, table_name, mode="WRITE_TRUNCATE"):
    table_id = f"{PROJECT}.{BQ_DATASET}.{table_name}"
    job = bq_client.load_table_from_dataframe(
        pdf, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition=mode),
    )
    job.result()
    print(f"loaded {len(pdf)} rows -> {table_id} ({mode})")


fires_blob = latest_blob("raw/fires/")
weather_blob = latest_blob("raw/weather/")
print("Using fires file   :", fires_blob.name)
print("Using weather file :", weather_blob.name)

fires_blob.download_to_filename("local_fires.parquet")
weather_blob.download_to_filename("local_weather.parquet")
bucket.blob("raw/historical_fires.parquet").download_to_filename("local_historical.parquet")
print("Downloaded all 3 files locally")

spark = SparkSession.builder.appName("WildfirePipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

fires_df = spark.read.parquet("local_fires.parquet")
weather_df = spark.read.parquet("local_weather.parquet")
historical_df = spark.read.parquet("local_historical.parquet")

print("Fires count:", fires_df.count())
print("Weather count:", weather_df.count())
print("Historical count:", historical_df.count())

# guard: empty fire fetch (e.g. NASA gap) - keep last good BQ snapshot, exit clean
if fires_df.count() == 0:
    print("No fires in latest file - keeping previous BQ snapshot, exiting")
    spark.stop()
    raise SystemExit(0)

fires_clean = (
    fires_df
    .na.drop(subset=["latitude", "longitude"])
    .filter((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)))
    .select(
        spark_round(col("latitude"), 2).alias("lat_round"),
        spark_round(col("longitude"), 2).alias("lon_round"),
        "latitude", "longitude",
        "bright_ti4", "confidence", "frp", "daynight",
        "acq_date", "acq_time", "ingestion_time",
    )
    .dropDuplicates()
)

weather_clean = (
    weather_df
    .na.drop(subset=["latitude", "longitude"])
    .select(
        spark_round(col("latitude"), 2).alias("lat_round"),
        spark_round(col("longitude"), 2).alias("lon_round"),
        "temperature", "humidity", "wind_speed", "pressure",
    )
    .dropDuplicates()
)

fire_weather = fires_clean.join(weather_clean, on=["lat_round", "lon_round"], how="left")
print("Joined fire-weather count:", fire_weather.count())
fire_weather.show(5)

historical_clean = (
    historical_df
    .select(
        "FOD_ID", "FIRE_YEAR", "DISCOVERY_DATE", "STAT_CAUSE_DESCR",
        "FIRE_SIZE", "FIRE_SIZE_CLASS", "LATITUDE", "LONGITUDE", "STATE",
    )
    .na.drop(subset=["LATITUDE", "LONGITUDE", "FIRE_SIZE", "FIRE_YEAR"])
    .filter((col("LATITUDE").between(-90, 90)) & (col("LONGITUDE").between(-180, 180)))
    .filter(col("FIRE_SIZE") > 0)
    .dropDuplicates(["FOD_ID"])
    .withColumn("STAT_CAUSE_DESCR", initcap(trim(lower(col("STAT_CAUSE_DESCR")))))
    .withColumn(
        "DISCOVERY_DATE_CONVERTED",
        expr("date_add('1858-11-16', cast(DISCOVERY_DATE - 2400000.5 as int))"),
    )
)

print("Historical clean count:", historical_clean.count())
historical_clean.select(
    "FOD_ID", "DISCOVERY_DATE", "DISCOVERY_DATE_CONVERTED",
    "STAT_CAUSE_DESCR", "FIRE_SIZE", "STATE",
).show(5)

fire_weather_pd = fire_weather.toPandas()
fire_weather_pd.to_parquet("processed_fire_weather.parquet", index=False)
print("Saved fire_weather:", fire_weather_pd.shape)

load_to_bq(fire_weather_pd, "fire_weather", mode="WRITE_TRUNCATE")

historical_clean_pd = historical_clean.toPandas()
historical_clean_pd.to_parquet("processed_historical.parquet", index=False)
print("Saved historical:", historical_clean_pd.shape)

hist_table = f"{PROJECT}.{BQ_DATASET}.historical_fires"
try:
    n = bq_client.get_table(hist_table).num_rows
except Exception:
    n = 0
if n == 0:
    load_to_bq(historical_clean_pd, "historical_fires", mode="WRITE_TRUNCATE")
else:
    print(f"historical_fires already has {n} rows - skipping reload (static data)")

spark.stop()
