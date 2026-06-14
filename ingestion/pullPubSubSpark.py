import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/g100004569/gcp-credentials.json"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

import json
from google.cloud import pubsub_v1, bigquery
from pyspark.sql import SparkSession

PROJECT = "fire-pipeline"
FIRE_SUB = "fireTopic-sub"
WEATHER_SUB = "weatherTopic-sub"
BQ_DATASET = "wildfire_data"
TEMP_BUCKET = "fire-pipeline-bucket"

subscriber = pubsub_v1.SubscriberClient()
bq_client = bigquery.Client(project=PROJECT)


def pull_messages(sub_name, max_msgs=100):
    sub_path = subscriber.subscription_path(PROJECT, sub_name)
    try:
        resp = subscriber.pull(
            request={"subscription": sub_path, "max_messages": max_msgs},
            timeout=10,
        )
    except Exception as e:
        print(f"pull failed for {sub_name}: {e}")
        return []

    if not resp.received_messages:
        print(f"no messages in {sub_name}")
        return []

    records, ack_ids = [], []
    for msg in resp.received_messages:
        records.append(json.loads(msg.message.data.decode("utf-8")))
        ack_ids.append(msg.ack_id)
    subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    return records


def normalize_numeric(records, fields):
    for r in records:
        for f in fields:
            if f in r and r[f] is not None:
                r[f] = float(r[f])
    return records


def load_to_bq(pdf, table_name):
    table_id = f"{PROJECT}.{BQ_DATASET}.{table_name}"
    job = bq_client.load_table_from_dataframe(
        pdf, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"loaded {len(pdf)} rows -> {table_id}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PubSubIngest").getOrCreate()

    fire_records = pull_messages(FIRE_SUB)
    weather_records = pull_messages(WEATHER_SUB)
    print(f"fire msgs: {len(fire_records)}, weather msgs: {len(weather_records)}")

    fire_records = normalize_numeric(fire_records, ["latitude", "longitude", "brightness", "frp"])
    weather_records = normalize_numeric(weather_records, ["latitude", "longitude", "temperature", "humidity", "wind_speed", "pressure"])

    if fire_records:
        fire_df = spark.createDataFrame(fire_records)
        fire_df.show(5)
        load_to_bq(fire_df.toPandas(), "fire_streaming")

    if weather_records:
        weather_df = spark.createDataFrame(weather_records)
        weather_df.show(5)
        load_to_bq(weather_df.toPandas(), "weather_streaming")

    spark.stop()