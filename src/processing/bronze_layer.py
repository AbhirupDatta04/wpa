import shutil
from pathlib import Path

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_timestamp

from config.settings import BUCKET_NAME,BRONZE_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)

LOCAL_RAW = Path("/tmp/wpa/raw")
LOCAL_BRONZE = Path("/tmp/wpa/bronze")
GCS_RAW_PREFIX = "raw"
GCS_BRONZE_PREFIX = "bronze"
RAW_FILES = ["users.csv", "sessions.csv", "events.csv", "trades.csv"]

def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("BronzeLayer").getOrCreate()


def get_gcs_bucket() -> storage.Bucket:
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


def download_raw_files() -> None:
    LOCAL_RAW.mkdir(parents=True, exist_ok=True)
    bucket = get_gcs_bucket()

    for file_name in RAW_FILES:
        local_path = LOCAL_RAW / file_name
        blob = bucket.blob(f"{GCS_RAW_PREFIX}/{file_name}")

        logger.info("Downloading raw file: %s", file_name)
        blob.download_to_filename(str(local_path))
        logger.info("Downloaded %s to %s", file_name, local_path)

def read_raw_data(spark: SparkSession):
    users_df = spark.read.csv(
        str(LOCAL_RAW / "users.csv"),
        header=True,
        inferSchema=True,
    )

    sessions_df = spark.read.csv(
        str(LOCAL_RAW / "sessions.csv"),
        header=True,
        inferSchema=True,
    )

    events_df = spark.read.csv(
        str(LOCAL_RAW / "events.csv"),
        header=True,
        inferSchema=True,
    )

    trades_df = spark.read.csv(
        str(LOCAL_RAW / "trades.csv"),
        header=True,
        inferSchema=True,
    )

    return users_df, sessions_df, events_df, trades_df

def clean_events(events_df):
    return (
        events_df
        .withColumn(
            "event_type",
            lower(col("event_type"))
        )
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"))
        )
    )


def clean_sessions(sessions_df):
    return (
        sessions_df
        .withColumn(
            "session_start",
            to_timestamp(col("session_start"))
        )
        .withColumn(
            "session_end",
            to_timestamp(col("session_end"))
        )
    )


def write_bronze_data(
    users_df,
    sessions_df,
    events_df,
    trades_df,
):
    LOCAL_BRONZE.mkdir(parents=True, exist_ok=True)

    datasets = {
        "users": users_df,
        "sessions": sessions_df,
        "events": events_df,
        "trades": trades_df,
    }

    bucket = get_gcs_bucket()

    for name, df in datasets.items():
        local_path = LOCAL_BRONZE / f"{name}.parquet"

        logger.info("Writing local parquet: %s", local_path)

        df.toPandas().to_parquet(local_path, index=False)

        gcs_path = f"{GCS_BRONZE_PREFIX}/{name}.parquet"

        logger.info("Uploading bronze dataset to GCS: %s", gcs_path)

        bucket.blob(gcs_path).upload_from_filename(str(local_path))

        logger.info("Uploaded %s successfully", gcs_path)

def run_bronze_layer():
    spark = build_spark_session()
    download_raw_files()

    print("Reading raw layer...")
    users_df, sessions_df, events_df, trades_df = read_raw_data(spark)

    print("Cleaning bronze datasets...")
    events_df = clean_events(events_df)
    sessions_df = clean_sessions(sessions_df)

    print("Writing bronze parquet datasets...")
    write_bronze_data(
        users_df,
        sessions_df,
        events_df,
        trades_df,
    )

    print("Bronze layer completed successfully")


if __name__ == "__main__":
    run_bronze_layer()