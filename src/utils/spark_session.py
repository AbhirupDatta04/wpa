from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)

        .config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5"
        )

        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )

        .config(
            "spark.hadoop.google.cloud.auth.service.account.enable",
            "true"
        )

        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/workspaces/wpa/wealth-product-analytics-a0af979fdc01.json"
        )

        .getOrCreate()
    )

    return spark