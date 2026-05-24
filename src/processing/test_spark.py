from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkValidation")
    .getOrCreate()
)

print("Spark session created successfully")

df = spark.createDataFrame(
    [
        (1, "NVDA"),
        (2, "AAPL"),
    ],
    ["id", "stock"]
)

df.show()