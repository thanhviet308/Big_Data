from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import pandas as pd
import os

from src.detection.fraud_detector import detect_fraud

schema = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType())
])

builder = SparkSession.builder.appName("FraudDetectionStreaming")

# On Databricks, Kafka connector is often available. For local Spark,
# set SPARK_KAFKA_PACKAGE, e.g. org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
kafka_package = os.getenv("SPARK_KAFKA_PACKAGE")
if kafka_package:
    builder = builder.config("spark.jars.packages", kafka_package)

spark = builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    .option("subscribe", os.getenv("KAFKA_TOPIC", "transactions"))
    .option("startingOffsets", "earliest")
    .load()
)

df_parsed = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    result = detect_fraud(pdf)

    print("\n🚨 NEW BATCH 🚨")
    print(result[[
        "type", "amount", "nameDest",
        "ml_prediction", "rule_flag", "final_flag"
    ]])

query = (
    df_parsed.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        os.getenv("SPARK_CHECKPOINT_PATH", "/tmp/fraud_detection/checkpoints"),
    )
    .start()
)

query.awaitTermination()