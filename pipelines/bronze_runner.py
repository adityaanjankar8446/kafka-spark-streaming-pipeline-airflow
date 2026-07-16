import signal
import sys
from pyspark.sql import SparkSession
import time


spark = (
    SparkSession.builder
        .appName("SDP-Bronze")
        .master("spark://sdp-spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.HDFSLogStore")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


pipelines = [
    {
        "topic": "orders",
        "target_path": "/opt/data/bronze/orders",
        "checkpoint_path": "/opt/checkpoints/bronze/orders"
    },
    {
        "topic": "customers",
        "target_path": "/opt/data/bronze/customers",
        "checkpoint_path": "/opt/checkpoints/bronze/customers"
    },
    {
        "topic": "products",
        "target_path": "/opt/data/bronze/products",
        "checkpoint_path": "/opt/checkpoints/bronze/products"
    }
]

queries = []


for pipe in pipelines:

    topic       = pipe["topic"]
    target_path = pipe["target_path"]
    checkpoint  = pipe["checkpoint_path"]

    print(f"Starting stream: {topic} -> {target_path}")

    df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:9092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
    )

    bronze_df = df.selectExpr("CAST(value AS STRING) AS raw_json")

    query = (
        bronze_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("path", target_path)
            .start()
    )

    queries.append(query)


RUN_SECONDS = 90
deadline = time.time() + RUN_SECONDS

for q in queries:
    remaining = max(deadline - time.time(), 1)
    q.awaitTermination(remaining)   # seconds, shared deadline across all 3 queries

for q in queries:
    if q.isActive:
        q.stop()

spark.stop()
sys.exit(0)