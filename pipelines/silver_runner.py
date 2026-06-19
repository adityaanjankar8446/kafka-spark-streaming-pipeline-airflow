import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp
from pyspark.sql.types import *
import time

spark = (
    SparkSession.builder
        .appName("SDP-Silver")
        .master("spark://sdp-spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
        .getOrCreate())
spark.sparkContext.setLogLevel("WARN")



order_schema = StructType([
    StructField("id",       IntegerType()),
    StructField("userId",   IntegerType()),
    StructField("date",     StringType()),
    StructField("products", ArrayType(StructType([
        StructField("productId", IntegerType()),
        StructField("quantity",  IntegerType())])))])

customer_schema = StructType([
    StructField("id",       IntegerType()),
    StructField("email",    StringType()),
    StructField("username", StringType()),
    StructField("phone",    StringType()),
    StructField("name",     StructType([
        StructField("firstname", StringType()),
        StructField("lastname",  StringType())])),
    StructField("address",  StructType([
        StructField("city",    StringType()),
        StructField("street",  StringType()),
        StructField("number",  IntegerType()),
        StructField("zipcode", StringType()),
        StructField("geolocation", StructType([
            StructField("lat",  StringType()),
            StructField("long", StringType())]))]))])

product_schema = StructType([
    StructField("id",          IntegerType()),
    StructField("title",       StringType()),
    StructField("price",       DoubleType()),
    StructField("description", StringType()),
    StructField("category",    StringType()),
    StructField("image",       StringType()),
    StructField("rating", StructType([
        StructField("rate",  DoubleType()),
        StructField("count", IntegerType())]))])



def silver_orders(df):
    parsed = (df.select(from_json(col("raw_json"), order_schema).alias("data")).select("data.*"))
    
    flattened = (parsed.select(
                        col("id").alias("order_id"),
                        col("userId").alias("customer_id"),
                        col("date").alias("order_date"),
                        explode(col("products")).alias("product"))

                        .select("order_id", "customer_id", "order_date",
                        col("product.productId").alias("product_id"),
                        col("product.quantity").alias("quantity")))
    return  flattened




def silver_customers(df):
    parsed = (df.select(from_json(col("raw_json"), customer_schema).alias("data")).select("data.*"))
          
    flattened =  (parsed.select(
                        col("id").alias("customer_id"),
                        col("email"),
                        col("username"),
                        col("phone"),

                        col("name.firstname").alias("first_name"),
                        col("name.lastname").alias("last_name"),

                        col("address.city").alias("city"),
                        col("address.street").alias("street"),
                        col("address.number").alias("house_number"),
                        col("address.zipcode").alias("zipcode"),

                        col("address.geolocation.lat").alias("latitude"),
                        col("address.geolocation.long").alias("longitude")))
    
    return flattened


def silver_products(df):
    parsed = (df.select(from_json(col("raw_json"), product_schema).alias("data")).select("data.*"))
    
    flattened =  parsed.select(
                        col("id").alias("product_id"),
                        col("title"), 
                        col("price"),
                        col("description"), 
                        col("category"), 
                        col("image"),
                        col("rating.rate").alias("rating_rate"),
                        col("rating.count").alias("rating_count"),
                        current_timestamp().alias("ingested_at"))
    return flattened



 
pipelines = [{"topic":           "orders",
            "bronze_path":     "/opt/data/bronze/orders",
            "target_path":     "/opt/data/silver/orders",
            "checkpoint_path": "/opt/checkpoints/silver/orders",
            "transform":       silver_orders,
            "dedup_key":       "order_id"},

            {"topic":           "customers",
            "bronze_path":     "/opt/data/bronze/customers",
            "target_path":     "/opt/data/silver/customers",
            "checkpoint_path": "/opt/checkpoints/silver/customers",
            "transform":       silver_customers,
            "dedup_key":       "customer_id"},

            {"topic":           "products",
            "bronze_path":     "/opt/data/bronze/products",
            "target_path":     "/opt/data/silver/products",
            "checkpoint_path": "/opt/checkpoints/silver/products",
            "transform":       silver_products,
            "dedup_key":       "product_id"}]

queries = []


for pipe in pipelines:
 
    os.makedirs(pipe["target_path"],     exist_ok=True)
    os.makedirs(pipe["checkpoint_path"], exist_ok=True)
  
    print(f"Starting silver stream: {pipe['topic']} --->>  {pipe['target_path']}")
    
    bronze_df = spark.readStream.format("delta").load(pipe["bronze_path"])


    silver_df = pipe["transform"](bronze_df)

    while True:
        try:
            df = spark.readStream.format("delta").load(pipe["bronze_path"])
            query = (silver_df.writeStream
                                .format("delta")
                                .outputMode("append")
                                .option("checkpointLocation", pipe["checkpoint_path"])
                                .option("path", pipe["target_path"])
                                .option("mergeSchema", "true")
                                .start())
        
            queries.append(query)
            print(f"Silver stream started: {pipe['bronze_path']} --->> {pipe['target_path']}")
            break
        except Exception as e:
            print(f"Bronze table not ready yet for {pipe['bronze_path']}, retrying in 15s... ({e})")
            time.sleep(15)
    

for q in queries:
    q.awaitTermination()
