import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, round, col, countDistinct

spark = (
    SparkSession.builder
        .appName("SDP-Gold")
        .master("spark://sdp-spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Paths ─────────────────────────────────────────────────────────────────────
SILVER = "/opt/data/silver"
GOLD   = "/opt/data/gold"

orders    = spark.read.format("delta").load(f"{SILVER}/orders")
products  = spark.read.format("delta").load(f"{SILVER}/products")
customers = spark.read.format("delta").load(f"{SILVER}/customers")


# ── 1. Daily Sales ────────────────────────────────────────────────────────────
# Total revenue and order count per day
daily_sales = (
    orders
    .join(products, "product_id")
    .groupBy("order_date")
    .agg(
        countDistinct("order_id")            .alias("total_orders"),
        round(sum(col("price") * col("quantity")), 2).alias("total_revenue"),
        sum("quantity")                      .alias("total_items_sold"),
    )
    .orderBy("order_date")
)


# ── 2. Product Performance ────────────────────────────────────────────────────
# Best selling products by revenue and quantity
product_performance = (
    orders
    .join(products, "product_id")
    .groupBy("product_id", "title", "category")
    .agg(
        sum("quantity")                             .alias("total_quantity_sold"),
        round(sum(col("price") * col("quantity")), 2).alias("total_revenue"),
        count("order_id")                           .alias("times_ordered"),
    )
    .orderBy(col("total_revenue").desc())
)


# ── 3. Customer Orders ────────────────────────────────────────────────────────
# Spend and order count per customer
customer_orders = (
    orders
    .join(products, "product_id")
    .join(customers, orders.user_id == customers.customer_id)
    .groupBy("customer_id", "first_name", "last_name", "email")
    .agg(
        countDistinct("order_id")                   .alias("total_orders"),
        round(sum(col("price") * col("quantity")), 2).alias("total_spent"),
        sum("quantity")                             .alias("total_items"),
    )
    .orderBy(col("total_spent").desc())
)


# ── Write Gold Tables ─────────────────────────────────────────────────────────
def write_gold(df, name):
    path = f"{GOLD}/{name}"
    os.makedirs(path, exist_ok=True)
    df.write.format("delta").mode("overwrite").save(path)
    print(f"Gold table written: {path}  ({df.count()} rows)")

write_gold(daily_sales,          "daily_sales")
write_gold(product_performance,  "product_performance")
write_gold(customer_orders,      "customer_orders")

print("\nGold pipeline complete.")
spark.stop()
