# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType
from datetime import datetime, timedelta

# Define schema
schema = StructType([
    StructField("sk_transaction_header", IntegerType(), True),
    StructField("sk_transaction_line", IntegerType(), True),
    StructField("sk_transaction_date", DateType(), True),
    StructField("gross_amount", FloatType(), True),
    StructField("net_amount", FloatType(), True),
    StructField("payment_type", StringType(), True),
    StructField("dw_datetime_load_utc", DateType(), True)
])

# Manually write data
data = [
    # Delta Load 1 (2 days ago)
    (1, 1, datetime.strptime("2024-02-11", "%Y-%m-%d"), 120.0, 100.0, "Credit Card", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (1, 2, datetime.strptime("2024-02-11", "%Y-%m-%d"), 130.0, 110.0, "Credit Card", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (1, 3, datetime.strptime("2024-02-11", "%Y-%m-%d"), 140.0, 120.0, "Credit Card", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (2, 4, datetime.strptime("2024-02-11", "%Y-%m-%d"), 130.0, 110.0, "Cash", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (2, 5, datetime.strptime("2024-02-11", "%Y-%m-%d"), 140.0, 120.0, "Cash", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (2, 6, datetime.strptime("2024-02-11", "%Y-%m-%d"), 150.0, 130.0, "Cash", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (3, 7, datetime.strptime("2024-02-11", "%Y-%m-%d"), 140.0, 120.0, "PayPal", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (3, 8, datetime.strptime("2024-02-11", "%Y-%m-%d"), 150.0, 130.0, "PayPal", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    (3, 9, datetime.strptime("2024-02-11", "%Y-%m-%d"), 160.0, 140.0, "PayPal", datetime.strptime("2024-02-12", "%Y-%m-%d")),
    
    # Delta Load 2 (yesterday)
    (1, 10, datetime.strptime("2024-02-12", "%Y-%m-%d"), 130.0, 110.0, "Credit Card", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (1, 11, datetime.strptime("2024-02-12", "%Y-%m-%d"), 140.0, 120.0, "Credit Card", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (1, 12, datetime.strptime("2024-02-12", "%Y-%m-%d"), 150.0, 130.0, "Credit Card", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (2, 13, datetime.strptime("2024-02-12", "%Y-%m-%d"), 140.0, 120.0, "Cash", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (2, 14, datetime.strptime("2024-02-12", "%Y-%m-%d"), 150.0, 130.0, "Cash", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (2, 15, datetime.strptime("2024-02-12", "%Y-%m-%d"), 160.0, 140.0, "Cash", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (3, 16, datetime.strptime("2024-02-12", "%Y-%m-%d"), 150.0, 130.0, "PayPal", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (3, 17, datetime.strptime("2024-02-12", "%Y-%m-%d"), 160.0, 140.0, "PayPal", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    (3, 18, datetime.strptime("2024-02-12", "%Y-%m-%d"), 170.0, 150.0, "PayPal", datetime.strptime("2024-02-13", "%Y-%m-%d")),
    
    # Delta Load 3 (today)
    (1, 19, datetime.strptime("2024-02-13", "%Y-%m-%d"), 140.0, 120.0, "Credit Card", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (1, 20, datetime.strptime("2024-02-13", "%Y-%m-%d"), 150.0, 130.0, "Credit Card", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (1, 21, datetime.strptime("2024-02-13", "%Y-%m-%d"), 160.0, 140.0, "Credit Card", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (2, 22, datetime.strptime("2024-02-13", "%Y-%m-%d"), 150.0, 130.0, "Cash", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (2, 23, datetime.strptime("2024-02-13", "%Y-%m-%d"), 160.0, 140.0, "Cash", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (2, 24, datetime.strptime("2024-02-13", "%Y-%m-%d"), 170.0, 150.0, "Cash", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (3, 25, datetime.strptime("2024-02-13", "%Y-%m-%d"), 160.0, 140.0, "PayPal", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (3, 26, datetime.strptime("2024-02-13", "%Y-%m-%d"), 170.0, 150.0, "PayPal", datetime.strptime("2024-02-14", "%Y-%m-%d")),
    (3, 27, datetime.strptime("2024-02-13", "%Y-%m-%d"), 180.0, 160.0, "PayPal", datetime.strptime("2024-02-14", "%Y-%m-%d"))
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS demo""")

# Write DataFrame as an unmanaged table in Databricks
df.write.mode("overwrite").saveAsTable("hive_metastore.demo.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE hive_metastore.demo.orders
