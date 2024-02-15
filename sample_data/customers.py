# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create DataFrame using schema
data = [(1, "John", 25),
        (2, "Alice", 30),
        (3, "Bob", 28),
        (4, "Sarah", 35),
        (5, "Michael", 40),
        (6, "Emma", None),
        (7, "Daniel", 32),
        (8, "Olivia", None),
        (9, "James", 37),
        (10, "Sophia", 42),
        (11, "William", 45),
        (12, "Charlotte", 27),
        (13, "Matthew", 50),
        (14, "Emily", 29),
        (15, "Alexander", 33),
        (16, "Ella", 38),
        (17, "Benjamin", 55),
        (18, "Grace", 31),
        (19, "Michaela", None),
        (20, "Jacob", 48)]

df = spark.createDataFrame(data, schema)

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS demo""")
# Write DataFrame as an unmanaged table in Databricks
df.write.mode("overwrite").saveAsTable("hive_metastore.demo.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE hive_metastore.demo.customers

# COMMAND ----------


