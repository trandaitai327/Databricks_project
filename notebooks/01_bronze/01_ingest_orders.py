# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest Orders Data
# MAGIC 
# MAGIC Đọc raw data từ dataset và lưu vào Bronze layer

# COMMAND ----------

# MAGIC %run ../00_setup/02_create_databases

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_timestamp, lit, monotonically_increasing_id
from datetime import datetime

# COMMAND ----------

# Configuration
catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
source_path = "/FileStore/datasets/olist_orders_dataset.csv"  # Điều chỉnh path theo môi trường
target_table = f"{bronze_schema}.orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Đọc raw CSV data

# COMMAND ----------

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(source_path)

print(f"✓ Read {df_raw.count()} records from source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Thêm metadata columns

# COMMAND ----------

df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("olist_orders_dataset.csv")) \
    .withColumn("record_id", monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Bronze layer (Delta format)

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records,
# MAGIC        MIN(order_purchase_timestamp) as earliest_order,
# MAGIC        MAX(order_purchase_timestamp) as latest_order
# MAGIC FROM olist_ecommerce.bronze.orders

