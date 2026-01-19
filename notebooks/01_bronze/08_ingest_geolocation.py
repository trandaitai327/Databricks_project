# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest Geolocation Data

# COMMAND ----------

# MAGIC %run ../00_setup/02_create_databases

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
source_path = "/FileStore/datasets/olist_geolocation_dataset.csv"
target_table = f"{bronze_schema}.geolocation"

# COMMAND ----------

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(source_path)

print(f"✓ Read {df_raw.count()} records from source")

# COMMAND ----------

df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("olist_geolocation_dataset.csv")) \
    .withColumn("record_id", monotonically_increasing_id())

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written to {target_table}")

