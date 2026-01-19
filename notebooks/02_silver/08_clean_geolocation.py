# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Geolocation Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, current_timestamp

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.geolocation"
target_table = f"{silver_schema}.geolocation"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("geolocation_zip_code_prefix", trim(col("geolocation_zip_code_prefix"))) \
    .withColumn("geolocation_lat", col("geolocation_lat").cast("double")) \
    .withColumn("geolocation_lng", col("geolocation_lng").cast("double")) \
    .withColumn("geolocation_city", trim(upper(col("geolocation_city")))) \
    .withColumn("geolocation_state", trim(upper(col("geolocation_state")))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("geolocation_zip_code_prefix").isNotNull())

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

