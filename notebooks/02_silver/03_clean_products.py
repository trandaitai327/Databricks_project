# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Products Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp, coalesce, lit

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.products"
target_table = f"{silver_schema}.products"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("product_category_name", trim(col("product_category_name"))) \
    .withColumn("product_weight_g", coalesce(col("product_weight_g"), lit(0))) \
    .withColumn("product_length_cm", coalesce(col("product_length_cm"), lit(0))) \
    .withColumn("product_height_cm", coalesce(col("product_height_cm"), lit(0))) \
    .withColumn("product_width_cm", coalesce(col("product_width_cm"), lit(0))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("product_id").isNotNull())

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

