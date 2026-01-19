# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Sellers Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, current_timestamp

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.sellers"
target_table = f"{silver_schema}.sellers"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("seller_id", trim(col("seller_id"))) \
    .withColumn("seller_city", trim(upper(col("seller_city")))) \
    .withColumn("seller_state", trim(upper(col("seller_state")))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("seller_id").isNotNull())

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

