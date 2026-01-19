# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Customers Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, current_timestamp

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.customers"
target_table = f"{silver_schema}.customers"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("customer_unique_id", trim(col("customer_unique_id"))) \
    .withColumn("customer_city", trim(upper(col("customer_city")))) \
    .withColumn("customer_state", trim(upper(col("customer_state")))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("customer_unique_id").isNotNull())

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

