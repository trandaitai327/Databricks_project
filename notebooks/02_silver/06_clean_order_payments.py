# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Order Payments Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, current_timestamp, coalesce, lit

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.order_payments"
target_table = f"{silver_schema}.order_payments"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("payment_type", upper(trim(col("payment_type")))) \
    .withColumn("payment_value", coalesce(col("payment_value"), lit(0.0))) \
    .withColumn("payment_installments", coalesce(col("payment_installments"), lit(1))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("payment_value") >= 0)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

