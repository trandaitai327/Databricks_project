# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Order Items Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp, coalesce, lit

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.order_items"
target_table = f"{silver_schema}.order_items"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("seller_id", trim(col("seller_id"))) \
    .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))) \
    .withColumn("price", coalesce(col("price"), lit(0.0))) \
    .withColumn("freight_value", coalesce(col("freight_value"), lit(0.0))) \
    .withColumn("total_item_value", col("price") + col("freight_value")) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("seller_id").isNotNull())

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

