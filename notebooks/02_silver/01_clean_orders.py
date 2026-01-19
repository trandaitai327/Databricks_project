# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Orders Data
# MAGIC 
# MAGIC Clean và validate data từ Bronze layer

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, when, trim, upper, current_timestamp, coalesce
from pyspark.sql.types import StringType

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.orders"
target_table = f"{silver_schema}.orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Đọc data từ Bronze

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Cleaning và Transformation

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("order_status", upper(trim(col("order_status")))) \
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Checks

# COMMAND ----------

# Validate required fields
null_counts = df_silver.select([
    (col("order_id").isNull().cast("int").sum().alias("null_order_id")),
    (col("customer_id").isNull().cast("int").sum().alias("null_customer_id")),
    (col("order_status").isNull().cast("int").sum().alias("null_order_status"))
]).collect()[0]

print(f"Data Quality Checks:")
print(f"  - Null order_id: {null_counts['null_order_id']}")
print(f"  - Null customer_id: {null_counts['null_customer_id']}")
print(f"  - Null order_status: {null_counts['null_order_status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Silver layer

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Silver data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_status,
# MAGIC   COUNT(*) as count,
# MAGIC   MIN(order_purchase_timestamp) as earliest,
# MAGIC   MAX(order_purchase_timestamp) as latest
# MAGIC FROM olist_ecommerce.silver.orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY count DESC

