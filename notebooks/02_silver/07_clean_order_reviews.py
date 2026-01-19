# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Order Reviews Data

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp, coalesce, lit

# COMMAND ----------

catalog_name = "olist_ecommerce"
bronze_schema = f"{catalog_name}.bronze"
silver_schema = f"{catalog_name}.silver"

source_table = f"{bronze_schema}.order_reviews"
target_table = f"{silver_schema}.order_reviews"

# COMMAND ----------

df_bronze = spark.table(source_table)

# COMMAND ----------

df_silver = df_bronze \
    .withColumn("review_id", trim(col("review_id"))) \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("review_score", coalesce(col("review_score"), lit(0))) \
    .withColumn("review_creation_date", to_timestamp(col("review_creation_date"))) \
    .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp"))) \
    .withColumn("cleaned_timestamp", current_timestamp()) \
    .filter(col("review_id").isNotNull()) \
    .filter(col("order_id").isNotNull()) \
    .filter((col("review_score") >= 1) & (col("review_score") <= 5))

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"✓ Written {df_silver.count()} cleaned records to {target_table}")

