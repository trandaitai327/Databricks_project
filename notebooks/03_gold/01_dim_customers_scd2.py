# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Dim Customers với SCD Type 2
# MAGIC 
# MAGIC Implement Slowly Changing Dimension Type 2 để track lịch sử thay đổi của customers

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, when, lit, coalesce
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.customers"
target_table = f"{gold_schema}.dim_customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Đọc data từ Silver layer

# COMMAND ----------

df_source = spark.table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare source data với business key

# COMMAND ----------

df_new = df_source.select(
    col("customer_unique_id").alias("customer_business_key"),
    col("customer_id"),
    col("customer_zip_code_prefix"),
    col("customer_city"),
    col("customer_state"),
    current_timestamp().alias("effective_from"),
    lit(None).cast("timestamp").alias("effective_to"),
    lit(True).alias("is_current"),
    lit(1).alias("version_number")
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Type 2 Merge Logic

# COMMAND ----------

# Check if target table exists
try:
    target_delta = DeltaTable.forName(spark, target_table)
    table_exists = True
except:
    table_exists = False

if not table_exists:
    # First load: create table
    df_new.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    print(f"✓ Created {target_table} with initial load")
else:
    # SCD Type 2 Merge
    target_delta = DeltaTable.forName(spark, target_table)
    
    # Identify changed records
    changed_records = df_new.alias("source") \
        .join(
            target_delta.toDF().alias("target"),
            (col("source.customer_business_key") == col("target.customer_business_key")) &
            (col("target.is_current") == True),
            "inner"
        ) \
        .filter(
            (col("source.customer_city") != col("target.customer_city")) |
            (col("source.customer_state") != col("target.customer_state")) |
            (col("source.customer_zip_code_prefix") != col("target.customer_zip_code_prefix")) |
            (col("source.customer_id") != col("target.customer_id")) |
            (
                (col("source.customer_city").isNull() & col("target.customer_city").isNotNull()) |
                (col("source.customer_city").isNotNull() & col("target.customer_city").isNull())
            ) |
            (
                (col("source.customer_state").isNull() & col("target.customer_state").isNotNull()) |
                (col("source.customer_state").isNotNull() & col("target.customer_state").isNull())
            )
        ) \
        .select(
            col("source.*"),
            col("target.version_number").alias("target_version")
        )
    
    # Update old records: set is_current = False and effective_to
    update_condition = "source.customer_business_key = target.customer_business_key AND target.is_current = true"
    
    target_delta.alias("target") \
        .merge(
            changed_records.alias("source"),
            update_condition
        ) \
        .whenMatchedUpdate(set={
            "is_current": lit(False),
            "effective_to": current_timestamp()
        }) \
        .execute()
    
    # Insert new records with incremented version
    new_records = changed_records.withColumn(
        "version_number",
        col("target_version") + 1
    ).select(
        col("customer_business_key"),
        col("customer_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state"),
        col("effective_from"),
        col("effective_to"),
        col("is_current"),
        col("version_number")
    )
    
    target_delta.alias("target") \
        .merge(
            new_records.alias("source"),
            "source.customer_business_key = target.customer_business_key"
        ) \
        .whenNotMatchedInsert(values={
            "customer_business_key": "source.customer_business_key",
            "customer_id": "source.customer_id",
            "customer_zip_code_prefix": "source.customer_zip_code_prefix",
            "customer_city": "source.customer_city",
            "customer_state": "source.customer_state",
            "effective_from": "source.effective_from",
            "effective_to": "source.effective_to",
            "is_current": "source.is_current",
            "version_number": "source.version_number"
        }) \
        .execute()
    
    # Insert completely new customers
    new_customers = df_new.alias("source") \
        .join(
            target_delta.toDF().alias("target"),
            col("source.customer_business_key") == col("target.customer_business_key"),
            "left_anti"
        )
    
    target_delta.alias("target") \
        .merge(
            new_customers.alias("source"),
            "source.customer_business_key = target.customer_business_key"
        ) \
        .whenNotMatchedInsert(values={
            "customer_business_key": "source.customer_business_key",
            "customer_id": "source.customer_id",
            "customer_zip_code_prefix": "source.customer_zip_code_prefix",
            "customer_city": "source.customer_city",
            "customer_state": "source.customer_state",
            "effective_from": "source.effective_from",
            "effective_to": "source.effective_to",
            "is_current": "source.is_current",
            "version_number": "source.version_number"
        }) \
        .execute()
    
    print(f"✓ SCD Type 2 merge completed for {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_business_key,
# MAGIC   customer_city,
# MAGIC   customer_state,
# MAGIC   is_current,
# MAGIC   version_number,
# MAGIC   effective_from,
# MAGIC   effective_to
# MAGIC FROM olist_ecommerce.gold.dim_customers
# MAGIC WHERE customer_business_key IN (
# MAGIC   SELECT customer_business_key 
# MAGIC   FROM olist_ecommerce.gold.dim_customers 
# MAGIC   GROUP BY customer_business_key 
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY customer_business_key, version_number

