# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Dim Sellers với SCD Type 2

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.sellers"
target_table = f"{gold_schema}.dim_sellers"

# COMMAND ----------

df_source = spark.table(source_table)

# COMMAND ----------

df_new = df_source.select(
    col("seller_id").alias("seller_business_key"),
    col("seller_zip_code_prefix"),
    col("seller_city"),
    col("seller_state"),
    current_timestamp().alias("effective_from"),
    lit(None).cast("timestamp").alias("effective_to"),
    lit(True).alias("is_current"),
    lit(1).alias("version_number")
).distinct()

# COMMAND ----------

try:
    target_delta = DeltaTable.forName(spark, target_table)
    table_exists = True
except:
    table_exists = False

if not table_exists:
    df_new.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    print(f"✓ Created {target_table} with initial load")
else:
    target_delta = DeltaTable.forName(spark, target_table)
    
    changed_records = df_new.alias("source") \
        .join(
            target_delta.toDF().alias("target"),
            (col("source.seller_business_key") == col("target.seller_business_key")) &
            (col("target.is_current") == True),
            "inner"
        ) \
        .filter(
            (col("source.seller_city") != col("target.seller_city")) |
            (col("source.seller_state") != col("target.seller_state")) |
            (col("source.seller_zip_code_prefix") != col("target.seller_zip_code_prefix")) |
            (
                (col("source.seller_city").isNull() & col("target.seller_city").isNotNull()) |
                (col("source.seller_city").isNotNull() & col("target.seller_city").isNull())
            ) |
            (
                (col("source.seller_state").isNull() & col("target.seller_state").isNotNull()) |
                (col("source.seller_state").isNotNull() & col("target.seller_state").isNull())
            )
        ) \
        .select(
            col("source.*"),
            col("target.version_number").alias("target_version")
        )
    
    update_condition = "source.seller_business_key = target.seller_business_key AND target.is_current = true"
    
    target_delta.alias("target") \
        .merge(changed_records.alias("source"), update_condition) \
        .whenMatchedUpdate(set={
            "is_current": lit(False),
            "effective_to": current_timestamp()
        }) \
        .execute()
    
    new_records = changed_records.withColumn(
        "version_number", col("target_version") + 1
    ).select(
        col("seller_business_key"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state"),
        col("effective_from"),
        col("effective_to"),
        col("is_current"),
        col("version_number")
    )
    
    target_delta.alias("target") \
        .merge(new_records.alias("source"), "source.seller_business_key = target.seller_business_key") \
        .whenNotMatchedInsertAll() \
        .execute()
    
    new_sellers = df_new.alias("source") \
        .join(
            target_delta.toDF().alias("target"),
            col("source.seller_business_key") == col("target.seller_business_key"),
            "left_anti"
        )
    
    target_delta.alias("target") \
        .merge(new_sellers.alias("source"), "source.seller_business_key = target.seller_business_key") \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print(f"✓ SCD Type 2 merge completed for {target_table}")

