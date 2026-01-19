# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Dim Geolocation với SCD Type 2

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.geolocation"
target_table = f"{gold_schema}.dim_geolocation"

# COMMAND ----------

df_source = spark.table(source_table)

# COMMAND ----------

df_new = df_source.select(
    col("geolocation_zip_code_prefix").alias("zip_code_business_key"),
    col("geolocation_lat").alias("latitude"),
    col("geolocation_lng").alias("longitude"),
    col("geolocation_city"),
    col("geolocation_state"),
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
            (col("source.zip_code_business_key") == col("target.zip_code_business_key")) &
            (col("target.is_current") == True),
            "inner"
        ) \
        .filter(
            (col("source.geolocation_city") != col("target.geolocation_city")) |
            (col("source.geolocation_state") != col("target.geolocation_state")) |
            (abs(col("source.latitude") - col("target.latitude")) > 0.001) |
            (abs(col("source.longitude") - col("target.longitude")) > 0.001) |
            (
                (col("source.geolocation_city").isNull() & col("target.geolocation_city").isNotNull()) |
                (col("source.geolocation_city").isNotNull() & col("target.geolocation_city").isNull())
            ) |
            (
                (col("source.geolocation_state").isNull() & col("target.geolocation_state").isNotNull()) |
                (col("source.geolocation_state").isNotNull() & col("target.geolocation_state").isNull())
            )
        ) \
        .select(
            col("source.*"),
            col("target.version_number").alias("target_version")
        )
    
    update_condition = "source.zip_code_business_key = target.zip_code_business_key AND target.is_current = true"
    
    target_delta.alias("target") \
        .merge(changed_records.alias("source"), update_condition) \
        .whenMatchedUpdate(set={
            "is_current": lit(False),
            "effective_to": current_timestamp()
        }) \
        .execute()
    
    new_records = changed_records.withColumn(
        "version_number", col("target_version") + 1
    )
    
    target_delta.alias("target") \
        .merge(new_records.alias("source"), "source.zip_code_business_key = target.zip_code_business_key") \
        .whenNotMatchedInsertAll() \
        .execute()
    
    new_geolocations = df_new.alias("source") \
        .join(
            target_delta.toDF().alias("target"),
            col("source.zip_code_business_key") == col("target.zip_code_business_key"),
            "left_anti"
        )
    
    target_delta.alias("target") \
        .merge(new_geolocations.alias("source"), "source.zip_code_business_key = target.zip_code_business_key") \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print(f"✓ SCD Type 2 merge completed for {target_table}")

