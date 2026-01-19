# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Dim Products với SCD Type 2

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.products"
target_table = f"{gold_schema}.dim_products"

# COMMAND ----------

df_source = spark.table(source_table)

# COMMAND ----------

df_new = df_source.select(
    col("product_id").alias("product_business_key"),
    col("product_category_name"),
    col("product_name_lenght"),
    col("product_description_lenght"),
    col("product_photos_qty"),
    col("product_weight_g"),
    col("product_length_cm"),
    col("product_height_cm"),
    col("product_width_cm"),
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
            (col("source.product_business_key") == col("target.product_business_key")) &
            (col("target.is_current") == True),
            "inner"
        ) \
        .filter(
            (col("source.product_category_name") != col("target.product_category_name")) |
            (abs(col("source.product_weight_g") - col("target.product_weight_g")) > 0.01) |
            (abs(col("source.product_length_cm") - col("target.product_length_cm")) > 0.01) |
            (abs(col("source.product_height_cm") - col("target.product_height_cm")) > 0.01) |
            (abs(col("source.product_width_cm") - col("target.product_width_cm")) > 0.01) |
            (
                (col("source.product_category_name").isNull() & col("target.product_category_name").isNotNull()) |
                (col("source.product_category_name").isNotNull() & col("target.product_category_name").isNull())
            )
        ) \
        .select(
            col("source.*"),
            col("target.version_number").alias("target_version")
        )
    
    update_condition = "source.product_business_key = target.product_business_key AND target.is_current = true"
    
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
        col("product_business_key"),
        col("product_category_name"),
        col("product_name_lenght"),
        col("product_description_lenght"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        col("effective_from"),
        col("effective_to"),
        col("is_current"),
        col("version_number")
    )
    
    target_delta.alias("target") \
        .merge(new_records.alias("source"), "source.product_business_key = target.product_business_key") \
        .whenNotMatchedInsertAll() \
        .execute()
    
    new_products = df_new.alias("source") \
        .join(
            target_delta.toDF().alias("target"),
            col("source.product_business_key") == col("target.product_business_key"),
            "left_anti"
        )
    
    target_delta.alias("target") \
        .merge(new_products.alias("source"), "source.product_business_key = target.product_business_key") \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print(f"✓ SCD Type 2 merge completed for {target_table}")

