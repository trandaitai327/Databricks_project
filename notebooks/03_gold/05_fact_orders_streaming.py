# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Fact Orders Streaming
# MAGIC 
# MAGIC Stream data từ Silver layer và write vào Gold layer fact table

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_date
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.orders"
target_table = f"{gold_schema}.fact_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define streaming query

# COMMAND ----------

# Read stream from Silver layer
df_stream = spark.readStream \
    .format("delta") \
    .table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform data for fact table

# COMMAND ----------

df_fact = df_stream.select(
    col("order_id"),
    col("customer_id"),
    col("order_status"),
    col("order_purchase_timestamp"),
    col("order_approved_at"),
    col("order_delivered_carrier_date"),
    col("order_delivered_customer_date"),
    col("order_estimated_delivery_date"),
    to_date(col("order_purchase_timestamp")).alias("order_date"),
    current_timestamp().alias("load_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write stream to Gold layer (Upsert mode)

# COMMAND ----------

def upsert_to_fact_orders(microBatchDF, batchId):
    """
    Upsert function for streaming write
    """
    if microBatchDF.isEmpty():
        return
    
    try:
        target_delta = DeltaTable.forName(spark, target_table)
        
        target_delta.alias("target") \
            .merge(
                microBatchDF.alias("source"),
                "source.order_id = target.order_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    except Exception as e:
        # If table doesn't exist, create it
        error_msg = str(e).lower()
        if "does not exist" in error_msg or "table or view not found" in error_msg:
            print(f"Creating table {target_table} on batch {batchId}")
            microBatchDF.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(target_table)
        else:
            print(f"Error in batch {batchId}: {error_msg}")
            raise e

# COMMAND ----------

# Start streaming query
query = df_fact.writeStream \
    .foreachBatch(upsert_to_fact_orders) \
    .outputMode("update") \
    .option("checkpointLocation", f"/tmp/checkpoints/{target_table.replace('.', '_')}") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print(f"✓ Streaming completed for {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. For continuous streaming (uncomment if needed)
# MAGIC 
# MAGIC ```python
# MAGIC query = df_fact.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", f"/tmp/checkpoints/{target_table.replace('.', '_')}") \
# MAGIC     .option("mergeSchema", "true") \
# MAGIC     .trigger(processingTime='1 minute') \
# MAGIC     .table(target_table)
# MAGIC ```

