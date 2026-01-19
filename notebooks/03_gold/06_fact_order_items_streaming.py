# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Fact Order Items Streaming

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_date
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.order_items"
target_table = f"{gold_schema}.fact_order_items"

# COMMAND ----------

df_stream = spark.readStream \
    .format("delta") \
    .table(source_table)

# COMMAND ----------

df_fact = df_stream.select(
    col("order_id"),
    col("order_item_id"),
    col("product_id"),
    col("seller_id"),
    col("shipping_limit_date"),
    col("price"),
    col("freight_value"),
    col("total_item_value"),
    to_date(col("shipping_limit_date")).alias("shipping_date"),
    current_timestamp().alias("load_timestamp")
)

# COMMAND ----------

def upsert_to_fact_order_items(microBatchDF, batchId):
    if microBatchDF.isEmpty():
        return
    
    try:
        target_delta = DeltaTable.forName(spark, target_table)
        
        target_delta.alias("target") \
            .merge(
                microBatchDF.alias("source"),
                "source.order_id = target.order_id AND source.order_item_id = target.order_item_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    except Exception as e:
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

query = df_fact.writeStream \
    .foreachBatch(upsert_to_fact_order_items) \
    .outputMode("update") \
    .option("checkpointLocation", f"/tmp/checkpoints/{target_table.replace('.', '_')}") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print(f"✓ Streaming completed for {target_table}")

