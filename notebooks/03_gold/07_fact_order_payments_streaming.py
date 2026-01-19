# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Fact Order Payments Streaming

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "olist_ecommerce"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

source_table = f"{silver_schema}.order_payments"
target_table = f"{gold_schema}.fact_order_payments"

# COMMAND ----------

df_stream = spark.readStream \
    .format("delta") \
    .table(source_table)

# COMMAND ----------

df_fact = df_stream.select(
    col("order_id"),
    col("payment_sequential"),
    col("payment_type"),
    col("payment_installments"),
    col("payment_value"),
    current_timestamp().alias("load_timestamp")
)

# COMMAND ----------

def upsert_to_fact_order_payments(microBatchDF, batchId):
    if microBatchDF.isEmpty():
        return
    
    try:
        target_delta = DeltaTable.forName(spark, target_table)
        
        target_delta.alias("target") \
            .merge(
                microBatchDF.alias("source"),
                "source.order_id = target.order_id AND source.payment_sequential = target.payment_sequential"
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
    .foreachBatch(upsert_to_fact_order_payments) \
    .outputMode("update") \
    .option("checkpointLocation", f"/tmp/checkpoints/{target_table.replace('.', '_')}") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print(f"✓ Streaming completed for {target_table}")

