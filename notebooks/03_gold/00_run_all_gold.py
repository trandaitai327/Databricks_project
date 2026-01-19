# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Run All Processing
# MAGIC 
# MAGIC Chạy toàn bộ processing cho Gold layer (SCD Type 2 và Streaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimension Tables (SCD Type 2)

# COMMAND ----------

# MAGIC %run ./01_dim_customers_scd2

# COMMAND ----------

# MAGIC %run ./02_dim_products_scd2

# COMMAND ----------

# MAGIC %run ./03_dim_sellers_scd2

# COMMAND ----------

# MAGIC %run ./04_dim_geolocation_scd2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fact Tables (Streaming)

# COMMAND ----------

# MAGIC %run ./05_fact_orders_streaming

# COMMAND ----------

# MAGIC %run ./06_fact_order_items_streaming

# COMMAND ----------

# MAGIC %run ./07_fact_order_payments_streaming

# COMMAND ----------

# MAGIC %run ./08_fact_order_reviews_streaming

# COMMAND ----------

print("✓ All Gold layer processing completed!")

