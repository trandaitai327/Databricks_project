# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Run All Ingestion
# MAGIC 
# MAGIC Chạy toàn bộ ingestion pipeline cho Bronze layer

# COMMAND ----------

# MAGIC %run ./01_ingest_orders

# COMMAND ----------

# MAGIC %run ./02_ingest_customers

# COMMAND ----------

# MAGIC %run ./03_ingest_products

# COMMAND ----------

# MAGIC %run ./04_ingest_sellers

# COMMAND ----------

# MAGIC %run ./05_ingest_order_items

# COMMAND ----------

# MAGIC %run ./06_ingest_order_payments

# COMMAND ----------

# MAGIC %run ./07_ingest_order_reviews

# COMMAND ----------

# MAGIC %run ./08_ingest_geolocation

# COMMAND ----------

print("✓ All Bronze layer ingestion completed!")

