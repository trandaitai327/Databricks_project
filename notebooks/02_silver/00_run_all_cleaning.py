# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Run All Cleaning
# MAGIC 
# MAGIC Chạy toàn bộ cleaning pipeline cho Silver layer

# COMMAND ----------

# MAGIC %run ./01_clean_orders

# COMMAND ----------

# MAGIC %run ./02_clean_customers

# COMMAND ----------

# MAGIC %run ./03_clean_products

# COMMAND ----------

# MAGIC %run ./04_clean_sellers

# COMMAND ----------

# MAGIC %run ./05_clean_order_items

# COMMAND ----------

# MAGIC %run ./06_clean_order_payments

# COMMAND ----------

# MAGIC %run ./07_clean_order_reviews

# COMMAND ----------

# MAGIC %run ./08_clean_geolocation

# COMMAND ----------

print("✓ All Silver layer cleaning completed!")

