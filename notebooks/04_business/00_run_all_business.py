# Databricks notebook source
# MAGIC %md
# MAGIC # Business Layer: Run All Business Logic
# MAGIC 
# MAGIC Chạy toàn bộ business layer processing

# COMMAND ----------

# MAGIC %run ./01_create_business_views

# COMMAND ----------

# MAGIC %run ./02_daily_revenue_metrics

# COMMAND ----------

# MAGIC %run ./03_product_performance

# COMMAND ----------

# MAGIC %run ./04_customer_analytics

# COMMAND ----------

print("✓ All Business layer processing completed!")

