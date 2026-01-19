# Databricks notebook source
# MAGIC %md
# MAGIC # Create Databases và Base Tables Structure
# MAGIC 
# MAGIC Tạo cấu trúc database cơ bản nếu chưa dùng Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alternative: Tạo databases nếu không dùng Unity Catalog
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_db;
# MAGIC CREATE DATABASE IF NOT EXISTS silver_db;
# MAGIC CREATE DATABASE IF NOT EXISTS gold_db;
# MAGIC CREATE DATABASE IF NOT EXISTS business_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set default database
# MAGIC 
# MAGIC Điều chỉnh theo catalog/schema đã tạo

# COMMAND ----------

catalog_name = "olist_ecommerce"
current_user = spark.sql("SELECT current_user()").collect()[0][0]

print(f"Current user: {current_user}")
print(f"Using catalog: {catalog_name}")

