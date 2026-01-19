# Databricks notebook source
# MAGIC %md
# MAGIC # Main ETL Pipeline
# MAGIC 
# MAGIC End-to-end pipeline từ Bronze → Silver → Gold → Business

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

# MAGIC %run ../00_setup/00_run_all_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bronze Layer (Ingestion)

# COMMAND ----------

# MAGIC %run ../01_bronze/00_run_all_ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Layer (Cleaning)

# COMMAND ----------

# MAGIC %run ../02_silver/00_run_all_cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Layer (SCD Type 2 & Streaming)

# COMMAND ----------

# MAGIC %run ../03_gold/00_run_all_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Business Layer (Reporting)

# COMMAND ----------

# MAGIC %run ../04_business/00_run_all_business

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Data Quality Checks

# COMMAND ----------

# MAGIC %run ../07_governance/data_quality_checks

# COMMAND ----------

print("=" * 80)
print("✓ COMPLETE PIPELINE EXECUTED SUCCESSFULLY!")
print("=" * 80)

