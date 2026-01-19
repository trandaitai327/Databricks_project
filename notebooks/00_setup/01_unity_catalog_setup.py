# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup và Governance Configuration
# MAGIC 
# MAGIC Notebook này thiết lập Unity Catalog và các policies cho project

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tạo Catalog và Schemas

# COMMAND ----------

# Tạo catalog chính cho project
catalog_name = "olist_ecommerce"

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
COMMENT 'Olist E-Commerce Data Lakehouse'
WITH DBPROPERTIES (
  'owner' = 'data-engineering-team',
  'environment' = 'production'
)
""")

# COMMAND ----------

# Tạo schemas cho các layer
layers = ["bronze", "silver", "gold", "business"]

for layer in layers:
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{layer}
    COMMENT 'Schema for {layer} layer data'
    """)
    print(f"✓ Created schema: {catalog_name}.{layer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Thiết lập Data Quality Rules

# COMMAND ----------

# Tạo function để validate data quality
spark.sql("""
CREATE OR REPLACE FUNCTION validate_not_null(column_name STRING, table_name STRING)
RETURNS BOOLEAN
RETURN (
  SELECT COUNT(*) = 0 
  FROM (SELECT * FROM table_name) t
  WHERE column_name IS NULL
)
""")

# Set catalog properties for governance
try:
    spark.sql(f"""
    ALTER CATALOG {catalog_name} SET DBPROPERTIES (
      'data_quality_enabled' = 'true',
      'pii_data' = 'false',
      'retention_days' = '365',
      'governance_level' = 'high'
    )
    """)
    print("✓ Catalog governance properties configured")
except Exception as e:
    print(f"Note: Could not set catalog properties (may require additional permissions): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Thiết lập Access Control

# COMMAND ----------

# Grant permissions (ví dụ - điều chỉnh theo nhu cầu thực tế)
# Uncomment và điều chỉnh theo environment của bạn

# Grant catalog usage
# spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `data-engineering-team@company.com`")
# spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `data-analytics-team@company.com`")

# Grant schema privileges
# for layer in layers:
#     spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{layer} TO `data-engineering-team@company.com`")
#     if layer in ["gold", "business"]:
#         spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{layer} TO `data-analytics-team@company.com`")

# Grant table privileges
# spark.sql(f"GRANT SELECT ON ALL TABLES IN SCHEMA {catalog_name}.business TO `business-users@company.com`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tạo External Locations (nếu cần)

# COMMAND ----------

# Ví dụ với AWS S3 (điều chỉnh theo môi trường)
# spark.sql("""
# CREATE EXTERNAL LOCATION IF NOT EXISTS bronze_data
# URL 's3://your-bucket/bronze/'
# WITH (STORAGE CREDENTIAL aws_credential)
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Enable Delta Sharing (Optional)

# COMMAND ----------

# spark.sql(f"""
# CREATE SHARE IF NOT EXISTS {catalog_name}_share
# COMMENT 'Share Olist E-Commerce data'
# """)

# COMMAND ----------

print("✓ Unity Catalog setup completed!")

