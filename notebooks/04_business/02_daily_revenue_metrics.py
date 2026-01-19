# Databricks notebook source
# MAGIC %md
# MAGIC # Business Layer: Daily Revenue Metrics Table

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, avg, date_trunc, current_timestamp

# COMMAND ----------

catalog_name = "olist_ecommerce"
gold_schema = f"{catalog_name}.gold"
business_schema = f"{catalog_name}.business"

target_table = f"{business_schema}.daily_revenue_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Daily Revenue Metrics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_ecommerce.business.daily_revenue_metrics AS
# MAGIC SELECT 
# MAGIC     DATE(fo.order_date) as metric_date,
# MAGIC     COUNT(DISTINCT fo.order_id) as total_orders,
# MAGIC     COUNT(DISTINCT fo.customer_id) as unique_customers,
# MAGIC     SUM(COALESCE(foi.total_item_value, 0)) as total_revenue,
# MAGIC     SUM(COALESCE(foi.price, 0)) as total_price,
# MAGIC     SUM(COALESCE(foi.freight_value, 0)) as total_freight,
# MAGIC     SUM(COALESCE(fop.payment_value, 0)) as total_payment_value,
# MAGIC     AVG(COALESCE(fr.review_score, 0)) as avg_review_score,
# MAGIC     COUNT(DISTINCT foi.product_id) as unique_products_sold,
# MAGIC     COUNT(DISTINCT foi.seller_id) as unique_sellers,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC FROM olist_ecommerce.gold.fact_orders fo
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_items foi ON fo.order_id = foi.order_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_payments fop ON fo.order_id = fop.order_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_reviews fr ON fo.order_id = fr.order_id
# MAGIC WHERE fo.order_status = 'DELIVERED'
# MAGIC GROUP BY DATE(fo.order_date)
# MAGIC ORDER BY metric_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     metric_date,
# MAGIC     total_orders,
# MAGIC     total_revenue,
# MAGIC     avg_review_score
# MAGIC FROM olist_ecommerce.business.daily_revenue_metrics
# MAGIC ORDER BY metric_date DESC
# MAGIC LIMIT 30

