# Databricks notebook source
# MAGIC %md
# MAGIC # Business Layer: Customer Analytics Table

# COMMAND ----------

catalog_name = "olist_ecommerce"
gold_schema = f"{catalog_name}.gold"
business_schema = f"{catalog_name}.business"

target_table = f"{business_schema}.customer_analytics"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_ecommerce.business.customer_analytics AS
# MAGIC SELECT 
# MAGIC     dc.customer_business_key,
# MAGIC     dc.customer_city,
# MAGIC     dc.customer_state,
# MAGIC     COUNT(DISTINCT fo.order_id) as total_orders,
# MAGIC     SUM(COALESCE(foi.total_item_value, 0)) as lifetime_value,
# MAGIC     AVG(COALESCE(foi.total_item_value, 0)) as avg_order_value,
# MAGIC     MIN(fo.order_date) as first_order_date,
# MAGIC     MAX(fo.order_date) as last_order_date,
# MAGIC     DATEDIFF(MAX(fo.order_date), MIN(fo.order_date)) as customer_lifetime_days,
# MAGIC     AVG(COALESCE(fr.review_score, 0)) as avg_review_score,
# MAGIC     COUNT(DISTINCT foi.product_id) as unique_products_purchased,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC FROM olist_ecommerce.gold.dim_customers dc
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_orders fo ON dc.customer_id = fo.customer_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_items foi ON fo.order_id = foi.order_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_reviews fr ON fo.order_id = fr.order_id
# MAGIC WHERE dc.is_current = true
# MAGIC GROUP BY 
# MAGIC     dc.customer_business_key,
# MAGIC     dc.customer_city,
# MAGIC     dc.customer_state
# MAGIC ORDER BY lifetime_value DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     customer_state,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     SUM(lifetime_value) as state_total_revenue,
# MAGIC     AVG(lifetime_value) as avg_customer_value,
# MAGIC     AVG(total_orders) as avg_orders_per_customer
# MAGIC FROM olist_ecommerce.business.customer_analytics
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY state_total_revenue DESC

