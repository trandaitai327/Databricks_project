# Databricks notebook source
# MAGIC %md
# MAGIC # Business Layer: Product Performance Table

# COMMAND ----------

catalog_name = "olist_ecommerce"
gold_schema = f"{catalog_name}.gold"
business_schema = f"{catalog_name}.business"

target_table = f"{business_schema}.product_performance"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE olist_ecommerce.business.product_performance AS
# MAGIC SELECT 
# MAGIC     dp.product_business_key,
# MAGIC     dp.product_category_name,
# MAGIC     COUNT(DISTINCT foi.order_id) as total_orders,
# MAGIC     COUNT(foi.order_item_id) as total_items_sold,
# MAGIC     SUM(foi.total_item_value) as total_revenue,
# MAGIC     AVG(foi.price) as avg_price,
# MAGIC     MIN(foi.price) as min_price,
# MAGIC     MAX(foi.price) as max_price,
# MAGIC     AVG(foi.freight_value) as avg_freight,
# MAGIC     AVG(COALESCE(fr.review_score, 0)) as avg_review_score,
# MAGIC     COUNT(DISTINCT foi.seller_id) as unique_sellers,
# MAGIC     CURRENT_TIMESTAMP() as updated_at
# MAGIC FROM olist_ecommerce.gold.dim_products dp
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_items foi ON dp.product_business_key = foi.product_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_orders fo ON foi.order_id = fo.order_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_reviews fr ON fo.order_id = fr.order_id
# MAGIC WHERE dp.is_current = true
# MAGIC   AND fo.order_status = 'DELIVERED'
# MAGIC GROUP BY 
# MAGIC     dp.product_business_key,
# MAGIC     dp.product_category_name
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     product_category_name,
# MAGIC     COUNT(*) as product_count,
# MAGIC     SUM(total_revenue) as category_revenue,
# MAGIC     AVG(avg_review_score) as category_avg_review
# MAGIC FROM olist_ecommerce.business.product_performance
# MAGIC GROUP BY product_category_name
# MAGIC ORDER BY category_revenue DESC
# MAGIC LIMIT 20

