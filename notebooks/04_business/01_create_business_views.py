# Databricks notebook source
# MAGIC %md
# MAGIC # Business Layer: Create Business Views
# MAGIC 
# MAGIC Tạo các views và tables cuối cùng cho business reporting

# COMMAND ----------

catalog_name = "olist_ecommerce"
gold_schema = f"{catalog_name}.gold"
business_schema = f"{catalog_name}.business"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Fact Sales View (Star Schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW olist_ecommerce.business.v_fact_sales AS
# MAGIC SELECT 
# MAGIC     fo.order_id,
# MAGIC     fo.order_date,
# MAGIC     fo.order_status,
# MAGIC     COALESCE(dc.customer_business_key, sc.customer_unique_id) as customer_business_key,
# MAGIC     COALESCE(dc.customer_city, sc.customer_city) as customer_city,
# MAGIC     COALESCE(dc.customer_state, sc.customer_state) as customer_state,
# MAGIC     dp.product_business_key,
# MAGIC     dp.product_category_name,
# MAGIC     ds.seller_business_key,
# MAGIC     ds.seller_city,
# MAGIC     ds.seller_state,
# MAGIC     foi.price,
# MAGIC     foi.freight_value,
# MAGIC     foi.total_item_value,
# MAGIC     fop.payment_type,
# MAGIC     fop.payment_value,
# MAGIC     fop.payment_installments,
# MAGIC     fr.review_score
# MAGIC FROM olist_ecommerce.gold.fact_orders fo
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_items foi ON fo.order_id = foi.order_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_payments fop ON fo.order_id = fop.order_id
# MAGIC LEFT JOIN olist_ecommerce.gold.fact_order_reviews fr ON fo.order_id = fr.order_id
# MAGIC LEFT JOIN olist_ecommerce.silver.customers sc ON fo.customer_id = sc.customer_id
# MAGIC LEFT JOIN olist_ecommerce.gold.dim_customers dc ON sc.customer_unique_id = dc.customer_business_key AND dc.is_current = true
# MAGIC LEFT JOIN olist_ecommerce.gold.dim_products dp ON foi.product_id = dp.product_business_key AND dp.is_current = true
# MAGIC LEFT JOIN olist_ecommerce.gold.dim_sellers ds ON foi.seller_id = ds.seller_business_key AND ds.is_current = true

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Revenue Summary View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW olist_ecommerce.business.v_revenue_summary AS
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('month', order_date) as month,
# MAGIC     DATE_TRUNC('week', order_date) as week,
# MAGIC     order_date,
# MAGIC     customer_state,
# MAGIC     product_category_name,
# MAGIC     payment_type,
# MAGIC     COUNT(DISTINCT order_id) as total_orders,
# MAGIC     SUM(total_item_value) as total_revenue,
# MAGIC     SUM(price) as total_price,
# MAGIC     SUM(freight_value) as total_freight,
# MAGIC     AVG(review_score) as avg_review_score,
# MAGIC     COUNT(*) as total_items
# MAGIC FROM olist_ecommerce.business.v_fact_sales
# MAGIC WHERE order_status = 'DELIVERED'
# MAGIC GROUP BY 
# MAGIC     DATE_TRUNC('month', order_date),
# MAGIC     DATE_TRUNC('week', order_date),
# MAGIC     order_date,
# MAGIC     customer_state,
# MAGIC     product_category_name,
# MAGIC     payment_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Customer Performance View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW olist_ecommerce.business.v_customer_performance AS
# MAGIC SELECT 
# MAGIC     customer_business_key,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     COUNT(DISTINCT order_id) as total_orders,
# MAGIC     SUM(total_item_value) as lifetime_value,
# MAGIC     AVG(total_item_value) as avg_order_value,
# MAGIC     MIN(order_date) as first_order_date,
# MAGIC     MAX(order_date) as last_order_date,
# MAGIC     AVG(review_score) as avg_review_score
# MAGIC FROM olist_ecommerce.business.v_fact_sales
# MAGIC GROUP BY 
# MAGIC     customer_business_key,
# MAGIC     customer_city,
# MAGIC     customer_state

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Product Performance View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW olist_ecommerce.business.v_product_performance AS
# MAGIC SELECT 
# MAGIC     product_business_key,
# MAGIC     product_category_name,
# MAGIC     COUNT(DISTINCT order_id) as total_orders,
# MAGIC     COUNT(*) as total_items_sold,
# MAGIC     SUM(total_item_value) as total_revenue,
# MAGIC     AVG(price) as avg_price,
# MAGIC     AVG(review_score) as avg_review_score
# MAGIC FROM olist_ecommerce.business.v_fact_sales
# MAGIC GROUP BY 
# MAGIC     product_business_key,
# MAGIC     product_category_name

# COMMAND ----------

print("✓ Business views created successfully!")

