# Brazilian E-Commerce Databricks Project

Dự án thực hành Databricks với dataset Olist E-Commerce Brazil.

## Kiến trúc Data Lakehouse

```
Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Business (Reporting)
```

### Cấu trúc thư mục

```
notebooks/
├── 00_setup/
│   ├── 01_unity_catalog_setup.py
│   └── 02_create_databases.py
├── 01_bronze/
│   ├── 01_ingest_orders.py
│   ├── 02_ingest_customers.py
│   ├── 03_ingest_products.py
│   ├── 04_ingest_sellers.py
│   ├── 05_ingest_order_items.py
│   ├── 06_ingest_order_payments.py
│   ├── 07_ingest_order_reviews.py
│   └── 08_ingest_geolocation.py
├── 02_silver/
│   ├── 01_clean_orders.py
│   ├── 02_clean_customers.py
│   ├── 03_clean_products.py
│   ├── 04_clean_sellers.py
│   ├── 05_clean_order_items.py
│   ├── 06_clean_order_payments.py
│   ├── 07_clean_order_reviews.py
│   └── 08_clean_geolocation.py
├── 03_gold/
│   ├── 01_dim_customers_scd2.py
│   ├── 02_dim_products_scd2.py
│   ├── 03_dim_sellers_scd2.py
│   ├── 04_dim_geolocation_scd2.py
│   ├── 05_fact_orders_streaming.py
│   ├── 06_fact_order_items_streaming.py
│   ├── 07_fact_order_payments_streaming.py
│   └── 08_fact_order_reviews_streaming.py
├── 04_business/
│   ├── 01_create_business_views.py
│   ├── 02_daily_revenue_metrics.py
│   ├── 03_product_performance.py
│   └── 04_customer_analytics.py
├── 05_dashboards/
│   ├── dashboard_config.json
│   └── queries/
│       ├── revenue_trends.sql
│       ├── product_performance.sql
│       └── customer_segmentation.sql
├── 06_alerts/
│   ├── revenue_anomaly_detection.py
│   └── alert_config.json
└── 07_governance/
    ├── data_quality_checks.py
    └── lineage_documentation.md
```

## Yêu cầu

- Databricks Runtime 13.3 LTS hoặc cao hơn
- Unity Catalog enabled
- Delta Lake format
- Python 3.9+

## Cài đặt

1. Upload dataset vào DBFS hoặc S3/ADLS
2. Chạy notebooks theo thứ tự:
   - 00_setup/
   - 01_bronze/
   - 02_silver/
   - 03_gold/
   - 04_business/

## Streaming & SCD Type 2

- **Streaming**: Áp dụng cho các bảng fact (orders, order_items, payments, reviews)
- **SCD Type 2**: Áp dụng cho các bảng dimension (customers, products, sellers, geolocation)

## Dashboards & Alerts

- Dashboards: Xem trong `05_dashboards/`
- Alerts: Cấu hình trong `06_alerts/`

