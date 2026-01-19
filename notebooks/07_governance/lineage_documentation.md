# Data Lineage Documentation

## Overview
Tài liệu này mô tả data lineage cho Olist E-Commerce Data Lakehouse project.

## Data Flow Architecture

```
Dataset (CSV files)
    ↓
Bronze Layer (Raw Data)
    ↓
Silver Layer (Cleaned Data)
    ↓
Gold Layer (Business Logic)
    ├── Dimension Tables (SCD Type 2)
    └── Fact Tables (Streaming)
    ↓
Business Layer (Reporting & Analytics)
```

## Layer Details

### Bronze Layer
**Purpose**: Store raw, unprocessed data from source systems

**Tables**:
- `bronze.orders` ← `olist_orders_dataset.csv`
- `bronze.customers` ← `olist_customers_dataset.csv`
- `bronze.products` ← `olist_products_dataset.csv`
- `bronze.sellers` ← `olist_sellers_dataset.csv`
- `bronze.order_items` ← `olist_order_items_dataset.csv`
- `bronze.order_payments` ← `olist_order_payments_dataset.csv`
- `bronze.order_reviews` ← `olist_order_reviews_dataset.csv`
- `bronze.geolocation` ← `olist_geolocation_dataset.csv`

### Silver Layer
**Purpose**: Cleaned and validated data ready for business logic

**Tables**:
- `silver.orders` ← `bronze.orders`
- `silver.customers` ← `bronze.customers`
- `silver.products` ← `bronze.products`
- `silver.sellers` ← `bronze.sellers`
- `silver.order_items` ← `bronze.order_items`
- `silver.order_payments` ← `bronze.order_payments`
- `silver.order_reviews` ← `bronze.order_reviews`
- `silver.geolocation` ← `bronze.geolocation`

### Gold Layer

#### Dimension Tables (SCD Type 2)
**Purpose**: Historical tracking of dimension changes

**Tables**:
- `gold.dim_customers` ← `silver.customers` (SCD Type 2)
- `gold.dim_products` ← `silver.products` (SCD Type 2)
- `gold.dim_sellers` ← `silver.sellers` (SCD Type 2)
- `gold.dim_geolocation` ← `silver.geolocation` (SCD Type 2)

**SCD Type 2 Columns**:
- `effective_from`: Timestamp when record becomes effective
- `effective_to`: Timestamp when record is superseded (NULL for current)
- `is_current`: Boolean flag for current record
- `version_number`: Version number of the record

#### Fact Tables (Streaming)
**Purpose**: Transactional data with streaming capabilities

**Tables**:
- `gold.fact_orders` ← `silver.orders` (Streaming)
- `gold.fact_order_items` ← `silver.order_items` (Streaming)
- `gold.fact_order_payments` ← `silver.order_payments` (Streaming)
- `gold.fact_order_reviews` ← `silver.order_reviews` (Streaming)

### Business Layer
**Purpose**: Final reporting tables and views for business users

#### Views:
- `business.v_fact_sales`: Star schema view joining all fact and dimension tables
- `business.v_revenue_summary`: Aggregated revenue metrics
- `business.v_customer_performance`: Customer analytics view
- `business.v_product_performance`: Product performance view

#### Tables:
- `business.daily_revenue_metrics` ← `gold.fact_orders` + `gold.fact_order_items` + `gold.fact_order_payments`
- `business.product_performance` ← `gold.fact_order_items` + `gold.dim_products`
- `business.customer_analytics` ← `gold.fact_orders` + `gold.dim_customers`
- `business.revenue_anomaly_alerts` ← `business.daily_revenue_metrics` (Alert system)

## Data Relationships

### Star Schema
```
                    fact_orders (center)
                        /    |    \
        dim_customers    dim_products    dim_sellers
              |              |               |
        dim_geolocation (optional)
```

### Key Relationships
1. `fact_orders.customer_id` → `dim_customers.customer_business_key`
2. `fact_order_items.product_id` → `dim_products.product_business_key`
3. `fact_order_items.seller_id` → `dim_sellers.seller_business_key`
4. `fact_order_items.order_id` → `fact_orders.order_id`
5. `fact_order_payments.order_id` → `fact_orders.order_id`
6. `fact_order_reviews.order_id` → `fact_orders.order_id`

## Processing Patterns

### Batch Processing
- **Bronze → Silver**: Daily batch processing
- **Silver → Gold Dimensions**: SCD Type 2 merge on change detection
- **Gold → Business**: Daily aggregation

### Streaming Processing
- **Silver → Gold Facts**: Continuous streaming with micro-batch processing
- Checkpoint locations: `/tmp/checkpoints/{table_name}`

## Data Quality Checks

### Bronze Layer
- Schema validation
- Basic null checks
- Record count validation

### Silver Layer
- Data cleaning and standardization
- Null value handling
- Data type validation
- Duplicate detection

### Gold Layer
- Referential integrity
- SCD Type 2 integrity (only one current record per business key)
- Data completeness checks
- Business rule validation

## Governance

### Access Control
- Catalog: `olist_ecommerce`
- Schema-level permissions managed via Unity Catalog
- Data quality results stored in `business.data_quality_results`

### Metadata
- All tables use Delta Lake format
- Lineage tracked through table dependencies
- Data quality metrics logged with timestamps

