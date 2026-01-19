# Hướng Dẫn Setup và Chạy Pipeline

## 1. Chuẩn Bị Môi Trường

### Yêu Cầu
- Databricks Workspace với Unity Catalog enabled
- Databricks Runtime 13.3 LTS hoặc cao hơn
- Quyền tạo Catalog, Schema, và Tables
- Dataset files đã upload vào DBFS hoặc cloud storage

### Upload Dataset
1. Upload tất cả CSV files từ thư mục `dataset/` vào Databricks:
   - Cách 1: Upload qua UI vào `/FileStore/datasets/`
   - Cách 2: Upload vào S3/ADLS và cấu hình external location

## 2. Cấu Hình Path

### Cập nhật đường dẫn dataset trong các notebook Bronze layer:

Mở các file trong `notebooks/01_bronze/` và cập nhật `source_path`:

```python
source_path = "/FileStore/datasets/olist_orders_dataset.csv"  # Thay đổi nếu cần
```

Hoặc nếu dùng cloud storage:

```python
source_path = "s3://your-bucket/datasets/olist_orders_dataset.csv"
# hoặc
source_path = "abfss://container@storageaccount.dfs.core.windows.net/datasets/olist_orders_dataset.csv"
```

## 3. Chạy Pipeline

### Cách 1: Chạy Từng Bước (Khuyến Nghị cho Lần Đầu)

1. **Setup:**
   ```
   notebooks/00_setup/00_run_all_setup.py
   ```

2. **Bronze Layer:**
   ```
   notebooks/01_bronze/00_run_all_ingestion.py
   ```

3. **Silver Layer:**
   ```
   notebooks/02_silver/00_run_all_cleaning.py
   ```

4. **Gold Layer:**
   ```
   notebooks/03_gold/00_run_all_gold.py
   ```

5. **Business Layer:**
   ```
   notebooks/04_business/00_run_all_business.py
   ```

6. **Data Quality:**
   ```
   notebooks/07_governance/data_quality_checks.py
   ```

### Cách 2: Chạy Toàn Bộ Pipeline

```
notebooks/00_pipeline/00_main_pipeline.py
```

### Cách 3: Tạo Databricks Job

1. Import file `databricks_job_config.json`
2. Hoặc tạo job thủ công trong Databricks UI:
   - Workflows → Create Job
   - Add tasks theo thứ tự dependencies
   - Schedule (optional)

## 4. Kiểm Tra Kết Quả

### Verify Tables Created

```sql
SHOW TABLES IN olist_ecommerce.bronze;
SHOW TABLES IN olist_ecommerce.silver;
SHOW TABLES IN olist_ecommerce.gold;
SHOW TABLES IN olist_ecommerce.business;
```

### Sample Queries

```sql
-- Check Bronze data
SELECT COUNT(*) FROM olist_ecommerce.bronze.orders;

-- Check Silver data
SELECT COUNT(*) FROM olist_ecommerce.silver.orders;

-- Check Gold dimensions (SCD Type 2)
SELECT 
  customer_business_key,
  is_current,
  version_number,
  effective_from,
  effective_to
FROM olist_ecommerce.gold.dim_customers
LIMIT 10;

-- Check Gold facts
SELECT COUNT(*) FROM olist_ecommerce.gold.fact_orders;

-- Check Business layer
SELECT * FROM olist_ecommerce.business.daily_revenue_metrics
ORDER BY metric_date DESC
LIMIT 10;
```

## 5. Chạy Dashboards

1. Tạo Dashboard mới trong Databricks SQL
2. Import các queries từ `notebooks/05_dashboards/queries/`
3. Cấu hình charts theo `dashboard_config.json`

## 6. Setup Alerts

1. Chạy notebook: `notebooks/06_alerts/revenue_anomaly_detection.py`
2. Cấu hình notification channels trong `alert_config.json`
3. Schedule chạy định kỳ (daily/weekly)

## 7. Troubleshooting

### Lỗi: Catalog không tồn tại
- Kiểm tra quyền truy cập Unity Catalog
- Chạy lại `00_setup/01_unity_catalog_setup.py`

### Lỗi: Table không tồn tại
- Đảm bảo đã chạy các bước trước đó
- Kiểm tra dependencies

### Lỗi: Streaming timeout
- Tăng timeout trong notebook
- Kiểm tra checkpoint location permissions

### Lỗi: SCD Type 2 merge failed
- Kiểm tra business keys unique
- Verify data quality ở Silver layer

## 8. Monitoring

### Check Pipeline Status
```sql
SELECT * FROM olist_ecommerce.business.data_quality_results
ORDER BY check_timestamp DESC
LIMIT 100;
```

### Check Alerts
```sql
SELECT * FROM olist_ecommerce.business.revenue_anomaly_alerts
ORDER BY alert_timestamp DESC
LIMIT 50;
```

## 9. Next Steps

1. Tùy chỉnh business logic trong Business layer
2. Thêm metrics và KPIs
3. Setup scheduled jobs
4. Configure monitoring và alerting
5. Tối ưu performance (partitioning, Z-ordering, etc.)

