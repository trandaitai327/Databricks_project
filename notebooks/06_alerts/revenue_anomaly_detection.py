# Databricks notebook source
# MAGIC %md
# MAGIC # Revenue Anomaly Detection Alert
# MAGIC 
# MAGIC Phát hiện và cảnh báo khi doanh thu tăng/giảm đột biến

# COMMAND ----------

from pyspark.sql.functions import col, lag, when, abs, current_timestamp
from pyspark.sql.window import Window
import json

# COMMAND ----------

catalog_name = "olist_ecommerce"
business_schema = f"{catalog_name}.business"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Đọc daily revenue metrics

# COMMAND ----------

df_revenue = spark.table(f"{business_schema}.daily_revenue_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tính toán thay đổi doanh thu

# COMMAND ----------

window_spec = Window.orderBy("metric_date")

df_anomaly = df_revenue \
    .withColumn("previous_revenue", lag("total_revenue", 1).over(window_spec)) \
    .withColumn("revenue_change", col("total_revenue") - col("previous_revenue")) \
    .withColumn("revenue_change_pct", 
        when(col("previous_revenue") > 0, 
            (col("revenue_change") / col("previous_revenue")) * 100)
        .otherwise(0)
    ) \
    .withColumn("absolute_change_pct", abs(col("revenue_change_pct"))) \
    .filter(col("previous_revenue").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Xác định anomalies (threshold: 30% thay đổi)

# MAGIC 

# COMMAND ----------

threshold_pct = 30  # 30% change threshold

df_anomalies = df_anomaly \
    .filter(col("absolute_change_pct") >= threshold_pct) \
    .select(
        col("metric_date"),
        col("total_revenue"),
        col("previous_revenue"),
        col("revenue_change"),
        col("revenue_change_pct"),
        when(col("revenue_change_pct") > 0, "INCREASE")
            .otherwise("DECREASE")
            .alias("alert_type"),
        current_timestamp().alias("alert_timestamp")
    ) \
    .orderBy(col("metric_date").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Hiển thị alerts

# COMMAND ----------

anomaly_count = df_anomalies.count()

if anomaly_count > 0:
    print(f"⚠️ ALERT: {anomaly_count} revenue anomalies detected!")
    print(f"Threshold: {threshold_pct}% change")
    print("\nAnomalies:")
    df_anomalies.show(20, False)
    
    # Create alert summary
    alert_summary = df_anomalies.groupBy("alert_type").agg({
        "metric_date": "count",
        "revenue_change": "avg"
    }).collect()
    
    print("\nAlert Summary:")
    for row in alert_summary:
        print(f"  {row['alert_type']}: {row['count(metric_date)']} occurrences, "
              f"Avg change: {row['avg(revenue_change)']:.2f}")
else:
    print(f"✓ No revenue anomalies detected (threshold: {threshold_pct}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lưu alerts vào table (optional)

# COMMAND ----------

alert_table = f"{business_schema}.revenue_anomaly_alerts"

try:
    df_anomalies.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(alert_table)
    print(f"✓ Alerts saved to {alert_table}")
except Exception as e:
    # Create table if doesn't exist
    df_anomalies.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(alert_table)
    print(f"✓ Created and saved alerts to {alert_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gửi email/Slack notification (example structure)
# MAGIC 
# MAGIC ```python
# MAGIC # Example: Send email notification
# MAGIC if anomaly_count > 0:
# MAGIC     import smtplib
# MAGIC     from email.mime.text import MIMEText
# MAGIC     
# MAGIC     msg = MIMEText(f"Revenue anomaly detected: {anomaly_count} occurrences")
# MAGIC     msg['Subject'] = 'Revenue Anomaly Alert'
# MAGIC     msg['From'] = 'alerts@company.com'
# MAGIC     msg['To'] = 'data-team@company.com'
# MAGIC     
# MAGIC     # Send email (configure SMTP settings)
# MAGIC     # smtp_server.send_message(msg)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Query để xem historical alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     metric_date,
# MAGIC     alert_type,
# MAGIC     revenue_change_pct,
# MAGIC     total_revenue,
# MAGIC     alert_timestamp
# MAGIC FROM olist_ecommerce.business.revenue_anomaly_alerts
# MAGIC ORDER BY metric_date DESC
# MAGIC LIMIT 50

