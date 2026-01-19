# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks và Governance
# MAGIC 
# MAGIC Implement data quality checks cho tất cả các layers

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

catalog_name = "olist_ecommerce"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Quality Rules Definition

# COMMAND ----------

data_quality_rules = {
    "bronze.orders": {
        "not_null": ["order_id", "customer_id", "order_status"],
        "unique": ["order_id"],
        "value_ranges": {
            "order_status": ["delivered", "shipped", "canceled", "invoiced", "processing"]
        }
    },
    "silver.orders": {
        "not_null": ["order_id", "customer_id", "order_status"],
        "unique": ["order_id"],
        "referential_integrity": {
            "customer_id": "silver.customers.customer_id"
        }
    },
    "gold.fact_orders": {
        "not_null": ["order_id", "customer_id"],
        "unique": ["order_id"],
        "completeness": {
            "order_purchase_timestamp": 0.95  # 95% completeness
        }
    },
    "gold.dim_customers": {
        "not_null": ["customer_business_key"],
        "unique_current": ["customer_business_key"],  # Only current records should be unique
        "scd_integrity": True
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Null Check Function

# COMMAND ----------

def check_nulls(table_name, columns):
    """Check for null values in specified columns"""
    df = spark.table(table_name)
    results = {}
    
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        total_count = df.count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0
        results[col_name] = {
            "null_count": null_count,
            "null_percentage": null_pct,
            "status": "PASS" if null_count == 0 else "FAIL"
        }
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Uniqueness Check Function

# COMMAND ----------

def check_uniqueness(table_name, columns):
    """Check uniqueness of specified columns"""
    df = spark.table(table_name)
    results = {}
    
    for col_name in columns:
        total_count = df.count()
        distinct_count = df.select(col_name).distinct().count()
        duplicate_count = total_count - distinct_count
        results[col_name] = {
            "total_count": total_count,
            "distinct_count": distinct_count,
            "duplicate_count": duplicate_count,
            "status": "PASS" if duplicate_count == 0 else "FAIL"
        }
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Referential Integrity Check

# COMMAND ----------

def check_referential_integrity(child_table, child_column, parent_table, parent_column):
    """Check referential integrity between tables"""
    child_df = spark.table(child_table)
    parent_df = spark.table(parent_table)
    
    # Find orphan records
    orphan_count = child_df.alias("child") \
        .join(
            parent_df.select(parent_column).distinct().alias("parent"),
            col(f"child.{child_column}") == col(f"parent.{parent_column}"),
            "left_anti"
        ).count()
    
    total_count = child_df.count()
    orphan_pct = (orphan_count / total_count * 100) if total_count > 0 else 0
    
    return {
        "orphan_count": orphan_count,
        "orphan_percentage": orphan_pct,
        "status": "PASS" if orphan_count == 0 else "FAIL"
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run Data Quality Checks

# COMMAND ----------

def run_data_quality_checks():
    """Run all data quality checks"""
    results = {}
    
    for table_key, rules in data_quality_rules.items():
        full_table_name = f"{catalog_name}.{table_key}"
        table_results = {}
        
        # Null checks
        if "not_null" in rules:
            table_results["null_checks"] = check_nulls(full_table_name, rules["not_null"])
        
        # Uniqueness checks
        if "unique" in rules:
            table_results["uniqueness_checks"] = check_uniqueness(full_table_name, rules["unique"])
        
        # Referential integrity checks
        if "referential_integrity" in rules:
            ref_checks = {}
            for child_col, parent_ref in rules["referential_integrity"].items():
                parent_table, parent_col = parent_ref.split(".")
                parent_full = f"{catalog_name}.{parent_table}"
                ref_checks[child_col] = check_referential_integrity(
                    full_table_name, child_col, parent_full, parent_col
                )
            table_results["referential_integrity"] = ref_checks
        
        results[table_key] = table_results
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execute and Display Results

# COMMAND ----------

dq_results = run_data_quality_checks()

print("=" * 80)
print("DATA QUALITY CHECK RESULTS")
print("=" * 80)

for table, checks in dq_results.items():
    print(f"\nTable: {catalog_name}.{table}")
    print("-" * 80)
    
    if "null_checks" in checks:
        print("\nNull Checks:")
        for col_name, result in checks["null_checks"].items():
            status_icon = "✓" if result["status"] == "PASS" else "✗"
            print(f"  {status_icon} {col_name}: {result['null_count']} nulls ({result['null_percentage']:.2f}%)")
    
    if "uniqueness_checks" in checks:
        print("\nUniqueness Checks:")
        for col_name, result in checks["uniqueness_checks"].items():
            status_icon = "✓" if result["status"] == "PASS" else "✗"
            print(f"  {status_icon} {col_name}: {result['duplicate_count']} duplicates")
    
    if "referential_integrity" in checks:
        print("\nReferential Integrity Checks:")
        for col_name, result in checks["referential_integrity"].items():
            status_icon = "✓" if result["status"] == "PASS" else "✗"
            print(f"  {status_icon} {col_name}: {result['orphan_count']} orphan records ({result['orphan_percentage']:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Results to Table

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Create results DataFrame
results_list = []
for table, checks in dq_results.items():
    for check_type, check_results in checks.items():
        if isinstance(check_results, dict):
            for key, value in check_results.items():
                if isinstance(value, dict) and "status" in value:
                    results_list.append({
                        "table_name": table,
                        "check_type": check_type,
                        "column_name": key,
                        "check_status": value["status"],
                        "details": str(value),
                        "check_timestamp": current_timestamp()
                    })

if results_list:
    dq_results_df = spark.createDataFrame(results_list)
    
    results_table = f"{catalog_name}.business.data_quality_results"
    dq_results_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(results_table)
    
    print(f"\n✓ Data quality results saved to {results_table}")

# COMMAND ----------

print("\n" + "=" * 80)
print("Data Quality Checks Completed!")
print("=" * 80)

