-- Daily Revenue Trends
SELECT 
    metric_date,
    total_revenue,
    total_orders,
    unique_customers,
    avg_review_score,
    LAG(total_revenue) OVER (ORDER BY metric_date) as previous_day_revenue,
    total_revenue - LAG(total_revenue) OVER (ORDER BY metric_date) as daily_revenue_change,
    (total_revenue - LAG(total_revenue) OVER (ORDER BY metric_date)) / 
        NULLIF(LAG(total_revenue) OVER (ORDER BY metric_date), 0) * 100 as revenue_change_pct
FROM olist_ecommerce.business.daily_revenue_metrics
ORDER BY metric_date DESC
LIMIT 90

