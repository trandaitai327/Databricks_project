-- Revenue by State
SELECT 
    customer_state,
    SUM(total_revenue) as state_revenue,
    COUNT(DISTINCT customer_business_key) as customer_count,
    AVG(lifetime_value) as avg_customer_value
FROM olist_ecommerce.business.customer_analytics
GROUP BY customer_state
ORDER BY state_revenue DESC

