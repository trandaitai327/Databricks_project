-- Customers by State
SELECT 
    customer_state,
    COUNT(*) as customer_count,
    SUM(lifetime_value) as total_revenue,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(total_orders) as avg_orders_per_customer
FROM olist_ecommerce.business.customer_analytics
GROUP BY customer_state
ORDER BY customer_count DESC

