-- Customer Segmentation by Orders and Lifetime Value
SELECT 
    customer_business_key,
    customer_state,
    total_orders,
    lifetime_value,
    avg_order_value,
    CASE 
        WHEN lifetime_value >= 1000 THEN 'High Value'
        WHEN lifetime_value >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM olist_ecommerce.business.customer_analytics
ORDER BY lifetime_value DESC
LIMIT 1000

