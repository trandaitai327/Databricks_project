-- Top Products by Revenue
SELECT 
    product_category_name,
    SUM(total_revenue) as total_revenue,
    SUM(total_items_sold) as total_items_sold,
    AVG(avg_review_score) as avg_review_score,
    COUNT(DISTINCT product_business_key) as product_count
FROM olist_ecommerce.business.product_performance
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 15

