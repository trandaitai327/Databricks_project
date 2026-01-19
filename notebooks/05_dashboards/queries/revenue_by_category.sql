-- Revenue by Product Category
SELECT 
    product_category_name,
    SUM(total_revenue) as category_revenue,
    COUNT(DISTINCT product_business_key) as product_count,
    AVG(avg_review_score) as avg_review_score
FROM olist_ecommerce.business.product_performance
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY category_revenue DESC
LIMIT 20

