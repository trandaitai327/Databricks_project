-- Product Review Distribution
SELECT 
    CASE 
        WHEN avg_review_score >= 4.5 THEN 'Excellent (4.5-5.0)'
        WHEN avg_review_score >= 4.0 THEN 'Very Good (4.0-4.5)'
        WHEN avg_review_score >= 3.5 THEN 'Good (3.5-4.0)'
        WHEN avg_review_score >= 3.0 THEN 'Average (3.0-3.5)'
        ELSE 'Below Average (<3.0)'
    END as review_category,
    COUNT(*) as product_count,
    AVG(total_revenue) as avg_revenue
FROM olist_ecommerce.business.product_performance
GROUP BY review_category
ORDER BY review_category

