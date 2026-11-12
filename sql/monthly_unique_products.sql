INSERT INTO analytics.monthly_unique_products (
    sales_year,
    sales_month,
    total_unique_products
)
SELECT
    YEAR(o.order_date),
    MONTH(o.order_date),
    COUNT(DISTINCT oi.product_id)
FROM source.orders o
JOIN source.order_items oi
    ON o.order_id = oi.order_id
WHERE o.status = 'Completed'
GROUP BY
    YEAR(o.order_date),
    MONTH(o.order_date)

ON DUPLICATE KEY UPDATE
    total_unique_products = VALUES(total_unique_products),
    load_timestamp = CURRENT_TIMESTAMP;