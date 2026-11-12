INSERT INTO analytics.monthly_order_status (
    sales_year,
    sales_month,
    order_status,
    total_orders
)
SELECT
    YEAR(order_date),
    MONTH(order_date),
    status,
    COUNT(DISTINCT order_id)
FROM source.orders
GROUP BY
    YEAR(order_date),
    MONTH(order_date),
    status

ON DUPLICATE KEY UPDATE
    total_orders = VALUES(total_orders),
    load_timestamp = CURRENT_TIMESTAMP;