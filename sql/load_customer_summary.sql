
INSERT INTO analytics.customer_summary (customer_id,customer_name,total_orders,total_revenue,last_order_date,load_timestamp
)
SELECT c.customer_id,c.customer_name,
    COUNT(DISTINCT o.order_id),
    SUM(oi.quantity * oi.unit_price),
    MAX(o.order_date),
    CURRENT_TIMESTAMP
FROM source.customers AS c
LEFT JOIN source.orders AS o ON c.customer_id = o.customer_id
LEFT JOIN source.order_items AS oi ON o.order_id = oi.order_id
GROUP BY c.customer_id,c.customer_name
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    total_orders = VALUES(total_orders),
    total_revenue = VALUES(total_revenue),
    last_order_date = VALUES(last_order_date),
    load_timestamp = CURRENT_TIMESTAMP;
