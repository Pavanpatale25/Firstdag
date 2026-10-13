SELECT c.customer_id,c.customer_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    MAX(o.order_date) AS last_order_date
FROM source.customers c
LEFT JOIN source.orders o
ON c.customer_id = o.customer_id
LEFT JOIN source.order_items oi
ON o.order_id = oi.order_id
GROUP BY c.customer_id,c.customer_name
ORDER BY c.customer_id;