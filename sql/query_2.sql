SELECT
    c.customer_id,
    c.customer_name,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM source.customers c
JOIN source.orders o
    ON c.customer_id = o.customer_id
JOIN source.order_items oi
    ON o.order_id = oi.order_id
WHERE o.status = 'Completed'
GROUP BY
    c.customer_id,
    c.customer_name
ORDER BY
    total_revenue DESC
LIMIT 3;