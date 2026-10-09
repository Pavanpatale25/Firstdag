SELECT
    c.customer_id,
    c.customer_name,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total_revenue
FROM source.customers c
LEFT JOIN source.orders o
    ON c.customer_id = o.customer_id
    AND o.status = 'Completed'
LEFT JOIN source.order_items oi
    ON o.order_id = oi.order_id
GROUP BY
    c.customer_id,
    c.customer_name
ORDER BY
    total_revenue DESC;