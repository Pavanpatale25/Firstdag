
SELECT customer_id, customer_name
FROM source.customers
WHERE customer_name IS NULL
OR TRIM(customer_name) = ''
UNION ALL
SELECT c.customer_id, c.customer_name
FROM source.customers c
JOIN source.orders o ON c.customer_id = o.customer_id
JOIN source.order_items oi ON o.order_id = oi.order_id
WHERE oi.quantity <= 0 OR oi.unit_price < 0;
