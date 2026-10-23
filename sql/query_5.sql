SELECT *
FROM ( SELECT p.category,p.product_id,p.product_name,
SUM(oi.quantity) AS total_quantity,
ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity) DESC) AS rn
FROM products p
JOIN order_items oi
ON p.product_id = oi.product_id
JOIN orders o
ON oi.order_id = o.order_id
WHERE o.status = 'Completed'
GROUP BY p.category, p.product_id, p.product_name
) x
WHERE rn = 1;