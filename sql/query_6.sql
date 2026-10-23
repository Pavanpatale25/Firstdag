SELECT customer_id,customer_name,total_spending
FROM(SELECT c.customer_id,c.customer_name,
SUM(oi.quantity * oi.unit_price) AS total_spending,
DENSE_RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS rnk
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
JOIN order_items oi
ON o.order_id = oi.order_id
WHERE o.status = 'Completed'
GROUP BY c.customer_id, c.customer_name
) x
WHERE rnk = 2;