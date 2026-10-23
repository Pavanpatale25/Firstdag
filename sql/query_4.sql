SELECT sales_month,monthly_revenue,
SUM(monthly_revenue) OVER (ORDER BY sales_month) AS cumulative_revenue
FROM (SELECT DATE_FORMAT(o.order_date, '%Y-%m') AS sales_month,
SUM(oi.quantity * oi.unit_price) AS monthly_revenue
FROM orders o
JOIN order_items oi
ON o.order_id = oi.order_id
WHERE o.status = 'Completed'
GROUP BY sales_month
) m;