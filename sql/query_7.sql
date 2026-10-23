SELECT o.order_id,
SUM(oi.quantity * oi.unit_price) AS order_value,
SUM(oi.quantity * p.price) AS catalog_value,
SUM((oi.quantity * oi.unit_price) - (oi.quantity * p.price)) AS difference
FROM source.orders o
JOIN source.order_items oi ON o.order_id = oi.order_id
JOIN source.products p ON oi.product_id = p.product_id
GROUP BY o.order_id
HAVING SUM(oi.quantity * oi.unit_price) <> SUM(oi.quantity * p.price)
ORDER BY o.order_id;