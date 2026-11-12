INSERT INTO analytics.monthly_top_customer (
    sales_year,
    sales_month,
    customer_id,
    customer_name,
    city,
    total_revenue
)
SELECT
    sales_year,
    sales_month,
    customer_id,
    customer_name,
    city,
    total_revenue
FROM (
    SELECT
        monthly_sales.*,
        ROW_NUMBER() OVER (
            PARTITION BY sales_year, sales_month
            ORDER BY total_revenue DESC, customer_id ASC
        ) AS rn
    FROM (
        SELECT
            YEAR(o.order_date) AS sales_year,
            MONTH(o.order_date) AS sales_month,
            c.customer_id,
            c.customer_name,
            c.city,
            SUM(oi.quantity * oi.unit_price) AS total_revenue
        FROM source.orders o
        JOIN source.customers c
            ON o.customer_id = c.customer_id
        JOIN source.order_items oi
            ON o.order_id = oi.order_id
        WHERE o.status = 'Completed'
        GROUP BY
            YEAR(o.order_date),
            MONTH(o.order_date),
            c.customer_id,
            c.customer_name,
            c.city
    ) AS monthly_sales
) AS ranked_customers
WHERE rn = 1

ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    customer_name = VALUES(customer_name),
    city = VALUES(city),
    total_revenue = VALUES(total_revenue),
    load_timestamp = CURRENT_TIMESTAMP;