INSERT INTO analytics.top_selling_month (
    sales_year,
    sales_month,
    total_revenue
)
SELECT
    sales_year,
    sales_month,
    total_revenue
FROM (
    SELECT
        monthly_sales.*,
        ROW_NUMBER() OVER (
            PARTITION BY sales_year
            ORDER BY total_revenue DESC, sales_month ASC
        ) AS rn
    FROM (
        SELECT
            YEAR(o.order_date) AS sales_year,
            MONTH(o.order_date) AS sales_month,
            SUM(oi.quantity * oi.unit_price) AS total_revenue
        FROM source.orders o
        JOIN source.order_items oi
            ON o.order_id = oi.order_id
        WHERE o.status = 'Completed'
        GROUP BY
            YEAR(o.order_date),
            MONTH(o.order_date)
    ) AS monthly_sales
) AS ranked_months
WHERE rn = 1

ON DUPLICATE KEY UPDATE
    sales_month = VALUES(sales_month),
    total_revenue = VALUES(total_revenue),
    load_timestamp = CURRENT_TIMESTAMP;