WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(o.order_date, '%Y-%m') AS sales_month,
        SUM(oi.quantity * oi.unit_price) AS monthly_revenue
    FROM source.orders o
    JOIN source.order_items oi
        ON o.order_id = oi.order_id
    WHERE o.status = 'Completed'
    GROUP BY DATE_FORMAT(o.order_date, '%Y-%m')
)
SELECT
    sales_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        ORDER BY sales_month
    ) AS cumulative_revenue
FROM monthly_revenue
ORDER BY sales_month;