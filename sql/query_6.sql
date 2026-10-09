WITH customer_spending AS (
    SELECT
        c.customer_id,
        c.customer_name,
        SUM(oi.quantity * oi.unit_price) AS total_spending
    FROM source.customers c
    JOIN source.orders o
        ON c.customer_id = o.customer_id
    JOIN source.order_items oi
        ON o.order_id = oi.order_id
    WHERE o.status = 'Completed'
    GROUP BY
        c.customer_id,
        c.customer_name
),
ranked_customers AS (
    SELECT
        customer_id,
        customer_name,
        total_spending,
        DENSE_RANK() OVER (
            ORDER BY total_spending DESC
        ) AS spending_rank
    FROM customer_spending
)
SELECT
    customer_id,
    customer_name,
    total_spending
FROM ranked_customers
WHERE spending_rank = 2;