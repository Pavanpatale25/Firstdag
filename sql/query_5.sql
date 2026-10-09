WITH product_sales AS (
    SELECT
        p.category,
        p.product_id,
        p.product_name,
        SUM(oi.quantity) AS total_quantity
    FROM source.products p
    JOIN source.order_items oi
        ON p.product_id = oi.product_id
    JOIN source.orders o
        ON oi.order_id = o.order_id
    WHERE o.status = 'Completed'
    GROUP BY
        p.category,
        p.product_id,
        p.product_name
),
ranked_products AS (
    SELECT
        category,
        product_id,
        product_name,
        total_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY total_quantity DESC, product_id
        ) AS rn
    FROM product_sales
)
SELECT
    category,
    product_id,
    product_name,
    total_quantity
FROM ranked_products
WHERE rn = 1
ORDER BY category;