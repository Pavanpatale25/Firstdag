SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT CASE
        WHEN o.status = 'Completed' THEN o.order_id
    END) AS total_orders,
    COALESCE(
        SUM(
            CASE
                WHEN o.status = 'Completed'
                THEN oi.quantity * oi.unit_price
                ELSE 0
            END
        ),
        0
    ) AS total_revenue,
    MAX(
        CASE
            WHEN o.status = 'Completed' THEN o.order_date
        END
    ) AS last_order_date
FROM source.customers c
LEFT JOIN source.orders o
    ON c.customer_id = o.customer_id
LEFT JOIN source.order_items oi
    ON o.order_id = oi.order_id
WHERE c.customer_id IN (
    SELECT DISTINCT customer_id
    FROM source.orders
    WHERE updated_at > (
        SELECT last_processed_at
        FROM control.pipeline_watermark
        WHERE pipeline_name = 'customer_summary_etl'
    )
    AND updated_at <= NOW()
)
GROUP BY
    c.customer_id,
    c.customer_name
ORDER BY
    c.customer_id;