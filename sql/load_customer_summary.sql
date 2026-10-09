INSERT INTO analytics.customer_summary
(
    customer_id,
    customer_name,
    total_orders,
    total_revenue,
    last_order_date,
    load_timestamp
)
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
    ) AS last_order_date,
    CURRENT_TIMESTAMP
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
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    total_orders = VALUES(total_orders),
    total_revenue = VALUES(total_revenue),
    last_order_date = VALUES(last_order_date),
    load_timestamp = CURRENT_TIMESTAMP;