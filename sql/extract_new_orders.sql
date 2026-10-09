SELECT
    order_id,
    customer_id,
    order_date,
    status,
    updated_at
FROM source.orders
WHERE updated_at > (
    SELECT last_processed_at
    FROM control.pipeline_watermark
    WHERE pipeline_name = 'customer_summary_etl'
)
AND updated_at <= NOW()
ORDER BY updated_at;