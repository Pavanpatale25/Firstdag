INSERT INTO analytics.city_customer_count (
    city,
    total_customers
)
SELECT
    COALESCE(NULLIF(TRIM(city), ''), 'Unknown'),
    COUNT(*)
FROM source.customers
GROUP BY COALESCE(NULLIF(TRIM(city), ''), 'Unknown')

ON DUPLICATE KEY UPDATE
    total_customers = VALUES(total_customers),
    load_timestamp = CURRENT_TIMESTAMP;