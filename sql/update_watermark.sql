INSERT INTO control.pipeline_watermark
    (pipeline_name, last_processed_at)
VALUES
    ('customer_summary_etl', '1970-01-01 00:00:00')
ON DUPLICATE KEY UPDATE
    pipeline_name = VALUES(pipeline_name);