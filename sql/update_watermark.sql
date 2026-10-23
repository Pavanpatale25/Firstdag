
UPDATE control.pipeline_watermark
SET last_processed_at = %s
WHERE pipeline_name = 'customer_summary_etl';
