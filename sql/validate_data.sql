SELECT customer_id,customer_name,total_orders,total_revenue,last_order_date
FROM analytics.customer_summary
WHERE customer_id IS NULL
   OR customer_name IS NULL
   OR TRIM(customer_name) = ''
   OR total_orders < 0
   OR total_revenue < 0;