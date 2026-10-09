CREATE INDEX idx_orders_customer_status
ON source.orders (customer_id, status);

CREATE INDEX idx_orders_updated_at_customer
ON source.orders (updated_at, customer_id);

CREATE INDEX idx_orders_status_order_date
ON source.orders (status, order_date);

CREATE INDEX idx_order_items_product_id
ON source.order_items (product_id);