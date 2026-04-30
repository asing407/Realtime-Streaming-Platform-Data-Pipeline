-- Drop tables if they exist
DROP TABLE IF EXISTS funnel_metrics CASCADE;
DROP TABLE IF EXISTS trending_products CASCADE;
DROP TABLE IF EXISTS revenue_metrics CASCADE;
DROP TABLE IF EXISTS hourly_summary CASCADE;

-- Funnel Metrics Table
CREATE TABLE funnel_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count BIGINT,
    unique_users BIGINT,
    unique_sessions BIGINT,
    avg_order_value DECIMAL(10,2),
    first_time_buyers BIGINT,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(window_start, event_type)
);

-- Trending Products Table
CREATE TABLE trending_products (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    activity_count BIGINT,
    unique_viewers BIGINT,
    purchases BIGINT,
    cart_adds BIGINT,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(window_start, product_id)
);

-- Revenue Metrics Table
CREATE TABLE revenue_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    category VARCHAR(100),
    payment_method VARCHAR(50),
    total_revenue DECIMAL(12,2),
    order_count BIGINT,
    avg_order_value DECIMAL(10,2),
    high_value_orders BIGINT,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(window_start, category, payment_method)
);

-- Hourly Summary Table
CREATE TABLE hourly_summary (
    id SERIAL PRIMARY KEY,
    hour_start TIMESTAMP NOT NULL UNIQUE,
    total_events BIGINT,
    total_revenue DECIMAL(12,2),
    total_orders BIGINT,
    unique_users BIGINT,
    conversion_rate DECIMAL(5,2),
    avg_order_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_funnel_window ON funnel_metrics(window_start DESC);
CREATE INDEX idx_trending_window ON trending_products(window_start DESC);
CREATE INDEX idx_revenue_window ON revenue_metrics(window_start DESC);
CREATE INDEX idx_hourly_start ON hourly_summary(hour_start DESC);

-- Create views for dashboard
CREATE OR REPLACE VIEW latest_funnel AS
SELECT * FROM funnel_metrics
WHERE window_start >= NOW() - INTERVAL '2 hours'
ORDER BY window_start DESC;

CREATE OR REPLACE VIEW top_products_today AS
SELECT 
    product_name,
    category,
    SUM(activity_count) as total_activity,
    SUM(purchases) as total_purchases,
    SUM(cart_adds) as total_cart_adds
FROM trending_products
WHERE window_start >= CURRENT_DATE
GROUP BY product_name, category
ORDER BY total_activity DESC
LIMIT 10;
