CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(12) NOT NULL,
    week SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    day_name VARCHAR(12) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_id INTEGER PRIMARY KEY,
    country VARCHAR(80) NOT NULL,
    region_name VARCHAR(80) NOT NULL,
    city VARCHAR(80) NOT NULL,
    market_tier VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id INTEGER PRIMARY KEY,
    channel_name VARCHAR(40) NOT NULL,
    device_type VARCHAR(20) NOT NULL,
    campaign_name VARCHAR(80) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_code VARCHAR(20) NOT NULL UNIQUE,
    first_name VARCHAR(80) NOT NULL,
    last_name VARCHAR(80) NOT NULL,
    gender VARCHAR(20) NOT NULL,
    age INTEGER NOT NULL,
    age_band VARCHAR(20) NOT NULL,
    loyalty_tier VARCHAR(20) NOT NULL,
    signup_date DATE NOT NULL,
    city VARCHAR(80) NOT NULL,
    region_id INTEGER NOT NULL REFERENCES dim_region(region_id),
    segment VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY,
    sku VARCHAR(30) NOT NULL UNIQUE,
    product_name VARCHAR(120) NOT NULL,
    category VARCHAR(80) NOT NULL,
    subcategory VARCHAR(80) NOT NULL,
    brand VARCHAR(80) NOT NULL,
    supplier_name VARCHAR(120) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id BIGINT PRIMARY KEY,
    order_number VARCHAR(40) NOT NULL UNIQUE,
    customer_id INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    order_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    ship_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    region_id INTEGER NOT NULL REFERENCES dim_region(region_id),
    channel_id INTEGER NOT NULL REFERENCES dim_channel(channel_id),
    order_status VARCHAR(20) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    shipping_days INTEGER NOT NULL,
    subtotal NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL,
    tax_amount NUMERIC(12, 2) NOT NULL,
    shipping_amount NUMERIC(12, 2) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES fact_orders(order_id),
    product_id INTEGER NOT NULL REFERENCES dim_product(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    discount_amount NUMERIC(12, 2) NOT NULL,
    net_amount NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_returns (
    return_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES fact_orders(order_id),
    order_item_id BIGINT NOT NULL REFERENCES fact_order_items(order_item_id),
    return_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    return_reason VARCHAR(80) NOT NULL,
    return_quantity INTEGER NOT NULL,
    return_amount NUMERIC(12, 2) NOT NULL,
    resolution_status VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS app_analysis_sessions (
    session_id VARCHAR(40) PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    question TEXT NOT NULL,
    approved_sql TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    chart_type VARCHAR(20),
    csv_path TEXT,
    xlsx_path TEXT,
    pdf_path TEXT,
    warnings TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date ON fact_orders(order_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_region ON fact_orders(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_channel ON fact_orders(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_order ON fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product ON fact_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_returns_order ON fact_returns(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_returns_date ON fact_returns(return_date_id);
