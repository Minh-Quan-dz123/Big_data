-- =========================================
-- INIT KEYSPACE FOR KUBERNETES CASSANDRA CLUSTER
-- =========================================

-- 1. KEYSPACE (MULTI NODE CLUSTER)
CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'DC1': 3 -- dữ liệu được replicate 3 bản trong DC1
}
AND durable_writes = true; -- đảm bảo ghi xuống commit log → tránh mất dữ liệu

-- 2. USE KEYSPACE
USE ecommerce;

-- TABLE 1: USER SEGMENT
CREATE TABLE IF NOT EXISTS user_segment (
    user_id text PRIMARY KEY,

    days_since_signup int,

    total_orders int,
    total_completed_orders int,
    total_bad_orders int,

    completion_rate double,
    cancellation_rate double,

    cluster int,
    segment_name text
);

-- TABLE 2: USER CONSUMPTION PROFILE
CREATE TABLE IF NOT EXISTS user_consumption_profile (
    user_id text PRIMARY KEY,

    avg_order_value double,
    monthly_spending double,

    -- list products user đã mua gần nhất
    product_ids_in_latest_month list<text>,
    products_in_latest_month list<text>,
    categories_in_latest_month list<text>,

    -- giá sản phẩm
    product_prices_in_latest_month list<double>
);

-- TABLE 3: TRENDING PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS ecommerce.trending_products (
    product_id text PRIMARY KEY,

    trend_score double,
    view_growth double,
    order_growth double,

    trend_window text,
    trend_date bigint
);

-- TABLE 4: PRODUCT SIMILARITY
CREATE TABLE IF NOT EXISTS ecommerce.product_similarity (
    product_id_1 text,
    product_id_2 text,

    similarity_score double,
    similarity_type text,   -- content_based | collaborative | hybrid
    category_match boolean,

    computed_date timestamp,

    PRIMARY KEY (product_id_1, product_id_2)
);

-- TABLE 5: PRODUCT COMPLEMENTARY
CREATE TABLE IF NOT EXISTS ecommerce.product_complementary (
    product_id_1 text,
    product_id_2 text,

    relationship_type text,        -- co_purchase
    co_purchase_count int,

    confidence double,
    category_cross_sell boolean,

    complementary_score double,

    computed_date timestamp,

    PRIMARY KEY (product_id_1, product_id_2)
);

-- TABLE 6: USER RECOMMENDATIONS BATCH
CREATE TABLE IF NOT EXISTS ecommerce.user_recommendations_batch (
    user_id text,
    product_id text,

    segment_name text,
    product_name text,
    category text,

    recommendation_score double,
    recommendation_type text,  -- consumption | similar | complementary | trend | hybrid

    computed_date timestamp,

    PRIMARY KEY (user_id, recommendation_score, product_id)
) 
WITH CLUSTERING ORDER BY (recommendation_score DESC);

-- TABLE 7: REALTIME USER INTEREST
CREATE TABLE IF NOT EXISTS ecommerce.realtime_user_interest (
    user_id text,
    product_id text,

    product_name text,
    category text,

    event_type text,
    event_timestamp bigint,

    computed_score float,

    PRIMARY KEY (user_id, product_id)
);
ALTER TABLE ecommerce.realtime_user_interest
WITH default_time_to_live = 86400;  -- 24 giờ

-- TABLE 8: TRENDING PRODUCTS REALTIME
CREATE TABLE IF NOT EXISTS ecommerce.trending_products_realtime (
    computed_time bigint,
    product_id text,

    category text,
    view_count int,
    cart_count int,
    purchase_count int,
    trend_score double,

    computed_time_ts timestamp,

    PRIMARY KEY ((computed_time), trend_score, product_id)
)
WITH CLUSTERING ORDER BY (
    trend_score DESC,
    product_id ASC
);
ALTER TABLE ecommerce.trending_products_realtime
WITH default_time_to_live = 3600;  -- 1 giờ thôi



CREATE TABLE ecommerce.trending_metadata (
    key text PRIMARY KEY,
    latest_computed_time bigint,
    updated_at timestamp
);
