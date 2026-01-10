-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Clean up existing tables to ensure a fresh start
DROP MATERIALIZED VIEW IF EXISTS dex_volume_1h CASCADE;
DROP TABLE IF EXISTS dex_swaps CASCADE;
DROP TABLE IF EXISTS price_index CASCADE;
DROP TABLE IF EXISTS tokens CASCADE;
DROP TABLE IF EXISTS pools CASCADE;
DROP TABLE IF EXISTS price_deviations CASCADE;
DROP TABLE IF EXISTS correlation_analysis CASCADE;
DROP TABLE IF EXISTS slippage_analysis CASCADE;
DROP TABLE IF EXISTS dex_pool_state CASCADE;
CREATE DATABASE IF NOT EXISTS crypto_exchange;

-- Price Index table (aggregated CEX data)
CREATE TABLE IF NOT EXISTS price_index (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(255) NOT NULL,
    price_index DECIMAL(20, 8) NOT NULL,
    num_exchanges INT NOT NULL
);

SELECT create_hypertable('price_index', 'time', if_not_exists => TRUE);
CREATE INDEX idx_price_index_symbol_time ON price_index (symbol, time DESC);

-- DEX Swap events table
CREATE TABLE IF NOT EXISTS dex_swaps (
    time TIMESTAMPTZ NOT NULL,
    chain VARCHAR(50) NOT NULL,
    dex VARCHAR(50) NOT NULL,
    pool_address VARCHAR(66) NOT NULL,
    token_in VARCHAR(66) NOT NULL,
    token_out VARCHAR(66) NOT NULL,
    amount_in DECIMAL(100, 18) NOT NULL,
    amount_out DECIMAL(100, 18) NOT NULL,
    price DECIMAL(100, 18) NOT NULL,
    token_in_symbol VARCHAR(50),
    token_out_symbol VARCHAR(50),
    action VARCHAR(10),
    tx_hash VARCHAR(66),
    block_number BIGINT
);

SELECT create_hypertable('dex_swaps', 'time', if_not_exists => TRUE);
CREATE INDEX idx_dex_swaps_chain_dex ON dex_swaps (chain, dex, pool_address, time DESC);
CREATE INDEX idx_dex_swaps_tokens ON dex_swaps (token_in, token_out, time DESC);

-- Token metadata table
CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR(66) PRIMARY KEY,
    symbol VARCHAR(255),
    decimals INT,
    chain VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_tokens_chain_symbol ON tokens (chain, symbol);

-- Pool/Market info table
CREATE TABLE IF NOT EXISTS pools (
    pool_address VARCHAR(66) PRIMARY KEY,
    chain VARCHAR(50) NOT NULL,
    dex VARCHAR(50) NOT NULL,
    token0_address VARCHAR(66) NOT NULL,
    token1_address VARCHAR(66) NOT NULL,
    fee_tier DECIMAL(10, 6),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pools_chain_dex_tokens ON pools (chain, dex, token0_address, token1_address);

-- Price deviation analysis table
CREATE TABLE IF NOT EXISTS price_deviations (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    cex_price DECIMAL(20, 8) NOT NULL,
    dex_price DECIMAL(20, 8) NOT NULL,
    deviation_pct DECIMAL(10, 6),
    spread DECIMAL(20, 8),
    arbitrage_opportunity BOOLEAN DEFAULT FALSE
);

SELECT create_hypertable('price_deviations', 'time', if_not_exists => TRUE);
CREATE INDEX idx_price_deviations_symbol_time ON price_deviations (symbol, time DESC);

-- Lead-lag correlation table
CREATE TABLE IF NOT EXISTS correlation_analysis (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol VARCHAR(20) NOT NULL,
    period VARCHAR(10) NOT NULL,
    cex_dex_correlation DECIMAL(10, 8),
    dex_leading BOOLEAN,
    lead_lag_periods INT,
    lead_lag_seconds DECIMAL(10, 2),
    cex_volatility DECIMAL(15, 10),
    dex_volatility DECIMAL(15, 10),
    price_deviation_mean DECIMAL(15, 10),
    price_deviation_std DECIMAL(15, 10)
);

-- Slippage modeling results
CREATE TABLE IF NOT EXISTS slippage_analysis (
    time TIMESTAMPTZ NOT NULL,
    dex VARCHAR(50) NOT NULL,
    token_pair VARCHAR(50) NOT NULL,
    token_amount DECIMAL(40, 8) NOT NULL,
    actual_slippage DECIMAL(10, 6),
    predicted_slippage DECIMAL(10, 6),
    liquidity_depth DECIMAL(30, 8)
);

SELECT create_hypertable('slippage_analysis', 'time', if_not_exists => TRUE);
CREATE INDEX idx_slippage_analysis_dex_pair_time ON slippage_analysis (dex, token_pair, time DESC);

-- DEX pool state history table
CREATE TABLE IF NOT EXISTS dex_pool_state (
    time TIMESTAMPTZ NOT NULL,
    pool_address VARCHAR(66) NOT NULL,
    chain VARCHAR(50) NOT NULL,
    dex VARCHAR(50) NOT NULL,
    reserve0 DECIMAL(40, 8),
    reserve1 DECIMAL(40, 8),
    sqrt_price_x96 NUMERIC(78, 0),
    tick INT,
    liquidity NUMERIC(78, 0),
    price DECIMAL(100, 18),
    block_number BIGINT,
    triggered_by_tx VARCHAR(66)
);

SELECT create_hypertable('dex_pool_state', 'time', if_not_exists => TRUE);
CREATE INDEX idx_pool_state_pool_time ON dex_pool_state (pool_address, time DESC);

-- Materialized view for volume rollups (performance optimization)
CREATE MATERIALIZED VIEW IF NOT EXISTS dex_volume_1h
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS bucket,
    chain,
    dex,
    pool_address,
    COUNT(*) as swap_count,
    SUM(amount_in) as total_amount_in,
    SUM(amount_out) as total_amount_out,
    AVG(price) as avg_price
FROM dex_swaps
GROUP BY bucket, chain, dex, pool_address;

-- Add refresh policy (update every hour)
SELECT add_continuous_aggregate_policy('dex_volume_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- Grant permissions
GRANT CONNECT ON DATABASE crypto_exchange TO cmpe492;
GRANT USAGE ON SCHEMA public TO cmpe492;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cmpe492;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cmpe492;


DROP VIEW IF EXISTS dex_trades;

CREATE VIEW dex_trades AS
SELECT
    CASE
        WHEN t1.symbol = 'USDT' THEN 'buy'
        ELSE 'sell'
    END AS side,

    CASE
        WHEN t1.symbol = 'USDT'
            THEN t2.symbol || '/' || t1.symbol
        ELSE
            t1.symbol || '/' || t2.symbol
    END AS symbol_pair,

    CASE
        WHEN t1.symbol = 'USDT'
            THEN ds.amount_out      -- bought quantity
        ELSE
            ds.amount_in           -- sold quantity
    END AS quantity,

    CASE
        WHEN t1.symbol = 'USDT'
            THEN ds.amount_in       -- USDT spent
        ELSE
            ds.amount_out           -- USDT received
    END AS notional,

    ds.price
FROM dex_swaps ds
JOIN tokens t1 ON ds.token_in = t1.address
JOIN tokens t2 ON ds.token_out = t2.address;
