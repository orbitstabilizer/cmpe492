-- Create TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- CEX Ticker data table (time-series)
CREATE TABLE IF NOT EXISTS cex_tickers (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(255) NOT NULL,
    bid DECIMAL(20, 8) NOT NULL,
    ask DECIMAL(20, 8) NOT NULL,
    mid_price DECIMAL(20, 8) GENERATED ALWAYS AS ((bid + ask) / 2) STORED,
    volume_24h DECIMAL(30, 8),
    base_volume DECIMAL(30, 8),
    quote_volume DECIMAL(30, 8)
);

-- Create index for faster queries
SELECT create_hypertable('cex_tickers', 'time', if_not_exists => TRUE);
CREATE INDEX idx_cex_tickers_exchange_symbol_time ON cex_tickers (exchange, symbol, time DESC);

-- Price Index table (aggregated CEX data)
CREATE TABLE IF NOT EXISTS price_index (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(255) NOT NULL,
    price_index DECIMAL(20, 8) NOT NULL,
    num_exchanges INT NOT NULL,
    std_dev DECIMAL(20, 8)
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
    tx_hash VARCHAR(66),
    block_number BIGINT,
    trade_size_usd DECIMAL(20, 8),
    trade_size_bin VARCHAR(20),
    swap_direction VARCHAR(10),
    is_sandwich_victim BOOLEAN DEFAULT FALSE,
    is_arbitrage BOOLEAN DEFAULT FALSE
);

SELECT create_hypertable('dex_swaps', 'time', if_not_exists => TRUE);
CREATE INDEX idx_dex_swaps_chain_dex ON dex_swaps (chain, dex, pool_address, time DESC);
CREATE INDEX idx_dex_swaps_tokens ON dex_swaps (token_in, token_out, time DESC);

-- Token metadata table
CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR(66) PRIMARY KEY,
    symbol VARCHAR(255),
    name VARCHAR(255),
    decimals INT,
    chain VARCHAR(50),
    logo_url VARCHAR(500),
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
    fee_tier INT,
    tvl DECIMAL(30, 8),
    volume_24h DECIMAL(30, 8),
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
    analysis_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    lag_hours INT NOT NULL,
    correlation DECIMAL(5, 4),
    granger_p_value DECIMAL(10, 8),
    sample_size INT,
    cex_leads BOOLEAN,
    PRIMARY KEY (analysis_date, symbol, lag_hours)
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

-- CEX orderbook depth/liquidity snapshot table
CREATE TABLE IF NOT EXISTS cex_liquidity_snapshot (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    depth_0_5_pct DECIMAL(30, 8),
    depth_1_pct DECIMAL(30, 8),
    depth_2_pct DECIMAL(30, 8),
    bid_ask_spread_bps INT,
    top_bid DECIMAL(20, 8),
    top_ask DECIMAL(20, 8)
);

SELECT create_hypertable('cex_liquidity_snapshot', 'time', if_not_exists => TRUE);
CREATE INDEX idx_cex_liquidity_snapshot ON cex_liquidity_snapshot (exchange, symbol, time DESC);

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
    tvl_usd DECIMAL(30, 8),
    price DECIMAL(20, 8),
    block_number BIGINT,
    triggered_by_tx VARCHAR(66)
);

SELECT create_hypertable('dex_pool_state', 'time', if_not_exists => TRUE);
CREATE INDEX idx_pool_state_pool_time ON dex_pool_state (pool_address, time DESC);

-- Data ingestion logs (for monitoring)
CREATE TABLE IF NOT EXISTS data_ingestion_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    service_name VARCHAR(100),
    event_type VARCHAR(100),
    status VARCHAR(50),
    message TEXT,
    records_processed INT
);

CREATE INDEX idx_ingestion_logs_service_timestamp ON data_ingestion_logs (service_name, timestamp DESC);

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
    SUM(trade_size_usd) as total_volume_usd,
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
