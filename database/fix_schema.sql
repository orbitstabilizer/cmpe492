-- Fix correlation_analysis table schema
-- Run this when database is started

DROP TABLE IF EXISTS correlation_analysis CASCADE;

CREATE TABLE correlation_analysis (
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

SELECT 'correlation_analysis table fixed' as status;

