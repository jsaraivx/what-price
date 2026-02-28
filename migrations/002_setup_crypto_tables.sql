-- ============================================================
-- What-Price ETL — Crypto Migration
-- File: 002_setup_crypto_tables.sql
-- Run this on your Neon (PostgreSQL) database to set up
-- the crypto market data schema.
-- ============================================================

-- ── 1. Create Bronze Table ────────────────────────────────────
-- Raw ingestion from CoinGecko /coins/markets endpoint.
-- Idempotent: does nothing if the table already exists.

CREATE TABLE IF NOT EXISTS public.crypto_market_bronze (
    id                    BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    coin_id               VARCHAR(50)     NOT NULL,
    symbol                VARCHAR(20)     NOT NULL,
    name                  VARCHAR(100)    NOT NULL,
    current_price         NUMERIC(20, 8),
    market_cap            BIGINT,
    market_cap_rank       INT,
    total_volume          BIGINT,
    high_24h              NUMERIC(20, 8),
    low_24h               NUMERIC(20, 8),
    price_change_24h      NUMERIC(20, 8),
    price_change_pct_24h  NUMERIC(10, 5),
    circulating_supply    NUMERIC(30, 8),
    max_supply            NUMERIC(30, 8),
    ath                   NUMERIC(20, 8),
    ath_change_pct        NUMERIC(10, 5),
    quote_timestamp       TIMESTAMP       NOT NULL,
    processing_date       TIMESTAMP       NOT NULL
);

-- ── 2. UNIQUE Constraint (required for ON CONFLICT DO NOTHING) ─
-- Prevents duplicate rows when the DAG retries or runs manually.

ALTER TABLE public.crypto_market_bronze
    ADD CONSTRAINT uq_crypto_coin_timestamp
    UNIQUE (coin_id, quote_timestamp);

-- ── 3. Indexes for Query Performance ─────────────────────────
-- Speeds up dashboard queries filtering by timestamp and coin.

CREATE INDEX IF NOT EXISTS idx_crypto_quote_timestamp
    ON public.crypto_market_bronze (quote_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_coin_id
    ON public.crypto_market_bronze (coin_id);

CREATE INDEX IF NOT EXISTS idx_crypto_coin_timestamp
    ON public.crypto_market_bronze (coin_id, quote_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_market_cap_rank
    ON public.crypto_market_bronze (market_cap_rank);

-- ── 4. Silver View (analytical layer) ────────────────────────
-- Pre-computes volatility, supply ratio, and liquidity metrics.

CREATE OR REPLACE VIEW public.crypto_market_silver AS
SELECT
    id,
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_pct_24h,
    circulating_supply,
    max_supply,
    ath,
    ath_change_pct,
    quote_timestamp,
    processing_date,
    -- 24h Volatility: price range relative to current price
    ROUND(
        ((high_24h - low_24h) / NULLIF(current_price, 0)) * 100,
        4
    )                                                       AS volatility_24h_pct,
    -- Supply Ratio: how much of total supply is circulating
    ROUND(
        (circulating_supply / NULLIF(max_supply, 0)) * 100,
        4
    )                                                       AS supply_ratio_pct,
    -- Volume-to-Market-Cap: trading activity relative to size (liquidity proxy)
    ROUND(
        (total_volume::NUMERIC / NULLIF(market_cap, 0)) * 100,
        4
    )                                                       AS volume_to_mcap_pct,
    -- Distance from ATH
    ath_change_pct                                          AS distance_from_ath_pct
FROM public.crypto_market_bronze
WHERE current_price > 0
  AND market_cap > 0;

-- ── Done ───────────────────────────────────────────────────────
-- Verify with:
--   SELECT COUNT(*) FROM public.crypto_market_bronze;
--   SELECT * FROM public.crypto_market_silver LIMIT 5;
