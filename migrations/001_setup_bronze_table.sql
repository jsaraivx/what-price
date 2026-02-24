-- ============================================================
-- What-Price ETL — Database Migration
-- File: 001_setup_bronze_table.sql
-- Run this on your Neon (PostgreSQL) database before
-- activating the DAG for the first time.
-- ============================================================

-- ── 1. Create Bronze Table ────────────────────────────────────
-- Idempotent: does nothing if the table already exists.

CREATE TABLE IF NOT EXISTS public.currency_quotes_bronze (
    id              BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    quote_date      TIMESTAMP       NOT NULL,
    currency_code   VARCHAR(10)     NOT NULL,
    type            VARCHAR(5)      NOT NULL,
    currency        VARCHAR(5),
    buy_rate        NUMERIC(20, 10),
    sell_rate       NUMERIC(20, 10),
    parity_buy      NUMERIC(20, 10),
    parity_sell     NUMERIC(20, 10),
    processing_date TIMESTAMP
);

-- ── 2. UNIQUE Constraint (required for ON CONFLICT DO NOTHING) ─
-- The DAG uses INSERT ... ON CONFLICT (quote_date, currency_code, type) DO NOTHING
-- to prevent duplicate rows on retries or manual reruns.
-- Without this constraint, the ON CONFLICT clause will raise an error.

ALTER TABLE public.currency_quotes_bronze
    ADD CONSTRAINT uq_bronze_quote_date_currency_type
    UNIQUE (quote_date, currency_code, type);

-- ── 3. Indexes for Query Performance ─────────────────────────
-- Speeds up dashboard queries filtering by date and currency.

CREATE INDEX IF NOT EXISTS idx_bronze_quote_date
    ON public.currency_quotes_bronze (quote_date DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_currency
    ON public.currency_quotes_bronze (currency);

CREATE INDEX IF NOT EXISTS idx_bronze_currency_date
    ON public.currency_quotes_bronze (currency, quote_date DESC);

-- ── 4. Silver View (analytical layer) ────────────────────────
-- Pre-computes spread and parity deviation for the dashboard.
-- Only includes Type A currencies (buy/sell rates are meaningful).
-- Recreate with: DROP VIEW IF EXISTS public.currency_quotes_silver; then re-run.

CREATE OR REPLACE VIEW public.currency_quotes_silver AS
SELECT
    id,
    quote_date::date                                        AS quote_date,
    currency_code,
    currency,
    buy_rate,
    sell_rate,
    parity_buy,
    parity_sell,
    processing_date,
    -- Spread metrics
    (sell_rate - buy_rate)                                  AS abs_spread,
    ROUND(
        ((sell_rate - buy_rate) / NULLIF(buy_rate, 0)) * 100,
        6
    )                                                       AS spread_pct,
    -- Parity deviation: how much the buy rate diverges from parity
    ROUND(
        ((buy_rate - parity_buy) / NULLIF(parity_buy, 0)) * 100,
        6
    )                                                       AS parity_deviation_pct,
    -- ETL lag in hours
    EXTRACT(EPOCH FROM (processing_date - quote_date)) / 3600
                                                            AS ingest_lag_hours
FROM public.currency_quotes_bronze
WHERE type = 'A'            -- Type A: standard commercial rates
  AND buy_rate  > 0
  AND sell_rate > 0;

-- ── Done ───────────────────────────────────────────────────────
-- Verify with:
--   SELECT COUNT(*) FROM public.currency_quotes_bronze;
--   SELECT * FROM public.currency_quotes_silver LIMIT 5;
