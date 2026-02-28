import requests
import pandas as pd
import logging
import pendulum
import datetime
import numpy as np
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import time

CACHE_FILE = "/tmp/airflow_crypto_start_date_cache.txt"
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

def get_dynamic_start_date():
    """
    Fetches the max(quote_timestamp) from DB to set proper Airflow DAG start_date dynamically.
    Caches the value to avoid querying the database on every 30s Airflow DAG parse.
    """
    default_date = pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo")
    
    if os.path.exists(CACHE_FILE):
        if (time.time() - os.path.getmtime(CACHE_FILE)) < 86400: # 24h TTL
            try:
                with open(CACHE_FILE, "r") as f:
                    date_str = f.read().strip()
                if date_str:
                    return pendulum.parse(date_str).in_tz("America/Sao_Paulo")
            except:
                pass
                
    try:
        pgh = PostgresHook(postgres_conn_id="pg_conn")
        conn = pgh.get_conn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(quote_timestamp) FROM public.crypto_market_bronze")
            max_ts = cursor.fetchone()[0]
            if max_ts:
                start_dt = pendulum.instance(max_ts, tz="America/Sao_Paulo").subtract(hours=2)
                with open(CACHE_FILE, "w") as f:
                    f.write(start_dt.isoformat())
                return start_dt
    except Exception as e:
        logging.warning(f"Could not fetch dynamic start_date, falling back to default: {e}")
        
    return default_date

start_date = get_dynamic_start_date()

@dag(
    "crypto_market_etl",
    start_date=start_date,
    schedule="@hourly",
    default_args={
        "owner": 'jsaraivx',
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=1)
    },
    tags=['crypto', 'postgres', 'etl', 'coingecko'],
    catchup=False
)


def crypto_market_etl():

    @task
    def extract_data(**kwargs):
        """
        Extracts top 20 cryptocurrencies by market cap from the CoinGecko API.
        Returns a JSON-serializable list of coin market data.
        """
        url = f"{COINGECKO_BASE_URL}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 20,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h"
        }

        logging.info(f"Requesting CoinGecko API: {url}")

        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 429:
                raise Exception("CoinGecko rate limit hit. Will retry.")

            if response.status_code != 200:
                raise AirflowSkipException(
                    f"CoinGecko API returned status {response.status_code}. "
                    f"Response: {response.text[:200]}"
                )

            data = response.json()
            
            if not data:
                raise AirflowSkipException("CoinGecko returned empty response.")

            logging.info(f"Successfully extracted data for {len(data)} coins.")
            return data

        except AirflowSkipException:
            raise
        except Exception as e:
            logging.exception(f"Extraction error: {e}")
            raise

    @task
    def transform_data(raw_data):
        """
        Transforms CoinGecko JSON into a clean, flat DataFrame ready for Postgres ingestion.
        """
        if not raw_data:
            raise AirflowSkipException("No data received for transformation.")

        records = []
        for coin in raw_data:
            records.append({
                "coin_id":               coin.get("id"),
                "symbol":                coin.get("symbol", "").upper(),
                "name":                  coin.get("name"),
                "current_price":         coin.get("current_price"),
                "market_cap":            coin.get("market_cap"),
                "market_cap_rank":       coin.get("market_cap_rank"),
                "total_volume":          coin.get("total_volume"),
                "high_24h":              coin.get("high_24h"),
                "low_24h":               coin.get("low_24h"),
                "price_change_24h":      coin.get("price_change_24h"),
                "price_change_pct_24h":  coin.get("price_change_percentage_24h"),
                "circulating_supply":    coin.get("circulating_supply"),
                "max_supply":            coin.get("max_supply"),
                "ath":                   coin.get("ath"),
                "ath_change_pct":        coin.get("ath_change_percentage"),
                "quote_timestamp":       coin.get("last_updated"),
                "processing_date":       datetime.datetime.now().isoformat()
            })

        df = pd.DataFrame(records)
        df = df.replace({np.nan: None})

        logging.info(f"Transformed {len(df)} coin records.")
        return df.to_dict(orient='records')

    @task
    def ingest_data(data_list):
        """
        Loads transformed crypto data into crypto_market_bronze.
        Uses ON CONFLICT DO NOTHING for idempotent ingestion.
        """
        if not data_list:
            logging.warning("No data to ingest.")
            return

        pgh = PostgresHook(postgres_conn_id="pg_conn")
        conn = pgh.get_conn()
        cursor = conn.cursor()

        target_fields = [
            'coin_id', 'symbol', 'name', 'current_price', 'market_cap',
            'market_cap_rank', 'total_volume', 'high_24h', 'low_24h',
            'price_change_24h', 'price_change_pct_24h', 'circulating_supply',
            'max_supply', 'ath', 'ath_change_pct', 'quote_timestamp', 'processing_date'
        ]

        insert_sql = f"""
            INSERT INTO public.crypto_market_bronze ({', '.join(target_fields)})
            VALUES ({', '.join(['%s'] * len(target_fields))})
            ON CONFLICT (coin_id, quote_timestamp) DO NOTHING;
        """

        rows = [tuple(row[field] for field in target_fields) for row in data_list]

        logging.info(f"Starting ingestion of {len(rows)} rows...")

        try:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            logging.info(f"Ingestion complete. {cursor.rowcount} new rows inserted (duplicates skipped).")
        except Exception as e:
            conn.rollback()
            logging.error(f"Data ingestion error: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    # WORKFLOW DEFINITION
    raw_json = extract_data()
    clean_data = transform_data(raw_json)
    ingest_data(clean_data)


crypto_market_etl()