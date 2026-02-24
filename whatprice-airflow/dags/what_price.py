import requests
import pandas as pd
import logging
import pendulum
import datetime
import numpy as np
from io import StringIO
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import time

CACHE_FILE = "/tmp/airflow_start_date_cache.txt"

def get_dynamic_start_date():
    """
    Fetches the max(quote_date) from DB to set proper Airflow DAG start_date dynamically.
    Avoids 2000+ skipped runs triggered by 2020 catches upon fresh project installations.
    Caches the value to avoid DDoS'ing the Database on every 30s Airflow syntax Parse.
    """
    default_date = pendulum.datetime(2020, 1, 1, tz="America/Sao_Paulo")
    
    if os.path.exists(CACHE_FILE):
        if (time.time() - os.path.getmtime(CACHE_FILE)) < 86400: # 24h TTL
            try:
                with open(CACHE_FILE, "r") as f:
                    date_str = f.read().strip()
                if date_str:
                    return pendulum.parse(date_str).tz_convert("America/Sao_Paulo")
            except:
                pass
                
    try:
        pgh = PostgresHook(postgres_conn_id="pg_conn")
        conn = pgh.get_conn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(quote_date) FROM public.currency_quotes_bronze")
            max_date = cursor.fetchone()[0]
            if max_date:
                # Add a 1-day safety overlap to re-evaluate the edge of the ingested block
                start_dt = pendulum.instance(max_date, tz="America/Sao_Paulo").subtract(days=1)
                with open(CACHE_FILE, "w") as f:
                    f.write(start_dt.isoformat())
                return start_dt
    except Exception as e:
        logging.warning(f"Could not fetch dynamic start_date, falling back to 2020: {e}")
        
    return default_date

start_date = get_dynamic_start_date()

@dag(
    "what_price_etl",
    start_date=start_date,
    schedule="@daily",
    default_args={
        "owner": 'jsaraivx',
        "retries": 1
    },
    tags=['finance', 'postgres', 'etl'],
    catchup=True
)


def what_price_etl():

    @task
    def extract_data(**kwargs):
        logicaldate = kwargs.get("logical_date")
        date_str = logicaldate.strftime('%Y%m%d')
        date_for_query = logicaldate.strftime('%Y-%m-%d')

        logging.info(f"Checking if data already exists for {date_for_query}...")

        # ── Smart Catchup: skip dates already in the database ──────────
        # This allows catchup=True to safely backfill from 2020 without
        # re-downloading dates that are already ingested. On a fresh install,
        # all dates are processed. On a reinstall, existing dates are skipped.
        try:
            pgh = PostgresHook(postgres_conn_id="pg_conn")
            row_count = pgh.get_first(
                "SELECT COUNT(*) FROM public.currency_quotes_bronze WHERE quote_date::date = %s",
                parameters=(date_for_query,)
            )[0]

            if row_count > 0:
                raise AirflowSkipException(
                    f"Data for {date_for_query} already exists ({row_count} rows in DB). Skipping."
                )

            logging.info(f"No existing data for {date_for_query}. Proceeding with extraction.")

        except AirflowSkipException:
            raise
        except Exception as e:
            # If pg_conn is not configured yet, log a warning but don't fail
            logging.warning(f"Could not check existing data (pg_conn unavailable?): {e}. Proceeding anyway.")

        # ── Extract from BCB API ────────────────────────────────────────
        url = f"http://www4.bcb.gov.br/Download/fechamento/{date_str}.csv"
        logging.info(f"Request URL: {url}")

        try:
            response = requests.get(url, timeout=30)

            if response.status_code != 200 or not response.content.strip():
                raise AirflowSkipException(
                    f"No data available for {date_str} (holiday, weekend, or API unavailable). "
                    f"HTTP Status: {response.status_code}"
                )

            data = response.content.decode('utf-8')
            logging.info(f"Successfully extracted {len(data)} bytes for {date_str}")
            return data

        except AirflowSkipException:
            raise
        except Exception as e:
            logging.exception(f"Extraction error for {date_str}: {e}")
            raise

    @task
    def transform_data(csv_data):
        if not csv_data:
            raise AirflowSkipException("No CSV data received for transformation.")

        string_io_data = StringIO(csv_data)

        dcolumns = ("Quote_Date", "Currency_Code", "Type", "Currency", "Buy_Rate", "Sell_Rate", "Parity_Buy", "Parity_Sell")
        dtypes = {
            "Quote_Date": str,
            "Currency_Code": str,
            "Type": str,
            "Currency": str
        }

        try:
            df = pd.read_csv(
                string_io_data, sep=";", decimal=",", encoding="utf-8", thousands=".", header=None, names=dcolumns, dtype=dtypes
            )

            df['Quote_Date'] = pd.to_datetime(df['Quote_Date'], format='%d/%m/%Y', errors='coerce')
            df['Processing_date'] = datetime.datetime.now()
            df.columns = [
                'quote_date', 'currency_code', 'type', 'currency',
                'buy_rate', 'sell_rate', 'parity_buy', 'parity_sell', 'processing_date'
            ]

            df['quote_date'] = df['quote_date'].astype(str)
            df['processing_date'] = df['processing_date'].astype(str)

            df = df.replace({np.nan: None, 'NaT': None})

            logging.info(f"Processed DataFrame with {len(df)} rows.")

            return df.to_dict(orient='records')

        except Exception as e:
            logging.exception(f"Transform Error: {e}")
            raise e

    @task
    def ingest_data(data_list):
        if not data_list:
            logging.warning("No data to ingest")
            return

        pgh = PostgresHook(postgres_conn_id="pg_conn")
        conn = pgh.get_conn()
        cursor = conn.cursor()

        target_fields = [
            'quote_date', 'currency_code', 'type', 'currency',
            'buy_rate', 'sell_rate', 'parity_buy', 'parity_sell', 'processing_date'
        ]

        # ON CONFLICT DO NOTHING prevents duplicate rows on DAG retries or manual reruns.
        # Requires a UNIQUE constraint on (quote_date, currency_code, type) in the DB.
        insert_sql = f"""
            INSERT INTO public.currency_quotes_bronze ({', '.join(target_fields)})
            VALUES ({', '.join(['%s'] * len(target_fields))})
            ON CONFLICT (quote_date, currency_code, type) DO NOTHING;
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
    raw_csv = extract_data()
    clean_data = transform_data(raw_csv)
    ingest_data(clean_data)


what_price_etl()