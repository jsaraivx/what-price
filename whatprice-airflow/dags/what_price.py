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

start_date = pendulum.datetime(2020, 1, 1, tz="America/Sao_Paulo")

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
        logicaldate = logicaldate.strftime('%Y%m%d')
        logging.info(f"Extracting data for date: {logicaldate}")

        url = f"http://www4.bcb.gov.br/Download/fechamento/{logicaldate}.csv"
        logging.info(f"Request URL: {url}")

        try:
            response = requests.get(url, timeout=30)

            if response.status_code != 200 or not response.content.strip():
                raise AirflowSkipException(
                    f"No data available for {logicaldate} (holiday, weekend, or API unavailable). "
                    f"HTTP Status: {response.status_code}"
                )

            data = response.content.decode('utf-8')
            logging.info(f"Successfully extracted {len(data)} bytes for {logicaldate}")
            return data

        except AirflowSkipException:
            raise
        except Exception as e:
            logging.exception(f"Extraction error for {logicaldate}: {e}")
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