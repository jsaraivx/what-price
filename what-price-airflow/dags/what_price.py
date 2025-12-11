import requests
import pandas as pd
import logging
import pendulum
import datetime
import numpy as np
from io import StringIO
from airflow.sdk import dag, task
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
        logging.warning(logicaldate)

        url = f"http://www4.bcb.gov.br/Download/fechamento/{logicaldate}.csv"
        logging.warning(url)

        try:
            response = requests.get(url)
            if response.status_code==200:
                data = response.content.decode('utf-8')
                return data
        except Exception as e:
            logging.warning(e)

    @task
    def transform_data(csv_data):
        if not csv_data:
            logging.warning("No data for transform.")
            return None

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
        
        target_fields = [
            'quote_date', 'currency_code', 'type', 'currency', 
            'buy_rate', 'sell_rate', 'parity_buy', 'parity_sell', 'processing_date'
        ]
        
        rows = []
        for row in data_list:
            rows.append(tuple(row[field] for field in target_fields))

        logging.info(f"Start ingestion of {len(rows)} rows...")

        try:
            pgh.insert_rows(
                table="public.currency_quotes_bronze",
                rows=rows,
                target_fields=target_fields,
                commit_every=1000
            )
            logging.info("Data ingested on Database")
            
        except Exception as e:
            logging.error(f"Data ingestion error: {e}")
            raise e

    # WORKFLOW DEFINITION    
    raw_csv = extract_data()
    clean_data = transform_data(raw_csv)
    ingest_data(clean_data)


what_price_etl()