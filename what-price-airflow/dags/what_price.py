import requests
import pandas
import logging
import pendulum
import datetime

from io import StringIO

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

# local_tz = pendulum.timezone("America/Sao_Paulo")
today = datetime.date.today()
@dag(
    "what_price_etl_2",
    start_date=pendulum.datetime(2025,12,1),
    schedule="@daily",
    default_args={
        "owner": 'jsaraivx',
        "retries": 1
    },
    # start_date=pendulum.datetime(2025,1,1, tz=local_tz),
    catchup=True
)


def what_price_dag():

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


    @task(retries=1)
    def load_df(data):
        StrinIOdata = StringIO(data)

        dcolumns = ("Quote_Date", "Currency_Code","Type","Currency","Buy_Rate","Sell_Rate","Parity_Buy","Parity_Sell")
        dcolumnst = {
            "Quote_Date": str,
            "Currency_Code": str,
            "Type": str,
            "Currency": str,
            "Buy_Rate": float,
            "Sell_Rate": float,
            "Parity_Buy": float,
            "Parity_Sell": float
            }

        try:
            df = pandas.read_csv(StrinIOdata, sep=";", decimal=",", encoding="utf-8", thousands=".", header=None, names=dcolumns, dtype=dcolumnst, parse_dates=['Quote_Date'])
            df['Processing_date'] = datetime.datetime.today()
        except Exception as e:
            logging.exception(e)
            return None
        if df.empty:
            logging.warning("not data for today!")
            return None
        else:
            logging.warning("sucess on data scrap!")
            return df
    @task
    def create_table():
        pgh = PostgresHook(postgres_conn_id="pg_conn")
        conn = pgh.get_conn()
        curs = conn.cursor()
        try:
            curs.execute("""
                CREATE TABLE IF NOT EXISTS public.prices (
                    id SERIAL PRIMARY KEY,
                    Quote_Date TIMESTAMP,
                    Currency_Code VARCHAR(10),
                    Type VARCHAR(50),
                    Currency VARCHAR(4),
                    Buy_Rate FLOAT,
                    Sell_Rate FLOAT,
                    Parity_Buy FLOAT,
                    Parity_Sell FLOAT,
                    Processing_date TIMESTAMP
                )
            """)
            conn.commit()
            logging.warning("Table created successfully")
        except Exception as e:
            logging.warning(f"Error creating table: {e}")
        finally:
            curs.close()
            conn.close()
        
    @task
    def ingest_data(data):
        if data is None:
            logging.warning("No data to ingest")
            return
        
        pgh = PostgresHook(postgres_conn_id="pg_conn")
        conn = pgh.get_conn()
        curs = conn.cursor()
        try:
            for _, row in data.iterrows():
                curs.execute("""
                    INSERT INTO public.prices (
                        Quote_Date,
                        Currency_Code,
                        Type,
                        Currency,
                        Buy_Rate,
                        Sell_Rate,
                        Parity_Buy,
                        Parity_Sell,
                        Processing_date
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['Quote_Date'],
                    row['Currency_Code'],
                    row['Type'],
                    row['Currency'],
                    row['Buy_Rate'],
                    row['Sell_Rate'],
                    row['Parity_Buy'],
                    row['Parity_Sell'],
                    row['Processing_date']
                ))
            conn.commit()
            logging.warning(f"Successfully inserted {len(data)} rows")
        except Exception as e:
            logging.warning(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            curs.close()
            conn.close()

    getdata = extract_data()
    getdf = load_df(getdata)
    finaldbprocess = ingest_data(getdf)

    [getdata >> getdf] >>  finaldbprocess
what_price_dag()