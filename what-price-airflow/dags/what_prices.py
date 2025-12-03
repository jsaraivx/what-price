import requests
import pandas
import logging
from io import StringIO
from datetime import datetime
from airflow.sdk import Asset, dag, task, chain

# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    "what_prices",
    schedule="@daily",
    default_args={
        "owner": 'jsaraivx',
        "retries": 1,
        "start_date": datetime(2025,1,1)
    },
    catchup=True
)
def what_prices():

    @task
    def extract_data(**kwargs):
        ds_nodash = kwargs["ds_nodash"]

        url = f"http://www4.bcb.gov.br/Download/fechamento/{ds_nodash}.csv"
        logging.warning(url)

        try:
            response = requests.get(url)
            if response.status_code==200:
                data = response.content.decode('utf-8')
                return data
        except Exception as e:
            logging.warning(e)


    @task(retries=1)
    def load_df(raw_data):
        StrinIOdata = StringIO(raw_data)

        dcolumns = ("D_cotac", "Cod Moeda","Tipo","Moeda","Taxa Compra","Taxa Venda","Paridade Compra","Paridade Venda")
        dcolumnst = {
            "D_cotac": str,
            "Cod Moeda": str,
            "Tipo": str,
            "Moeda": str,
            "Taxa Compra": bool,
            "Taxa Venda": bool,
            "Paridade Compra": bool,
            "Paridade Venda": bool
            }

        df = pandas.read_csv(StrinIOdata, sep=";", decimal=",", encoding="utf-8", thousands=".", header=None, names=dcolumns, parse_dates=['D_cotac'])

        df['Processing_date'] = datetime.today()
        print(df)
        logging.warning("sucess!")


    getdata = extract_data()
    getdf = load_df(extract_data())
what_prices()