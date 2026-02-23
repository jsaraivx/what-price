# 💰 What-price ETL ⚙️
**End-to-End Currency Exchange Pipeline**

An end-to-end data engineering project that automates the extraction of historical and actuals currency rates from the Central Bank of Brazil, processes the data for analytical readiness, and visualizes market trends through an interactive dashboard.

![Project Status](https://img.shields.io/badge/Status-Active-success)
![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-orange)
![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-red)


### Live link to app: [WhatPrice ETL](https://whatprice-etl.streamlit.app/)


## 🏗️ Architecture

The project follows a modern ETL (Extract, Transform, Load) architecture managed within a monorepo structure.

```mermaid
graph LR
    A[Central Bank API] -->|Daily Extraction| B(Apache Airflow)
    B -->|Pandas Transformation| B
    B -->|Bulk Insert| C[(Postgres / Neon)]
    C -->|SQL Query| D[Streamlit Dashboard]
    D -->|Analytics| E((End User))

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style C fill:#bfb,stroke:#333,stroke-width:2px
    style D fill:#fbf,stroke:#333,stroke-width:2px
```

### Data Flow

1. **Ingestion (Extract):** Apache Airflow (TaskFlow API) extracts daily CSV files from the Central Bank.
    
2. **Processing (Transform):** Data cleaning, typing (Numeric precision), and normalization using **Pandas**.
    
3. **Storage (Load):** Optimized ingestion (Bulk Insert) into a **PostgreSQL** database hosted on **Neon Tech** (Serverless).
    
4. **Visualization:** Interactive dashboard built with **Streamlit** and **Plotly** to monitor spread and volatility.
    

## 🛠️ Tech Stack

This project was built using industry-standard tools for Data Engineering:

|**Component**|**Technology**|**Description**|
|---|---|---|
|**Orchestration**|Apache Airflow(Astronomer)|Manages DAGs, Retries, and Backfilling.|
|**Database**|PostgreSQL|Bronze/Silver layer storage.|
|**ETL Engine**|SQLAlchemy, Python, SQL|Data manipulation and schema enforcement.|
|**Frontend**|Streamlit|Interactive Web App for analytics.|
|**DevOps**|Docker Compose|Local development environment for Airflow.|

## 📂 Data Source & Schema

**Source:** Banco Central do Brasil (BCB)

**Format:** `.csv` (Daily Files)

**Example URL:** `http://www4.bcb.gov.br/Download/fechamento/20251202.csv`

### Database Schema (`public.currency_quotes_bronze`)

|**Column Name**|**Data Type**|**Description**|
|---|---|---|
|`id`|`BIGINT`|Primary Key (Identity)|
|`quote_date`|`TIMESTAMP`|Date of the quote|
|`currency_code`|`VARCHAR(10)`|ISO Numeric Code (e.g., 790)|
|`type`|`VARCHAR(5)`|Type A (Buy/Sell) or B|
|`currency`|`VARCHAR(5)`|Currency Symbol (e.g., USD, EUR)|
|`buy_rate`|`NUMERIC(20,10)`|Buying rate|
|`sell_rate`|`NUMERIC(20,10)`|Selling rate|
|`parity_buy`|`NUMERIC(20,10)`|Parity for purchase|
|`parity_sell`|`NUMERIC(20,10)`|Parity for sale|
|`processing_date`|`TIMESTAMP`|ETL Ingestion Metadata|

## 🚀 How to Run Locally

### Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/) & Docker Compose
- Python 3.9+ (for the dashboard only)

### 1. Clone the repository

```bash
git clone https://github.com/jsaraivx/what-price.git
cd what-price
```

### 2. Configure environment variables

```bash
cp .env_example .env
# Fill in your Neon DB credentials in .env
```

##### Generate a secure Airflow secret key:
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
# Paste the result as AIRFLOW_SECRET_KEY in .env
```

### 3. Start Airflow (ETL)

```bash
cd whatprice-airflow
docker compose up -d
```

_Access Airflow UI at: `http://localhost:8080`_

```
User:     admin
Password: admin
```

### 4. Configure the Airflow Connection (Neon DB)

Create a Postgres connection in the Airflow UI under **Admin → Connections**:

![Set Airflow connection](docs/Airflow_conn.png)

| Field | Value |
|---|---|
| Connection Id | `pg_conn` |
| Connection Type | `Postgres` |
| Host | your Neon host |
| Schema | your DB name |
| Login / Password | your Neon credentials |
| Port | `5432` |

### 5. Start Dashboard

```bash
cd ../dashboard
pip install -r requirements.txt
streamlit run app.py
```

_Access Dashboard at: `http://localhost:8501`_

### Useful Docker Commands

```bash
docker compose logs -f             # follow all logs
docker compose logs airflow-scheduler -f   # scheduler logs only
docker compose down                # stop everything
docker compose down -v             # stop and delete volumes (full reset)
```

## 📊 Visualizations

The dashboard provides real-time insights into market volatility and exchange rate spreads.

**For default, the ETL will extract data from API for dates after 2020-01-01.**

**You can change this in dag file.**

## 📚 References & Documentation

- [Airflow TaskFlow API Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
    
- [Airflow Templates Reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
    
- [BCB Data Portal](https://dadosabertos.bcb.gov.br/)
    

## 👨‍💻 Author

**João Gabriel Saraiva** - _Data Engineer_
