import streamlit as st
import pandas as pd
import plotly.express as px
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine

# --- 1. PAGE CONFIGURATION (MUST BE THE FIRST STREAMLIT LINE) ---
st.set_page_config(
    page_title="What-Price | Dashboard",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. Database Connection ---
@st.cache_resource
def get_db_conn():
    # Try reading from .env (local) or st.secrets (cloud)
    load_dotenv('.env')
    
    # Prioritize st.secrets if it exists (for Deploy), otherwise use .env
    if "POSTGRES_URL" in st.secrets:
        db_connection_str = st.secrets["POSTGRES_URL"]
    else:
        db_connection_str = os.getenv("POSTGRES_URL")
    
    if not db_connection_str:
        st.error("Connection string not found.")
        st.stop()

    return create_engine(
        db_connection_str,
        connect_args={'sslmode': 'require'} 
    )

# --- 3. Data Loading & Processing ---
@st.cache_data(ttl=3600) # 1 hour cache
def load_data():
    try:
        engine = get_db_conn()
        query = """
        SELECT * FROM currency_quotes_bronze 
        ORDER BY quote_date DESC
        """
        with engine.connect() as conn:
            # Using index_col='id' if your table has a PK, otherwise remove
            df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No processed data available in the Database yet.")
    st.stop()

# --- FEATURE ENGINEERING ---
df['quote_date'] = pd.to_datetime(df['quote_date'])
df['processing_date'] = pd.to_datetime(df['processing_date'])

df['abs_spread'] = df['sell_rate'] - df['buy_rate']
df['spread_pct'] = (df['abs_spread'] / df['buy_rate']) * 100
df['parity_deviation'] = ((df['buy_rate'] - df['parity_buy']) / df['parity_buy']) * 100
df['ingest_lag'] = df['processing_date'] - df['quote_date'] 

# --- 4. SIDEBAR METADATA ---
with st.sidebar:
    st.header("⚙️ Pipeline Metadata")
    st.info("Technical Data Ingestion Stats")
    
    last_process = df['processing_date'].max()
    row_count = len(df)
    
    st.markdown(f"""
    **Status:** ✅ Success
    
    **Rows Ingested:** `{row_count:,}`
    
    **Last Processing:**
    `{last_process.strftime('%Y-%m-%d %H:%M:%S')}`
    
    **Source:** Central Bank of Brazil API
    """)
    
    st.divider()
    st.caption("Developed by João Saraiva - Data Engineer")

# --- 5. MAIN DASHBOARD CONTENT ---

st.title("📊 Currency Exchange Analytics")
st.markdown("High-performance view of market trends, volatility, and correlations.")

# --- DATA PREP FOR CHARTS ---

# A. Time Series Aggregation (Correction with pd.Grouper)
df_daily = df.groupby(['currency', pd.Grouper(key='quote_date', freq='D')])[[
    'buy_rate', 'sell_rate', 'spread_pct'
]].mean().reset_index()

# B. Volatility Calculation
df_volatility = df.groupby('currency')['buy_rate'].std().reset_index()
df_volatility.columns = ['currency', 'volatility_std']

# C. Correlation Matrix (Top 10 Currencies)
top_currencies = df['currency'].value_counts().nlargest(10).index
# Pivot table needs aggregation if there are duplicates on the day
df_pivot = df[df['currency'].isin(top_currencies)].pivot_table(
    index='quote_date', 
    columns='currency', 
    values='buy_rate',
    aggfunc='mean' 
)
df_corr = df_pivot.corr()

# --- DASHBOARD VISUALIZATION ---

# --- KPI SECTION ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records", f"{len(df):,}")
with col2:
    avg_spread = df['spread_pct'].mean()
    st.metric("Global Avg Spread", f"{avg_spread:.2f}%")
with col3:
    if not df_volatility['volatility_std'].isna().all():
        most_volatile = df_volatility.sort_values('volatility_std', ascending=False).iloc[0]
        st.metric("Volatile Currency", most_volatile['currency'], f"±{most_volatile['volatility_std']:.4f}")
    else:
        st.metric("Volatile Currency", "N/A")
with col4:
    latest_date = df['quote_date'].max().strftime('%Y-%m-%d')
    st.metric("Latest Quote", latest_date)

st.divider()

# --- ROW 1: TRENDS OVER TIME ---
st.subheader("📈 Rate Evolution (Daily Average)")
st.caption("Data is **resampled to daily averages** to optimize performance.")

selected_currencies = st.multiselect(
    "Select Currencies to Compare:", 
    options=df_daily['currency'].unique(),
    default=df_daily['currency'].unique()[:5]
)

if selected_currencies:
    chart_data = df_daily[df_daily['currency'].isin(selected_currencies)]
    
    fig_line = px.line(
        chart_data,
        x='quote_date',
        y='buy_rate',
        color='currency',
        markers=True,
        title="Buy Rate Trends",
        template="plotly_white"
    )
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("Select currencies above to visualize trends.")

# --- ROW 2: VOLATILITY & CORRELATION ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("⚡ Volatility Ranking")
    
    top_volatility = df_volatility.sort_values('volatility_std', ascending=False).head(10)
    
    fig_vol = px.bar(
        top_volatility,
        x='currency',
        y='volatility_std',
        color='volatility_std',
        color_continuous_scale='Reds',
        title="Top 10 Most Volatile"
    )
    st.plotly_chart(fig_vol, use_container_width=True)

with c2:
    st.subheader("🔗 Correlations")
    
    fig_corr = px.imshow(
        df_corr,
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r",
        title="Correlation Matrix (Top 10)"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# --- RAW DATA ---
with st.expander("🔍 Audit Raw Data"):
    st.dataframe(df.sample(min(100, len(df))).sort_values('quote_date', ascending=False), use_container_width=True, hide_index=True)