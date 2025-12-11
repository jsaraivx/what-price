import streamlit as st
import pandas as pd
import plotly.express as px

# --- 1. Data Loading & Processing ---

df = pd.read_parquet("data.parquet")

df['abs_spread'] = df['sell_rate'] - df['buy_rate']
df['spread_pct'] = (df['abs_spread'] / df['buy_rate']) * 100
df['parity_deviation'] = ((df['buy_rate'] - df['parity_buy'])/ df['parity_buy'])*100
datef = '%Y-%m-%d'
df['quote_date'] = pd.to_datetime(df['quote_date'])
df['processing_date'] = pd.to_datetime(df['processing_date'])
df['ingest_lag'] = df['processing_date'] - df['quote_date'] 
df['duration'] = df['ingest_lag']


# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="What-Price | View",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="collapsed"
)

with st.sidebar:
    st.header("⚙️ Pipeline Metadata")
    st.info("Technical Data Ingestion Stats")
    
    last_process = df['processing_date'].max()
    row_count = len(df)
    
    st.markdown(f"""
    **Status:** ✅ Success
    
    **Rows Ingested:** `{row_count}`
    
    **Last Processing:**
    `{last_process.strftime('%Y-%m-%d %H:%M:%S')}`
    
    **Source:** https://www.bcb.gov.br/ API
    """)
    
    st.divider()
    st.caption("Developed by João Saraiva - Data Engineer")

# --- 4. MAIN DASHBOARD CONTENT ---

st.title("📊 Currency Exchange Analytics")
st.markdown("High-performance view of market trends, volatility, and correlations.")


# 1. TIME SERIES AGGREGATION (Resampling)
df['quote_date'] = pd.to_datetime(df['quote_date'])
df_daily = df.set_index('quote_date').groupby('currency')[['buy_rate', 'sell_rate', 'spread_pct']].resample('D').mean().reset_index()

# 2. VOLATILITY CALCULATION
df_volatility = df.groupby('currency')['buy_rate'].std().reset_index()
df_volatility.columns = ['currency', 'volatility_std']

# 3. CORRELATION MATRIX (Top 10 Currencies only to keep it readable)
# Pivot table to structure data for correlation analysis
top_currencies = df['currency'].value_counts().nlargest(10).index
df_pivot = df[df['currency'].isin(top_currencies)].pivot_table(index='quote_date', columns='currency', values='buy_rate')
df_corr = df_pivot.corr()

# --- DASHBOARD VISUALIZATION ---

# --- KPI SECTION ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records Processed", f"{len(df):,}")
with col2:
    avg_spread = df['spread_pct'].mean()
    st.metric("Global Avg Spread", f"{avg_spread:.2f}%")
with col3:
    # Most Volatile Currency
    most_volatile = df_volatility.sort_values('volatility_std', ascending=False).iloc[0]
    st.metric("Most Volatile Currency", most_volatile['currency'], f"±{most_volatile['volatility_std']:.4f}")
with col4:
    latest_date = df['quote_date'].max().strftime('%Y-%m-%d')
    st.metric("Latest Quote", latest_date)

st.divider()

# --- ROW 1: TRENDS OVER TIME (RESAMPLED) ---
st.subheader("📈 Rate Evolution (Daily Average)")
st.caption("Data is **resampled to daily averages** to optimize performance and visualize long-term trends.")

# Filter multiselect for the line chart
selected_currencies = st.multiselect(
    "Select Currencies to Compare:", 
    options=df_daily['currency'].unique(),
    default=df_daily['currency'].unique()[:5] # Pre-select first 5
)

if selected_currencies:
    # Filtering the PRE-AGGREGATED dataframe, not the raw one
    chart_data = df_daily[df_daily['currency'].isin(selected_currencies)]
    
    fig_line = px.line(
        chart_data,
        x='quote_date',
        y='buy_rate',
        color='currency',
        markers=True,
        title="Buy Rate Trends (Daily Aggregated)",
        template="plotly_white"
    )
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("Select currencies above to visualize trends.")

# --- ROW 2: VOLATILITY & CORRELATION ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("⚡ Volatility Ranking (Risk)")
    st.caption("Standard Deviation of Buy Rate. Higher bars = Unstable/Risky currency.")
    
    # Sort and take top 10 most volatile
    top_volatility = df_volatility.sort_values('volatility_std', ascending=False).head(10)
    
    fig_vol = px.bar(
        top_volatility,
        x='currency',
        y='volatility_std',
        color='volatility_std',
        color_continuous_scale='Reds',
        title="Top 10 Most Volatile Currencies"
    )
    st.plotly_chart(fig_vol, use_container_width=True)

with c2:
    st.subheader("🔗 Market Correlations")
    st.caption("Do currencies move together? (1.0 = Perfect Sync, -1.0 = Inverse).")
    
    fig_corr = px.imshow(
        df_corr,
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r", # Red=Correlation, Blue=Inverse
        title="Correlation Matrix (Top 10 Currencies)"
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# --- RAW DATA SAMPLER ---
with st.expander("🔍 Audit Raw Data (Sampled)"):
    st.markdown("Displaying a random sample of 100 rows from the dataset.")
    st.dataframe(df.sample(100).sort_values('quote_date'), use_container_width=True, hide_index=True)