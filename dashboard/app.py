import streamlit as st
import pandas as pd
import plotly.express as px
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(
    page_title="What-Price | Crypto Analytics",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* What-Price style Metric Cards styling */
    .stMetric { 
        background: linear-gradient(145deg, #0f172a, #1e293b);
        padding: 20px; 
        border-radius: 12px; 
        border: 1px solid #334155; 
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    div[data-testid="stMetricValue"] { color: #38bdf8; font-weight: 800; }
    div[data-testid="stMetricLabel"] { font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; }
</style>
""", unsafe_allow_html=True)

# --- 2. DATABASE CONNECTION ---
@st.cache_resource
def get_db_conn():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root_dir, '.env'))
    
    db_connection_str = os.getenv("POSTGRES_URL")
    if not db_connection_str and "POSTGRES_URL" in st.secrets:
        db_connection_str = st.secrets["POSTGRES_URL"]
        
    if not db_connection_str:
        st.error("Error: Connection string (POSTGRES_URL) not found.")
        st.stop()

    return create_engine(db_connection_str, connect_args={'sslmode': 'require'})

# --- 3. DATA INGESTION AND PROCESSING ---
@st.cache_data(ttl=300)
def load_data():
    engine = get_db_conn()
    query = "SELECT * FROM public.crypto_market_silver ORDER BY quote_timestamp DESC"
    try:
        df = pd.read_sql(query, engine)
        df['quote_timestamp'] = pd.to_datetime(df['quote_timestamp'])
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data found in What-Price DB. Run your Airflow DAG to populate the Data Warehouse.")
    st.stop()

# Derived datasets
df['quote_day'] = df['quote_timestamp'].dt.date
latest_ts = df['quote_timestamp'].max()
df_latest = df.sort_values('quote_timestamp', ascending=False).drop_duplicates(subset=['symbol']).copy()

# Sparklines Preparation (Price History for the table)
df_historical_prices = df.sort_values('quote_timestamp').groupby('symbol')['current_price'].apply(list).reset_index(name='price_history')
df_latest = df_latest.merge(df_historical_prices, on='symbol', how='left')

# --- 4. SIDEBAR ---
with st.sidebar:
    st.markdown("## 🔍 What-Price")
    st.markdown("*Market Intelligence Engine*")
    
    st.divider()
    
    moeda_ref = st.selectbox(
        "📌 Reference Asset",
        options=sorted(df_latest['symbol'].unique()),
        index=list(sorted(df_latest['symbol'].unique())).index("BTC") if "BTC" in df_latest['symbol'].values else 0
    )
    
    st.divider()
    st.markdown("### Pipeline Status")
    st.markdown(f"**Monitored Assets:** {df_latest['coin_id'].nunique()}")
    st.markdown(f"**Last Extraction:** {latest_ts.strftime('%m/%d/%Y %H:%M')}")
    st.caption("Developed by João Saraiva - Data Engineer")

# --- 5. MAIN DASHBOARD ---
st.title("What-Price Analytics ⚡")
st.markdown("Market monitoring, risk and liquidity derived from the Silver layer of our Data Warehouse.")

# Global KPIs (Always visible)
col1, col2, col3, col4 = st.columns(4)
ref_data = df_latest[df_latest['symbol'] == moeda_ref].iloc[0] if not df_latest[df_latest['symbol'] == moeda_ref].empty else None

with col1:
    if ref_data is not None:
        st.metric(f"Price {moeda_ref}", f"${ref_data['current_price']:,.2f}", f"{ref_data['price_change_pct_24h']:+.2f}%")
with col2:
    st.metric("Global Market Cap", f"${df_latest['market_cap'].sum()/1e12:.2f} Trillion")
with col3:
    top_gainer = df_latest.loc[df_latest['price_change_pct_24h'].idxmax()]
    st.metric(f"24h Highlight ({top_gainer['symbol']})", f"${top_gainer['current_price']:,.4f}", f"{top_gainer['price_change_pct_24h']:+.2f}%")
with col4:
    most_vol = df_latest.loc[df_latest['volatility_24h_pct'].idxmax()]
    st.metric(f"Risk/Volatility ({most_vol['symbol']})", f"{most_vol['volatility_24h_pct']:.2f}%", "Alert", delta_color="inverse")

st.markdown("<br>", unsafe_allow_html=True)

# --- 6. NAVIGATION TABS ---
tab1, tab2, tab3, tab4 = st.tabs(["🌍 Screener", "📈 Trends", "💧 Liquidity and Volatility", "🪙 Tokenomics"])

# --- TAB 1: OVERVIEW ---
with tab1:
    st.subheader("Market Screener")
    
    # Table with Sparklines using advanced st.dataframe
    display_df = df_latest[['market_cap_rank', 'symbol', 'name', 'current_price', 'price_change_pct_24h', 'price_history']].sort_values('market_cap_rank')
    
    st.dataframe(
        display_df,
        column_config={
            "market_cap_rank": st.column_config.NumberColumn("Rank", format="%d"),
            "symbol": "Symbol",
            "name": "Name",
            "current_price": st.column_config.NumberColumn("Price (USD)", format="$%.2f"),
            "price_change_pct_24h": st.column_config.NumberColumn("Change 24h (%)", format="%.2f%%"),
            "price_history": st.column_config.LineChartColumn("Evolution (Trend)")
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )

    st.subheader("Dominance Map (Market Cap)")
    fig_tree = px.treemap(
        df_latest[df_latest['market_cap'] > 0], 
        path=['symbol'], values='market_cap', color='price_change_pct_24h',
        color_continuous_scale='RdYlGn', color_continuous_midpoint=0
    )
    fig_tree.update_layout(margin=dict(t=0, l=0, r=0, b=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_tree, use_container_width=True)

# --- TAB 2: TRENDS ---
with tab2:
    st.subheader("Relative Price Evolution (Base 100)")
    st.caption("Compares the percentage performance of currencies over time independently of nominal price.")
    
    selecionadas = st.multiselect("Select assets:", options=sorted(df_latest['symbol'].unique()), default=[moeda_ref, "ETH"] if "ETH" in df_latest['symbol'].values else [moeda_ref])
    
    if selecionadas:
        df_trend = df[df['symbol'].isin(selecionadas)].copy()
        # Base 100 normalization using the first record of each currency
        df_trend['base_100'] = df_trend.groupby('symbol')['current_price'].transform(lambda x: (x / x.iloc[-1]) * 100)
        
        fig_line = px.line(df_trend, x='quote_timestamp', y='base_100', color='symbol', template="plotly_dark")
        fig_line.update_layout(yaxis_title="Base 100 Index", xaxis_title="", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Select at least one currency to view the chart.")

# --- TAB 3: LIQUIDITY AND VOLATILITY ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Liquidity Matrix")
        st.caption("Volume vs Market Cap. Assets above and to the right are larger and more liquid.")
        fig_scatter = px.scatter(
            df_latest[df_latest['market_cap'] > 0], x='market_cap', y='total_volume',
            size='volume_to_mcap_pct', color='volatility_24h_pct', text='symbol',
            log_x=True, log_y=True, template="plotly_dark", color_continuous_scale='Turbo'
        )
        fig_scatter.update_traces(textposition='top center')
        fig_scatter.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', coloraxis_colorbar=dict(title="Volatility %"))
        st.plotly_chart(fig_scatter, use_container_width=True)

    with c2:
        st.subheader("24h Volatility Ranking")
        st.caption("Price amplitude relative to current price.")
        df_vol = df_latest[['symbol', 'volatility_24h_pct']].sort_values('volatility_24h_pct', ascending=True).tail(15)
        fig_vol = px.bar(df_vol, x='volatility_24h_pct', y='symbol', orientation='h', template="plotly_dark", color='volatility_24h_pct', color_continuous_scale='Reds')
        fig_vol.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', coloraxis_showscale=False)
        st.plotly_chart(fig_vol, use_container_width=True)

# --- TAB 4: TOKENOMICS ---
with tab4:
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Maximum Supply Usage (Supply Ratio)")
        st.caption("The closer to 100%, the less inflationary the asset tends to be.")
        df_supply = df_latest[df_latest['supply_ratio_pct'] > 0][['symbol', 'supply_ratio_pct']].sort_values('supply_ratio_pct', ascending=True).tail(15)
        fig_sup = px.bar(df_supply, x='supply_ratio_pct', y='symbol', orientation='h', template="plotly_dark", color='supply_ratio_pct', color_continuous_scale='Viridis')
        fig_sup.update_layout(xaxis=dict(range=[0, 105]), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_sup, use_container_width=True)

    with c4:
        st.subheader("Distance from Historical Peak (ATH)")
        st.caption("Percentage decline from the highest price ever recorded (All-Time High).")
        df_ath = df_latest[['symbol', 'distance_from_ath_pct']].sort_values('distance_from_ath_pct', ascending=False).tail(15)
        fig_ath = px.bar(df_ath, x='distance_from_ath_pct', y='symbol', orientation='h', template="plotly_dark", color='distance_from_ath_pct', color_continuous_scale='RdBu')
        fig_ath.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_ath, use_container_width=True)