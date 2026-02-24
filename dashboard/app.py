import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Exchange Analytics | Enterprise Dashboard",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- INJECT CUSTOM CSS FOR DARK MODE HIGH CONTRAST ---
st.markdown("""
<style>
    /* Premium Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Dark Theme Background */
    .stApp {
        background-color: #0f172a;
        color: #f8fafc;
    }

    /* High Contrast Metrics */
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 800;
        color: #38bdf8; /* Vibrant Cyan */
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.85rem;
        font-weight: 600;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    /* Dark Metric Cards */
    div[data-testid="metric-container"] {
        background-color: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    
    /* Headers & Text */
    h1, h2, h3 {
        color: #f1f5f9 !important;
        font-weight: 700;
    }
    
    /* Selectbox & Inputs styling hint */
    .stSelectbox label {
        color: #94a3b8 !important;
    }

    /* Sidebar Dark Adjustment */
    [data-testid="stSidebar"] {
        background-color: #020617;
        border-right: 1px solid #1e293b;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. DATABASE CONNECTION ---
@st.cache_resource
def get_db_conn():
    # Load from project root .env
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root_dir, '.env'))
    
    db_connection_str = os.getenv("POSTGRES_URL")
    
    # Only try st.secrets if not found in environment (prevents Streamlit warning locally)
    if not db_connection_str:
        try:
            if "POSTGRES_URL" in st.secrets:
                db_connection_str = st.secrets["POSTGRES_URL"]
        except Exception:
            pass
    
    if not db_connection_str:
        st.error("System Error: Connection string (POSTGRES_URL) not found.")
        st.stop()

    return create_engine(
        db_connection_str,
        connect_args={'sslmode': 'require'} 
    )

# --- 3. DATA LOADING & PROCESSING ---
@st.cache_data(ttl=3600)
def load_data():
    try:
        engine = get_db_conn()
        query = """
        SELECT * FROM currency_quotes_bronze 
        ORDER BY quote_date DESC
        """
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Data ingestion failed: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No processed data available in the data warehouse.")
    st.stop()

# --- FEATURE ENGINEERING ---
df['quote_date'] = pd.to_datetime(df['quote_date'])
df['processing_date'] = pd.to_datetime(df['processing_date'])
df['quote_day'] = df['quote_date'].dt.date

# Financial Metrics
df['abs_spread'] = df['sell_rate'] - df['buy_rate']
df['spread_pct'] = (df['abs_spread'] / df['buy_rate']) * 100

# --- 4. SIDEBAR METADATA & CONTROLS ---
with st.sidebar:
    st.markdown("### Pipeline Telemetry")
    
    last_process = df['processing_date'].max()
    row_count = len(df)
    
    st.markdown(f"""
    **Status:** Active
    **Records Ingested:** `{row_count:,}`
    **Last Sync:** `{last_process.strftime('%Y-%m-%d %H:%M:%S')}`
    **Upstream Source:** BCB API
    """)
    
    st.divider()
    
    st.markdown("### Dashboard Controls")
    currencies_available = sorted(df['currency'].unique())
    
    # Default to USD if available, otherwise first currency
    default_index = currencies_available.index('USD') if 'USD' in currencies_available else 0
    
    reference_currency = st.selectbox(
        "Select Reference Currency (KPIs)", 
        options=currencies_available,
        index=default_index
    )
    
    st.divider()
    st.caption("Maintained by Data Engineering")

# --- 5. MAIN DASHBOARD CONTENT ---

st.title("Currency Exchange Analytics")
st.markdown("Market trends, relative volatility, and liquidity distribution.")

# --- DATA PREP FOR CHARTS & KPIS ---

# A. Time Series Aggregation
df_daily = df.groupby(['currency', 'quote_day'])[['buy_rate', 'sell_rate', 'spread_pct']].mean().reset_index()

# B. Relative Volatility (Coefficient of Variation)
df_volatility = df.groupby('currency')['buy_rate'].agg(['std', 'mean']).reset_index()
df_volatility['cv_pct'] = (df_volatility['std'] / df_volatility['mean']) * 100
df_volatility = df_volatility.dropna(subset=['cv_pct'])

# --- KPI SECTION ---
# Calculate metrics for the selected reference currency
ref_data = df_daily[df_daily['currency'] == reference_currency].sort_values('quote_day')

col1, col2, col3, col4 = st.columns(4)

with col1:
    if len(ref_data) >= 2:
        latest_rate = ref_data.iloc[-1]['buy_rate']
        prev_rate = ref_data.iloc[-2]['buy_rate']
        delta_pct = ((latest_rate - prev_rate) / prev_rate) * 100
        st.metric(f"{reference_currency} Latest Rate", f"{latest_rate:.4f}", f"{delta_pct:.2f}% (24h)")
    elif len(ref_data) == 1:
        st.metric(f"{reference_currency} Latest Rate", f"{ref_data.iloc[0]['buy_rate']:.4f}")
    else:
        st.metric(f"{reference_currency} Latest Rate", "N/A")

with col2:
    avg_spread_ref = ref_data['spread_pct'].mean() if not ref_data.empty else 0
    st.metric(f"{reference_currency} Avg Spread", f"{avg_spread_ref:.4f}%")

with col3:
    if not df_volatility.empty:
        most_volatile = df_volatility.sort_values('cv_pct', ascending=False).iloc[0]
        st.metric("Highest Relative Volatility", most_volatile['currency'], f"{most_volatile['cv_pct']:.2f}% CV", delta_color="inverse")
    else:
        st.metric("Highest Relative Volatility", "N/A")

with col4:
    st.metric("Data Freshness (Lag)", "Real-time" if (pd.Timestamp.utcnow().tz_localize(None) - last_process).total_seconds() < 3600 else "Delayed")

st.divider()

# --- ROW 1: TRENDS OVER TIME ---
st.subheader("Exchange Rate Evolution (Base 100 Indexed)")
st.markdown("<span style='font-size: 0.85em; color: gray;'>Rates are indexed to 100 at the start of the period for accurate percentage comparison across different currency scales.</span>", unsafe_allow_html=True)

selected_currencies = st.multiselect(
    "Select Currencies for Trend Comparison:", 
    options=currencies_available,
    default=[reference_currency] if reference_currency in currencies_available else currencies_available[:3]
)

if selected_currencies:
    chart_data = df_daily[df_daily['currency'].isin(selected_currencies)].copy()
    
    # Calculate Base 100 Indexing for fair visual comparison
    chart_data['indexed_rate'] = chart_data.groupby('currency')['buy_rate'].transform(lambda x: (x / x.iloc[0]) * 100)
    
    fig_line = px.line(
        chart_data,
        x='quote_day',
        y='indexed_rate',
        color='currency',
        template="plotly_dark",
        color_discrete_sequence=px.colors.qualitative.Vivid
    )
    
    fig_line.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis_title="Performance Index",
        xaxis_title="",
        legend_title="Currency",
        hovermode="x unified",
        margin=dict(l=0, r=0, t=20, b=0)
    )
    fig_line.update_xaxes(showgrid=False, linecolor="#334155")
    fig_line.update_yaxes(showgrid=True, gridcolor="#1e293b", linecolor="#334155")

    st.plotly_chart(fig_line, width='stretch')
else:
    st.info("Select currencies above to visualize trends.")

# --- ROW 2: VOLATILITY & LIQUIDITY ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("Relative Volatility (CV %)")
    st.markdown("<span style='font-size: 0.85em; color: gray;'>Measures standard deviation relative to the mean. Higher values indicate higher historical risk.</span>", unsafe_allow_html=True)
    
    top_volatility = df_volatility.sort_values('cv_pct', ascending=False).head(10)
    
    fig_vol = px.bar(
        top_volatility,
        x='cv_pct',
        y='currency',
        orientation='h',
        template="plotly_dark",
        color='cv_pct',
        color_continuous_scale='Viridis' # High contrast neon scale
    )
    fig_vol.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis_title="Volatility (CV %)",
        yaxis_title="",
        yaxis={'categoryorder':'total ascending'},
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_showscale=False
    )
    fig_vol.update_xaxes(showgrid=True, gridcolor="#1e293b")
    fig_vol.update_yaxes(showgrid=False)
    
    st.plotly_chart(fig_vol, width='stretch')

with c2:
    st.subheader("Spread Distribution (%)")
    st.markdown("<span style='font-size: 0.85em; color: gray;'>Distribution of percentage difference between sell and buy rates (Liquidity proxy).</span>", unsafe_allow_html=True)
    
    top_10_curr = df['currency'].value_counts().nlargest(10).index
    df_box = df[df['currency'].isin(top_10_curr)]
    
    fig_box = px.box(
        df_box,
        x='spread_pct',
        y='currency',
        orientation='h',
        template="plotly_dark",
        color='currency',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_box.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis_title="Spread %",
        yaxis_title="",
        yaxis={'categoryorder':'median descending'},
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False
    )
    fig_box.update_xaxes(showgrid=True, gridcolor="#1e293b")

    st.plotly_chart(fig_box, width='stretch')

# --- RAW DATA AUDIT ---
st.divider()
with st.expander("Full Data Audit (Data Warehouse)"):
    st.dataframe(
        df.sort_values('quote_date', ascending=False), 
        width='stretch', 
        hide_index=True
    )