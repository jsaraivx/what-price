import streamlit as st
import pandas as pd
import plotly.express as px
from io import StringIO

# --- Page Configuration ---

# --- 1. Data Loading & Processing ---

df = pd.read_csv("data-1765334584657.csv")

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

st.title("📊 Currency Exchange Monitoring")
st.markdown("Analytical view of buy rates, sell rates, spreads, and market parity.")

# --- SECTION A: Business KPIs ---
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Monitored Currencies", df['currency'].nunique())

with col2:
    # Highest Spread
    best_spread = df.loc[df['spread_pct'].idxmax()]
    st.metric(
        "Max Spread (%)", 
        f"{best_spread['spread_pct']:.2f}%", 
        f"Currency: {best_spread['currency']}"
    )

with col3:
    # Strongest Currency
    strongest = df.loc[df['buy_rate'].idxmax()]
    st.metric("Highest Value (Nominal)", strongest['currency'], f"{strongest['buy_rate']:.4f}")

with col4:
    # Technical KPI
    ref_date = df['quote_date'].max()
    st.metric("Reference Date", ref_date.strftime('%Y-%m-%d'))

st.markdown("---")

# --- SECTION B: Market Efficiency (Row 1) ---

c1, c2 = st.columns([3, 2]) 

with c1:
    st.subheader("Scatter: Base Rate vs. Spread")
    st.caption("Identifies anomalies. Currencies at the top have high spreads (expensive to trade).")
    
    fig_scatter = px.scatter(
        df,
        x="buy_rate",
        y="spread_pct",
        color="currency",
        size="buy_rate",
        hover_data=["currency", "sell_rate", "parity_buy"],
        log_x=True, # Log scale for value disparity (VES vs PAB)
        title="Currency Value vs. Margin Relationship (%)",
        template="plotly_white"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with c2:
    st.subheader("Top 5 Highest Spreads")
    st.caption("Currencies with the largest difference between buy/sell rates.")
    
    top_spreads = df.nlargest(5, 'spread_pct').sort_values('spread_pct', ascending=True)
    
    fig_bar = px.bar(
        top_spreads,
        x="spread_pct",
        y="currency",
        orientation='h',
        text_auto='.2f',
        color="spread_pct",
        color_continuous_scale="Blues",
    )
    fig_bar.update_layout(xaxis_title="Spread %", yaxis_title=None, showlegend=False)
    st.plotly_chart(fig_bar, use_container_width=True)

# --- SECTION C: Parity & Distribution (Row 2) ---

st.subheader("📉 Market vs. Parity Deviation")
st.caption("Positive values indicate the market rate is **higher** than the theoretical parity (Overvalued).")

# Filtering out near-zero values to clean the chart
df_parity = df[df['parity_deviation'].abs() > 0.01].sort_values('parity_deviation')

fig_parity = px.bar(
    df_parity,
    x="parity_deviation",
    y="currency",
    orientation='h',
    title="Parity Deviation (%)",
    color="parity_deviation",
    color_continuous_scale="RdBu", # Red/Blue diverging scale
    text_auto='.2f'
)
fig_parity.update_layout(xaxis_title="Deviation %", yaxis_title=None, showlegend=False)
st.plotly_chart(fig_parity, use_container_width=True)

# --- SECTION D: Overview & Stats (Row 3) ---

c3, c4 = st.columns(2)

with c3:
    st.subheader("🗺️ Market Map (Treemap)")
    st.caption("Size = Buy Rate (Log adjusted). Color = Spread % (Red is high).")
    
    # Helper for treemap sizing (avoiding zeros or tiny numbers breaking visuals)
    df['log_buy_rate'] = df['buy_rate'].apply(lambda x: 1 if x < 0.0001 else x) 

    fig_tree = px.treemap(
        df,
        path=['currency'],
        values='log_buy_rate', 
        color='spread_pct',
        hover_data=['buy_rate', 'sell_rate'],
        color_continuous_scale='RdYlGn_r', # Red is high spread (Bad/Expensive)
    )
    st.plotly_chart(fig_tree, use_container_width=True)

with c4:
    st.subheader("📦 Spread Distribution Analysis")
    st.caption("Statistical view to detect outliers in profit margins.")

    fig_box = px.box(
        df,
        y="spread_pct",
        points="all",
        hover_data=["currency"],
        title="Spread Variability Box Plot",
        template="plotly_white"
    )
    st.plotly_chart(fig_box, use_container_width=True)

# --- SECTION E: Data Grid (Raw Data) ---
with st.expander("🔍 Inspect Processed Data (Gold Layer)"):
    st.markdown("Raw data table available for audit.")
    
    st.dataframe(
        df[['quote_date', 'currency', 'buy_rate', 'sell_rate', 'spread_pct', 'parity_deviation', 'processing_date']]
        .sort_values(by='currency'),
        column_config={
            "quote_date": st.column_config.DateColumn("Quote Date"),
            "processing_date": st.column_config.DatetimeColumn("Processing Date", format="YYYY-MM-DD HH:mm"),
            "buy_rate": st.column_config.NumberColumn("Buy Rate", format="%.5f"),
            "sell_rate": st.column_config.NumberColumn("Sell Rate", format="%.5f"),
            "spread_pct": st.column_config.ProgressColumn("Spread %", format="%.2f%%", min_value=0, max_value=df['spread_pct'].max()),
            "parity_deviation": st.column_config.NumberColumn("Parity Dev %", format="%.2f%%"),
        },
        use_container_width=True,
        hide_index=True
    )