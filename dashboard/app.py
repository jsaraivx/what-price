import streamlit as st
import pandas as pd
import plotly.express as px
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="What-Price | Crypto Analytics",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS CUSTOMIZADO ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* Estilização dos Cards de Métrica estilo What-Price */
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

# --- 2. CONEXÃO COM BANCO DE DADOS ---
@st.cache_resource
def get_db_conn():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root_dir, '.env'))
    
    db_connection_str = os.getenv("POSTGRES_URL")
    if not db_connection_str and "POSTGRES_URL" in st.secrets:
        db_connection_str = st.secrets["POSTGRES_URL"]
        
    if not db_connection_str:
        st.error("Erro: String de conexão (POSTGRES_URL) não encontrada.")
        st.stop()

    return create_engine(db_connection_str, connect_args={'sslmode': 'require'})

# --- 3. INGESTÃO E PROCESSAMENTO DE DADOS ---
@st.cache_data(ttl=300)
def load_data():
    engine = get_db_conn()
    query = "SELECT * FROM public.crypto_market_silver ORDER BY quote_timestamp DESC"
    try:
        df = pd.read_sql(query, engine)
        df['quote_timestamp'] = pd.to_datetime(df['quote_timestamp'])
        return df
    except Exception as e:
        st.error(f"Falha ao carregar dados: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("Nenhum dado encontrado no What-Price DB. Execute sua DAG do Airflow para popular o Data Warehouse.")
    st.stop()

# Datasets derivados
df['quote_day'] = df['quote_timestamp'].dt.date
latest_ts = df['quote_timestamp'].max()
df_latest = df.sort_values('quote_timestamp', ascending=False).drop_duplicates(subset=['symbol']).copy()

# Preparação de Sparklines (Histórico de Preços para a tabela)
df_historical_prices = df.sort_values('quote_timestamp').groupby('symbol')['current_price'].apply(list).reset_index(name='price_history')
df_latest = df_latest.merge(df_historical_prices, on='symbol', how='left')

# --- 4. BARRA LATERAL (SIDEBAR) ---
with st.sidebar:
    st.markdown("## 🔍 What-Price")
    st.markdown("*Market Intelligence Engine*")
    
    st.divider()
    
    moeda_ref = st.selectbox(
        "📌 Ativo de Referência",
        options=sorted(df_latest['symbol'].unique()),
        index=list(sorted(df_latest['symbol'].unique())).index("BTC") if "BTC" in df_latest['symbol'].values else 0
    )
    
    st.divider()
    st.markdown("### Status do Pipeline")
    st.markdown(f"**Ativos Monitorados:** {df_latest['coin_id'].nunique()}")
    st.markdown(f"**Última Extração:** {latest_ts.strftime('%d/%m/%Y %H:%M')}")
    st.caption("Developed by João Saraiva - Data Engineer")

# --- 5. DASHBOARD PRINCIPAL ---
st.title("What-Price Analytics ⚡")
st.markdown("Monitoramento de mercado, risco e liquidez derivados da camada Silver do nosso Data Warehouse.")

# KPIs Globais (Sempre visíveis)
col1, col2, col3, col4 = st.columns(4)
ref_data = df_latest[df_latest['symbol'] == moeda_ref].iloc[0] if not df_latest[df_latest['symbol'] == moeda_ref].empty else None

with col1:
    if ref_data is not None:
        st.metric(f"Preço {moeda_ref}", f"${ref_data['current_price']:,.2f}", f"{ref_data['price_change_pct_24h']:+.2f}%")
with col2:
    st.metric("Market Cap Global", f"${df_latest['market_cap'].sum()/1e12:.2f} Trilhão")
with col3:
    top_gainer = df_latest.loc[df_latest['price_change_pct_24h'].idxmax()]
    st.metric(f"Destaque 24h ({top_gainer['symbol']})", f"${top_gainer['current_price']:,.4f}", f"{top_gainer['price_change_pct_24h']:+.2f}%")
with col4:
    most_vol = df_latest.loc[df_latest['volatility_24h_pct'].idxmax()]
    st.metric(f"Risco/Volatilidade ({most_vol['symbol']})", f"{most_vol['volatility_24h_pct']:.2f}%", "Atenção", delta_color="inverse")

st.markdown("<br>", unsafe_allow_html=True)

# --- 6. ABAS DE NAVEGAÇÃO ---
tab1, tab2, tab3, tab4 = st.tabs(["🌍 Screener", "📈 Tendências", "💧 Liquidez e Volatilidade", "🪙 Tokenomics"])

# --- ABA 1: VISÃO GERAL ---
with tab1:
    st.subheader("Screener do Mercado")
    
    # Tabela com Sparklines usando st.dataframe avançado
    display_df = df_latest[['market_cap_rank', 'symbol', 'name', 'current_price', 'price_change_pct_24h', 'price_history']].sort_values('market_cap_rank')
    
    st.dataframe(
        display_df,
        column_config={
            "market_cap_rank": st.column_config.NumberColumn("Rank", format="%d"),
            "symbol": "Símbolo",
            "name": "Nome",
            "current_price": st.column_config.NumberColumn("Preço (USD)", format="$%.2f"),
            "price_change_pct_24h": st.column_config.NumberColumn("Variação 24h (%)", format="%.2f%%"),
            "price_history": st.column_config.LineChartColumn("Evolução (Trend)")
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )

    st.subheader("Mapa de Dominância (Market Cap)")
    fig_tree = px.treemap(
        df_latest[df_latest['market_cap'] > 0], 
        path=['symbol'], values='market_cap', color='price_change_pct_24h',
        color_continuous_scale='RdYlGn', color_continuous_midpoint=0
    )
    fig_tree.update_layout(margin=dict(t=0, l=0, r=0, b=0), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_tree, use_container_width=True)

# --- ABA 2: TENDÊNCIAS ---
with tab2:
    st.subheader("Evolução de Preço Relativo (Base 100)")
    st.caption("Compara a performance percentual das moedas ao longo do tempo independentemente do preço nominal.")
    
    selecionadas = st.multiselect("Selecione os ativos:", options=sorted(df_latest['symbol'].unique()), default=[moeda_ref, "ETH"] if "ETH" in df_latest['symbol'].values else [moeda_ref])
    
    if selecionadas:
        df_trend = df[df['symbol'].isin(selecionadas)].copy()
        # Normalização Base 100 usando o primeiro registro de cada moeda
        df_trend['base_100'] = df_trend.groupby('symbol')['current_price'].transform(lambda x: (x / x.iloc[-1]) * 100)
        
        fig_line = px.line(df_trend, x='quote_timestamp', y='base_100', color='symbol', template="plotly_dark")
        fig_line.update_layout(yaxis_title="Índice Base 100", xaxis_title="", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Selecione ao menos uma moeda para visualizar o gráfico.")

# --- ABA 3: LIQUIDEZ E VOLATILIDADE ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Matriz de Liquidez")
        st.caption("Volume vs Market Cap. Ativos acima e à direita são maiores e mais líquidos.")
        fig_scatter = px.scatter(
            df_latest[df_latest['market_cap'] > 0], x='market_cap', y='total_volume',
            size='volume_to_mcap_pct', color='volatility_24h_pct', text='symbol',
            log_x=True, log_y=True, template="plotly_dark", color_continuous_scale='Turbo'
        )
        fig_scatter.update_traces(textposition='top center')
        fig_scatter.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', coloraxis_colorbar=dict(title="Volatilidade %"))
        st.plotly_chart(fig_scatter, use_container_width=True)

    with c2:
        st.subheader("Ranking de Volatilidade 24h")
        st.caption("Amplitude de preço relativa ao preço atual.")
        df_vol = df_latest[['symbol', 'volatility_24h_pct']].sort_values('volatility_24h_pct', ascending=True).tail(15)
        fig_vol = px.bar(df_vol, x='volatility_24h_pct', y='symbol', orientation='h', template="plotly_dark", color='volatility_24h_pct', color_continuous_scale='Reds')
        fig_vol.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', coloraxis_showscale=False)
        st.plotly_chart(fig_vol, use_container_width=True)

# --- ABA 4: TOKENOMICS ---
with tab4:
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Utilização da Oferta Máxima (Supply Ratio)")
        st.caption("Quanto mais próximo de 100%, menos inflacionário o ativo tende a ser.")
        df_supply = df_latest[df_latest['supply_ratio_pct'] > 0][['symbol', 'supply_ratio_pct']].sort_values('supply_ratio_pct', ascending=True).tail(15)
        fig_sup = px.bar(df_supply, x='supply_ratio_pct', y='symbol', orientation='h', template="plotly_dark", color='supply_ratio_pct', color_continuous_scale='Viridis')
        fig_sup.update_layout(xaxis=dict(range=[0, 105]), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_sup, use_container_width=True)

    with c4:
        st.subheader("Distância do Topo Histórico (ATH)")
        st.caption("Queda percentual desde o maior preço já registrado (All-Time High).")
        df_ath = df_latest[['symbol', 'distance_from_ath_pct']].sort_values('distance_from_ath_pct', ascending=False).tail(15)
        fig_ath = px.bar(df_ath, x='distance_from_ath_pct', y='symbol', orientation='h', template="plotly_dark", color='distance_from_ath_pct', color_continuous_scale='RdBu')
        fig_ath.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_ath, use_container_width=True)