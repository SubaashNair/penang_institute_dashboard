"""
Penang Key Statistics Dashboard
Data sourced from OpenDOSM (open.dosm.gov.my) via the data.gov.my API.
Mirrors the datasets tracked by the Penang Institute (github.com/Penang-Institute).
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import io
from datetime import datetime
from urllib.parse import urlencode

# ── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Key Penang Statistics",
    # page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Main background */
    .stApp {
        background-color: #0e1117;
    }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1f2e 0%, #151923 100%);
        border: 1px solid rgba(99, 179, 237, 0.15);
        border-radius: 12px;
        padding: 20px 24px;
        transition: border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        border-color: rgba(99, 179, 237, 0.4);
    }
    div[data-testid="stMetric"] label {
        color: #8899a6 !important;
        font-size: 0.85rem !important;
        letter-spacing: 0.03em;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #e8edf2 !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background: #1a1f2e;
        border-radius: 8px 8px 0 0;
        border: 1px solid rgba(99, 179, 237, 0.1);
        border-bottom: none;
        padding: 10px 20px;
        color: #8899a6;
    }
    .stTabs [aria-selected="true"] {
        background: #1e2738 !important;
        border-color: rgba(99, 179, 237, 0.3) !important;
        color: #63b3ed !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #12161e;
        border-right: 1px solid rgba(99, 179, 237, 0.08);
    }

    /* Plotly chart containers */
    .stPlotlyChart {
        background: #151923;
        border-radius: 12px;
        padding: 8px;
        border: 1px solid rgba(99, 179, 237, 0.08);
    }

    /* Headers */
    h1, h2, h3 {
        color: #e8edf2 !important;
    }

    /* Dividers */
    hr {
        border-color: rgba(99, 179, 237, 0.1) !important;
    }

    /* Expander */
    .streamlit-expanderHeader {
        background: #1a1f2e !important;
        border-radius: 8px;
    }

    /* Footer */
    .footer-text {
        text-align: center;
        color: #556677;
        font-size: 0.8rem;
        padding: 30px 0 10px 0;
        border-top: 1px solid rgba(99, 179, 237, 0.08);
        margin-top: 40px;
    }
    .footer-text a {
        color: #63b3ed;
        text-decoration: none;
    }
</style>
""", unsafe_allow_html=True)


# ── Data Fetching ────────────────────────────────────────────────────────────

API_BASE = "https://api.data.gov.my/data-catalogue/"
STORAGE_BASE = "https://storage.dosm.gov.my"


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_api(dataset_id, extra_params=None, limit=100):
    """Fetch data from the data.gov.my catalogue API."""
    params = {
        "id": dataset_id,
        "sort": "-date",
        "ifilter": "pulau pinang@state",
        "limit": limit,
    }
    if extra_params:
        params.update(extra_params)
    url = f"{API_BASE}?{urlencode(params)}"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data:
            return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Could not fetch {dataset_id}: {e}")
    return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_population_parquet():
    """Download and parse the population parquet from DOSM storage."""
    url = f"{STORAGE_BASE}/population/population_state.parquet"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))
        return df[df["state"] == "Pulau Pinang"].copy()
    except Exception as e:
        st.warning(f"Could not fetch population data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_trade_data():
    """Fetch Penang trade data from the Penang Institute's prepared Google Sheet."""
    url = (
        "https://docs.google.com/spreadsheets/d/e/"
        "2PACX-1vTXPphxXsVCens8610825ZL2PlrfwJLDRFgnIohNM3omCzQnbtB5U9zhlV_UD-Ph_Kj7R_jadtsMc7a"
        "/pub?gid=1137776307&single=true&output=csv"
    )
    try:
        df = pd.read_csv(url)
        # Filter for Pulau Pinang
        penang = df[df["State"] == "Pulau Pinang"].copy()
        return penang
    except Exception as e:
        st.warning(f"Could not fetch trade data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_all_states_api(dataset_id, extra_params=None, limit=500):
    """Fetch data for ALL states (no ifilter) for comparison charts."""
    params = {
        "id": dataset_id,
        "sort": "-date",
        "limit": limit,
    }
    if extra_params:
        params.update(extra_params)
    url = f"{API_BASE}?{urlencode(params)}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data:
            return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Could not fetch {dataset_id} (all states): {e}")
    return pd.DataFrame()


# ── Chart Theme ──────────────────────────────────────────────────────────────

CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, system-ui, sans-serif", color="#c0cad8"),
    margin=dict(l=40, r=20, t=50, b=40),
    xaxis=dict(gridcolor="rgba(99,179,237,0.06)", zeroline=False),
    yaxis=dict(gridcolor="rgba(99,179,237,0.06)", zeroline=False),
    hoverlabel=dict(
        bgcolor="#1e2738",
        font_size=13,
        font_color="#e8edf2",
        bordercolor="rgba(99,179,237,0.3)",
    ),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)

COLORS = {
    "primary": "#63b3ed",
    "secondary": "#9f7aea",
    "accent": "#48bb78",
    "warn": "#f6ad55",
    "danger": "#fc8181",
    "info": "#76e4f7",
    "palette": [
        "#63b3ed", "#9f7aea", "#48bb78", "#f6ad55",
        "#fc8181", "#76e4f7", "#f687b3", "#b794f4",
    ],
}


def apply_layout(fig, title="", height=400):
    """Apply consistent chart layout."""
    fig.update_layout(**CHART_LAYOUT, title=dict(text=title, x=0.02, font_size=16), height=height)
    return fig


# ── Helper Functions ─────────────────────────────────────────────────────────

def fmt_quarter(date_str):
    """Convert YYYY-MM-DD to 'Q# YYYY'."""
    d = pd.to_datetime(date_str)
    return f"Q{(d.month + 2) // 3} {d.year}"


def fmt_month(date_str):
    """Convert YYYY-MM-DD to 'Mon YYYY'."""
    d = pd.to_datetime(date_str)
    return d.strftime("%b %Y")


def fmt_year(date_str):
    """Convert YYYY-MM-DD to 'YYYY'."""
    return str(pd.to_datetime(date_str).year)


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Key Penang Statistics")
    st.markdown(
        "Live data from [OpenDOSM](https://open.dosm.gov.my/data-catalogue), "
        "tracking the same datasets as the "
        "[Penang Institute](https://github.com/Penang-Institute/pistats)."
    )
    st.divider()
    st.markdown("##### Data categories")
    st.markdown("""
    - **Economy** — GDP, CPI, Labour, Trade
    - **Social** — Population, Household Income
    - **Demographics** — Births, Deaths, Fertility, Ethnicity, Age
    """)
    st.divider()
    st.markdown(
        '<p style="font-size:0.75rem;color:#556677;">'
        'Data refreshed hourly from data.gov.my API<br>'
        'Source: Department of Statistics Malaysia (DOSM)'
        '</p>',
        unsafe_allow_html=True,
    )


# ── Title ────────────────────────────────────────────────────────────────────

st.markdown(
    '<h1 style="margin-bottom:0; font-size:2rem;">Key Penang Statistics</h1>'
    '<p style="color:#8899a6; margin-top:4px; margin-bottom:24px;">'
    'Live data from OpenDOSM · Penang Institute tracking</p>',
    unsafe_allow_html=True,
)


# ── KPI Cards ────────────────────────────────────────────────────────────────

with st.spinner("Loading key metrics from DOSM..."):
    pop_df = fetch_population_parquet()
    gdp_df = fetch_api("gdp_state_real_supply", {"filter": "p0@sector", "contains": "growth_yoy@series"}, limit=20)
    cpi_df = fetch_api("cpi_state_inflation", {"filter": "overall@division"}, limit=60)
    unemp_df = fetch_api("lfs_qtr_state", limit=40)
    income_df = fetch_api("hh_income_state", limit=20)

# Build KPI row
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

# Population
if not pop_df.empty:
    pop_latest = pop_df[
        (pop_df["sex"] == "both") & (pop_df["age"] == "overall") & (pop_df["ethnicity"] == "overall")
    ].sort_values("date", ascending=False).iloc[0]
    pop_val = pop_latest["population"]
    pop_year = fmt_year(pop_latest["date"])
    kpi1.metric("Population", f"{pop_val / 1000:.2f} mil", f"As of {pop_year}")
else:
    kpi1.metric("Population", "N/A")

# GDP Growth
if not gdp_df.empty:
    gdp_latest = gdp_df.iloc[0]
    gdp_prev = gdp_df.iloc[1] if len(gdp_df) > 1 else None
    gdp_delta = f"{gdp_latest['value'] - gdp_prev['value']:.1f}pp" if gdp_prev is not None else None
    kpi2.metric("GDP Growth", f"{gdp_latest['value']:.2f}%", gdp_delta)
else:
    kpi2.metric("GDP Growth", "N/A")

# CPI Inflation
if not cpi_df.empty:
    cpi_latest = cpi_df.iloc[0]
    cpi_prev = cpi_df.iloc[1] if len(cpi_df) > 1 else None
    cpi_delta = f"{cpi_latest['inflation_yoy'] - cpi_prev['inflation_yoy']:.1f}pp" if cpi_prev is not None else None
    kpi3.metric("CPI Inflation (YoY)", f"{cpi_latest['inflation_yoy']:.1f}%", cpi_delta)
else:
    kpi3.metric("CPI Inflation (YoY)", "N/A")

# Unemployment
if not unemp_df.empty:
    u_latest = unemp_df.iloc[0]
    u_prev = unemp_df.iloc[1] if len(unemp_df) > 1 else None
    u_delta = f"{u_latest['u_rate'] - u_prev['u_rate']:.1f}pp" if u_prev is not None else None
    kpi4.metric("Unemployment", f"{u_latest['u_rate']:.1f}%", u_delta, delta_color="inverse")
else:
    kpi4.metric("Unemployment", "N/A")

# Household Income
if not income_df.empty:
    inc_latest = income_df.iloc[0]
    inc_prev = income_df.iloc[1] if len(income_df) > 1 else None
    inc_delta = f"+RM{inc_latest['income_median'] - inc_prev['income_median']:,.0f}" if inc_prev is not None and inc_latest['income_median'] > inc_prev['income_median'] else None
    kpi5.metric("Median Income", f"RM{inc_latest['income_median']:,.0f}", inc_delta)
else:
    kpi5.metric("Median Income", "N/A")

st.divider()

# ── Tabs ─────────────────────────────────────────────────────────────────────

tab_econ, tab_social, tab_demo = st.tabs(["Economy", "Social", "Demographics"])

# ═══════════════════════════════════════════════════════════════════════════
#  ECONOMY TAB
# ═══════════════════════════════════════════════════════════════════════════

with tab_econ:
    st.markdown("### Economic Indicators")
    st.markdown(
        '<p style="color:#8899a6; font-size:0.85rem;">'
        'GDP, consumer prices, labour force, and trade performance for Penang.</p>',
        unsafe_allow_html=True,
    )

    # ── GDP Section ─────────────────────────────────────────────────────
    col_gdp1, col_gdp2 = st.columns([3, 2])

    with col_gdp1:
        if not gdp_df.empty:
            df_g = gdp_df.sort_values("date").copy()
            df_g["year"] = df_g["date"].apply(fmt_year)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_g["year"], y=df_g["value"],
                marker_color=[COLORS["accent"] if v >= 0 else COLORS["danger"] for v in df_g["value"]],
                hovertemplate="<b>%{x}</b><br>GDP Growth: %{y:.2f}%<extra></extra>",
            ))
            fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.15)")
            apply_layout(fig, "GDP Growth Rate (% YoY) — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

    with col_gdp2:
        # GDP by sector (absolute values)
        gdp_sector_df = fetch_api(
            "gdp_state_real_supply",
            {"contains": "abs@series"},
            limit=200,
        )
        if not gdp_sector_df.empty:
            sector_labels = {
                "p0": "Total", "p1": "Agriculture", "p2": "Mining",
                "p3": "Manufacturing", "p4": "Construction", "p5": "Services",
            }
            latest_date = gdp_sector_df["date"].max()
            latest_sector = gdp_sector_df[
                (gdp_sector_df["date"] == latest_date) &
                (gdp_sector_df["sector"] != "p0")
            ].copy()
            latest_sector["sector_name"] = latest_sector["sector"].map(sector_labels)
            latest_sector = latest_sector.dropna(subset=["sector_name"])
            if not latest_sector.empty:
                fig = px.pie(
                    latest_sector, values="value", names="sector_name",
                    color_discrete_sequence=COLORS["palette"],
                    hole=0.45,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                apply_layout(fig, f"GDP by Sector ({fmt_year(latest_date)})", height=380)
                st.plotly_chart(fig, use_container_width=True)

    # ── CPI Section ─────────────────────────────────────────────────────
    st.markdown("---")

    col_cpi1, col_cpi2 = st.columns([3, 2])

    with col_cpi1:
        if not cpi_df.empty:
            df_c = cpi_df.sort_values("date").copy()
            df_c["month"] = df_c["date"].apply(fmt_month)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(df_c["date"]), y=df_c["inflation_yoy"],
                mode="lines+markers",
                line=dict(color=COLORS["warn"], width=2.5),
                marker=dict(size=5, color=COLORS["warn"]),
                fill="tozeroy",
                fillcolor="rgba(246,173,85,0.08)",
                hovertemplate="<b>%{text}</b><br>Inflation: %{y:.1f}%<extra></extra>",
                text=df_c["month"],
            ))
            fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.15)")
            apply_layout(fig, "CPI Inflation (% YoY) — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

    with col_cpi2:
        # CPI by division (latest month)
        cpi_div_df = fetch_api("cpi_state", limit=30)
        if not cpi_div_df.empty:
            div_labels = {
                "01": "Food & Beverages",
                "02": "Alcohol & Tobacco",
                "03": "Clothing",
                "04": "Housing & Utilities",
                "05": "Furnishing",
                "06": "Health",
                "07": "Transport",
                "08": "Communication",
                "09": "Recreation",
                "10": "Education",
                "11": "Restaurants & Hotels",
                "12": "Insurance & Finance",
                "13": "Personal Care",
                "overall": "Overall",
            }
            latest_date = cpi_div_df["date"].max()
            div_latest = cpi_div_df[
                (cpi_div_df["date"] == latest_date) & (cpi_div_df["division"] != "overall")
            ].copy()
            div_latest["division_name"] = div_latest["division"].map(div_labels)
            div_latest = div_latest.dropna(subset=["division_name"])
            if not div_latest.empty:
                div_latest = div_latest.sort_values("index", ascending=True)
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    y=div_latest["division_name"], x=div_latest["index"],
                    orientation="h",
                    marker_color=COLORS["primary"],
                    hovertemplate="<b>%{y}</b><br>CPI Index: %{x:.1f}<extra></extra>",
                ))
                apply_layout(fig, f"CPI by Division ({fmt_month(latest_date)})", height=420)
                fig.update_layout(yaxis=dict(tickfont_size=11))
                st.plotly_chart(fig, use_container_width=True)

    # ── Labour Force Section ────────────────────────────────────────────
    st.markdown("---")

    col_lab1, col_lab2 = st.columns(2)

    with col_lab1:
        if not unemp_df.empty:
            df_u = unemp_df.sort_values("date").copy()
            df_u["quarter"] = df_u["date"].apply(fmt_quarter)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(df_u["date"]), y=df_u["u_rate"],
                mode="lines+markers",
                line=dict(color=COLORS["danger"], width=2.5),
                marker=dict(size=6, color=COLORS["danger"]),
                fill="tozeroy",
                fillcolor="rgba(252,129,129,0.08)",
                hovertemplate="<b>%{text}</b><br>Unemployment: %{y:.1f}%<extra></extra>",
                text=df_u["quarter"],
            ))
            apply_layout(fig, "Unemployment Rate (%) — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

    with col_lab2:
        if not unemp_df.empty and "lf" in unemp_df.columns:
            df_lf = unemp_df.sort_values("date").copy()
            df_lf["quarter"] = df_lf["date"].apply(fmt_quarter)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(df_lf["date"]), y=df_lf["lf_employed"],
                name="Employed", mode="lines",
                line=dict(color=COLORS["accent"], width=2),
                stackgroup="one",
                hovertemplate="<b>%{text}</b><br>Employed: %{y:.1f}k<extra></extra>",
                text=df_lf["quarter"],
            ))
            fig.add_trace(go.Scatter(
                x=pd.to_datetime(df_lf["date"]), y=df_lf["lf_unemployed"],
                name="Unemployed", mode="lines",
                line=dict(color=COLORS["danger"], width=2),
                stackgroup="one",
                hovertemplate="<b>%{text}</b><br>Unemployed: %{y:.1f}k<extra></extra>",
                text=df_lf["quarter"],
            ))
            apply_layout(fig, "Labour Force Composition ('000) — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

    # ── Participation rate ──────────────────────────────────────────────
    if not unemp_df.empty and "p_rate" in unemp_df.columns:
        df_p = unemp_df.sort_values("date").copy()
        df_p["quarter"] = df_p["date"].apply(fmt_quarter)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(df_p["date"]), y=df_p["p_rate"],
            mode="lines+markers",
            line=dict(color=COLORS["info"], width=2.5),
            marker=dict(size=5),
            hovertemplate="<b>%{text}</b><br>Participation: %{y:.1f}%<extra></extra>",
            text=df_p["quarter"],
        ))
        apply_layout(fig, "Labour Force Participation Rate (%) — Penang", height=340)
        st.plotly_chart(fig, use_container_width=True)

    # ── Trade Section ───────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### External Trade")

    trade_df = fetch_trade_data()
    if not trade_df.empty:
        # Ensure Value is numeric
        trade_df["Value (RM mil)"] = pd.to_numeric(trade_df["Value (RM mil)"], errors="coerce")
        trade_df = trade_df.dropna(subset=["Value (RM mil)"])

        # Aggregate by date and trade type for Penang
        trade_agg = trade_df.groupby(["Date", "Type of trade"])["Value (RM mil)"].sum().reset_index()
        trade_agg["Date"] = pd.to_datetime(trade_agg["Date"])
        trade_agg = trade_agg.sort_values("Date")

        col_t1, col_t2 = st.columns([3, 2])

        with col_t1:
            fig = go.Figure()
            for ttype, color in [("Export", COLORS["accent"]), ("Import", COLORS["warn"])]:
                subset = trade_agg[trade_agg["Type of trade"] == ttype]
                fig.add_trace(go.Scatter(
                    x=subset["Date"], y=subset["Value (RM mil)"],
                    name=ttype + "s", mode="lines",
                    line=dict(color=color, width=2),
                    hovertemplate="<b>%{x|%b %Y}</b><br>" + ttype + ": RM%{y:,.0f}m<extra></extra>",
                ))
            apply_layout(fig, "External Trade (RM mil) — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

        with col_t2:
            # Trade balance
            exports = trade_agg[trade_agg["Type of trade"] == "Export"].set_index("Date")["Value (RM mil)"]
            imports = trade_agg[trade_agg["Type of trade"] == "Import"].set_index("Date")["Value (RM mil)"]
            balance = (exports - imports).dropna().reset_index()
            balance.columns = ["Date", "Balance"]
            if not balance.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=balance["Date"], y=balance["Balance"],
                    marker_color=[COLORS["accent"] if v >= 0 else COLORS["danger"] for v in balance["Balance"]],
                    hovertemplate="<b>%{x|%b %Y}</b><br>Balance: RM%{y:,.0f}m<extra></extra>",
                ))
                fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.15)")
                apply_layout(fig, "Trade Balance (RM mil) — Penang", height=380)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(
            "External trade data could not be loaded. "
            "See [Penang Institute](https://statistics.penanginstitute.org/) for the full trade dashboard."
        )

# ═══════════════════════════════════════════════════════════════════════════
#  SOCIAL TAB
# ═══════════════════════════════════════════════════════════════════════════

with tab_social:
    st.markdown("### Social Indicators")
    st.markdown(
        '<p style="color:#8899a6; font-size:0.85rem;">'
        'Population trends, household income, and inequality metrics.</p>',
        unsafe_allow_html=True,
    )

    # ── Population Trend ────────────────────────────────────────────────
    col_pop1, col_pop2 = st.columns([3, 2])

    with col_pop1:
        if not pop_df.empty:
            pop_trend = pop_df[
                (pop_df["sex"] == "both") & (pop_df["age"] == "overall") & (pop_df["ethnicity"] == "overall")
            ].sort_values("date").copy()
            pop_trend["year"] = pop_trend["date"].apply(lambda x: str(pd.to_datetime(x).year))
            pop_trend["pop_mil"] = pop_trend["population"] / 1000

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=pop_trend["year"], y=pop_trend["pop_mil"],
                mode="lines+markers",
                line=dict(color=COLORS["primary"], width=3),
                marker=dict(size=5),
                fill="tozeroy",
                fillcolor="rgba(99,179,237,0.08)",
                hovertemplate="<b>%{x}</b><br>Population: %{y:.3f} mil<extra></extra>",
            ))
            apply_layout(fig, "Population Trend — Penang (millions)", height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col_pop2:
        if not pop_df.empty:
            # Ethnicity breakdown (latest)
            latest_date = pop_df["date"].max()
            eth_df = pop_df[
                (pop_df["date"] == latest_date) &
                (pop_df["sex"] == "both") &
                (pop_df["age"] == "overall") &
                (pop_df["ethnicity"] != "overall")
            ].copy()
            eth_labels = {
                "bumi_malay": "Bumiputera (Malay)",
                "bumi_other": "Bumiputera (Other)",
                "chinese": "Chinese",
                "indian": "Indian",
                "other_citizen": "Other Citizens",
                "other_noncitizen": "Non-citizens",
            }
            eth_df["eth_name"] = eth_df["ethnicity"].map(eth_labels)
            eth_df = eth_df.dropna(subset=["eth_name"])

            if not eth_df.empty:
                fig = px.pie(
                    eth_df, values="population", names="eth_name",
                    color_discrete_sequence=COLORS["palette"],
                    hole=0.4,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label", textfont_size=11)
                apply_layout(fig, f"Ethnic Composition ({fmt_year(latest_date)})", height=400)
                st.plotly_chart(fig, use_container_width=True)

    # ── Household Income ────────────────────────────────────────────────
    st.markdown("---")
    col_inc1, col_inc2 = st.columns(2)

    with col_inc1:
        if not income_df.empty:
            df_i = income_df.sort_values("date").copy()
            df_i["year"] = df_i["date"].apply(fmt_year)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_i["year"], y=df_i["income_median"],
                name="Median", marker_color=COLORS["primary"],
                hovertemplate="<b>%{x}</b><br>Median: RM%{y:,.0f}<extra></extra>",
            ))
            if "income_mean" in df_i.columns:
                fig.add_trace(go.Bar(
                    x=df_i["year"], y=df_i["income_mean"],
                    name="Mean", marker_color=COLORS["secondary"],
                    hovertemplate="<b>%{x}</b><br>Mean: RM%{y:,.0f}<extra></extra>",
                ))
            fig.update_layout(barmode="group")
            apply_layout(fig, "Household Income (RM) — Penang", height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col_inc2:
        if not income_df.empty:
            df_i2 = income_df.sort_values("date").copy()
            df_i2["year"] = df_i2["date"].apply(fmt_year)
            if "income_mean" in df_i2.columns:
                df_i2["gap"] = df_i2["income_mean"] - df_i2["income_median"]
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_i2["year"], y=df_i2["gap"],
                    mode="lines+markers",
                    line=dict(color=COLORS["warn"], width=2.5),
                    marker=dict(size=7),
                    hovertemplate="<b>%{x}</b><br>Mean-Median Gap: RM%{y:,.0f}<extra></extra>",
                ))
                apply_layout(fig, "Income Inequality Indicator (Mean - Median)", height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Mean income data not available for inequality indicator.")

    # ── Population by sex trend ─────────────────────────────────────────
    st.markdown("---")
    if not pop_df.empty:
        sex_trend = pop_df[
            (pop_df["age"] == "overall") &
            (pop_df["ethnicity"] == "overall") &
            (pop_df["sex"].isin(["male", "female"]))
        ].sort_values("date").copy()
        sex_trend["year"] = sex_trend["date"].apply(lambda x: str(pd.to_datetime(x).year))
        sex_trend["pop_k"] = sex_trend["population"]

        if not sex_trend.empty:
            fig = go.Figure()
            for sex, color in [("male", COLORS["primary"]), ("female", COLORS["secondary"])]:
                subset = sex_trend[sex_trend["sex"] == sex]
                fig.add_trace(go.Scatter(
                    x=subset["year"], y=subset["pop_k"],
                    name=sex.capitalize(),
                    mode="lines",
                    line=dict(color=color, width=2.5),
                    hovertemplate="<b>%{x}</b><br>%{y:.1f}k<extra></extra>",
                ))
            apply_layout(fig, "Population by Sex ('000) — Penang", height=360)
            st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════
#  DEMOGRAPHICS TAB
# ═══════════════════════════════════════════════════════════════════════════

with tab_demo:
    st.markdown("### Demographics")
    st.markdown(
        '<p style="color:#8899a6; font-size:0.85rem;">'
        'Births, deaths, fertility, and age structure.</p>',
        unsafe_allow_html=True,
    )

    # ── Births & Deaths ─────────────────────────────────────────────────
    births_df = fetch_api("births_annual_state", limit=30)
    deaths_df = fetch_api("deaths_state", limit=30)

    col_bd1, col_bd2 = st.columns(2)

    with col_bd1:
        if not births_df.empty:
            df_b = births_df.sort_values("date").copy()
            df_b["year"] = df_b["date"].apply(fmt_year)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_b["year"], y=df_b["abs"],
                marker_color=COLORS["accent"],
                hovertemplate="<b>%{x}</b><br>Births: %{y:,.0f}<br><extra></extra>",
            ))
            apply_layout(fig, "Annual Live Births — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

    with col_bd2:
        if not deaths_df.empty:
            df_d = deaths_df.sort_values("date").copy()
            df_d["year"] = df_d["date"].apply(fmt_year)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_d["year"], y=df_d["abs"],
                marker_color=COLORS["danger"],
                hovertemplate="<b>%{x}</b><br>Deaths: %{y:,.0f}<extra></extra>",
            ))
            apply_layout(fig, "Annual Deaths — Penang", height=380)
            st.plotly_chart(fig, use_container_width=True)

    # ── Crude rates ─────────────────────────────────────────────────────
    if not births_df.empty and not deaths_df.empty:
        st.markdown("---")
        df_br = births_df[["date", "rate"]].rename(columns={"rate": "birth_rate"}).sort_values("date")
        df_dr = deaths_df[["date", "rate"]].rename(columns={"rate": "death_rate"}).sort_values("date")
        rates = pd.merge(df_br, df_dr, on="date", how="inner")
        rates["year"] = rates["date"].apply(fmt_year)
        rates["natural_increase"] = rates["birth_rate"] - rates["death_rate"]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rates["year"], y=rates["birth_rate"],
            name="Crude Birth Rate", mode="lines+markers",
            line=dict(color=COLORS["accent"], width=2),
            marker=dict(size=5),
        ))
        fig.add_trace(go.Scatter(
            x=rates["year"], y=rates["death_rate"],
            name="Crude Death Rate", mode="lines+markers",
            line=dict(color=COLORS["danger"], width=2),
            marker=dict(size=5),
        ))
        fig.add_trace(go.Scatter(
            x=rates["year"], y=rates["natural_increase"],
            name="Natural Increase", mode="lines",
            line=dict(color=COLORS["info"], width=2, dash="dash"),
        ))
        apply_layout(fig, "Crude Birth & Death Rates (per 1,000) — Penang", height=380)
        st.plotly_chart(fig, use_container_width=True)

    # ── Fertility ───────────────────────────────────────────────────────
    st.markdown("---")
    fertility_df = fetch_api("fertility_state", limit=100)

    col_fert1, col_fert2 = st.columns(2)

    with col_fert1:
        if not fertility_df.empty:
            tfr = fertility_df[fertility_df["age_group"] == "tfr"].sort_values("date").copy()
            tfr["year"] = tfr["date"].apply(fmt_year)
            if not tfr.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=tfr["year"], y=tfr["fertility_rate"],
                    mode="lines+markers",
                    line=dict(color=COLORS["secondary"], width=2.5),
                    marker=dict(size=6),
                    hovertemplate="<b>%{x}</b><br>TFR: %{y:.2f}<extra></extra>",
                ))
                fig.add_hline(
                    y=2.1, line_dash="dot", line_color="rgba(255,255,255,0.3)",
                    annotation_text="Replacement level (2.1)",
                    annotation_position="top left",
                    annotation_font_color="#8899a6",
                )
                apply_layout(fig, "Total Fertility Rate — Penang", height=380)
                st.plotly_chart(fig, use_container_width=True)

    with col_fert2:
        if not fertility_df.empty:
            latest_fert_date = fertility_df["date"].max()
            asfr = fertility_df[
                (fertility_df["date"] == latest_fert_date) &
                (fertility_df["age_group"] != "tfr")
            ].copy()
            if not asfr.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=asfr["age_group"], y=asfr["fertility_rate"],
                    marker_color=COLORS["secondary"],
                    hovertemplate="Age %{x}<br>Rate: %{y:.1f}<extra></extra>",
                ))
                apply_layout(fig, f"Age-Specific Fertility Rate ({fmt_year(latest_fert_date)})", height=380)
                st.plotly_chart(fig, use_container_width=True)

    # ── Age Distribution (Population Pyramid) ──────────────────────────
    st.markdown("---")
    st.markdown("#### Age Distribution")

    if not pop_df.empty:
        latest_date = pop_df["date"].max()
        age_data = pop_df[
            (pop_df["date"] == latest_date) &
            (pop_df["ethnicity"] == "overall") &
            (pop_df["age"] != "overall")
        ].copy()

        # Define proper age group ordering
        age_order = ["0-4", "5-9", "10-14", "15-19", "20-24", "25-29", "30-34",
                     "35-39", "40-44", "45-49", "50-54", "55-59", "60-64",
                     "65-69", "70-74", "75-79", "80-84", "85+"]
        age_data["age"] = pd.Categorical(age_data["age"], categories=age_order, ordered=True)
        age_data = age_data.dropna(subset=["age"])

        # Get male and female separately
        male_age = age_data[age_data["sex"] == "male"].sort_values("age")
        female_age = age_data[age_data["sex"] == "female"].sort_values("age")

        if not male_age.empty and not female_age.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=male_age["age"],
                x=-male_age["population"],
                name="Male",
                orientation="h",
                marker_color=COLORS["primary"],
                hovertemplate="<b>%{y}</b><br>Male: %{customdata:.1f}k<extra></extra>",
                customdata=male_age["population"],
            ))
            fig.add_trace(go.Bar(
                y=female_age["age"],
                x=female_age["population"],
                name="Female",
                orientation="h",
                marker_color=COLORS["secondary"],
                hovertemplate="<b>%{y}</b><br>Female: %{x:.1f}k<extra></extra>",
            ))
            apply_layout(fig, f"Population Pyramid ({fmt_year(latest_date)})", height=500)
            fig.update_layout(
                barmode="overlay",
                bargap=0.05,
                xaxis=dict(
                    title="Population ('000)",
                    tickvals=[-80, -60, -40, -20, 0, 20, 40, 60, 80],
                    ticktext=["80", "60", "40", "20", "0", "20", "40", "60", "80"],
                ),
                yaxis=dict(title="Age Group"),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Fallback: show total age distribution
            total_age = age_data[age_data["sex"] == "both"].sort_values("age")
            if not total_age.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=total_age["age"], y=total_age["population"],
                    marker_color=COLORS["primary"],
                    hovertemplate="<b>%{x}</b><br>Population: %{y:.1f}k<extra></extra>",
                ))
                apply_layout(fig, f"Population by Age Group ({fmt_year(latest_date)})", height=400)
                st.plotly_chart(fig, use_container_width=True)


# ── Data Source Inventory ────────────────────────────────────────────────────

st.divider()

with st.expander("📋 Data Sources & API Endpoints", expanded=False):
    st.markdown("""
    This dashboard pulls live data from the following [OpenDOSM](https://open.dosm.gov.my/data-catalogue) sources,
    matching the datasets tracked by [Penang Institute](https://github.com/Penang-Institute/pistats):

    | Metric | DOSM Dataset ID | Dashboard Section |
    |--------|----------------|-------------------|
    | Population | `population_state` (parquet) | Social |
    | GDP Growth | `gdp_state_real_supply` | Economy |
    | CPI Inflation | `cpi_state_inflation` | Economy |
    | CPI by Division | `cpi_state` | Economy |
    | Unemployment | `lfs_qtr_state` | Economy |
    | Household Income | `hh_income_state` | Social |
    | Live Births | `births_annual_state` | Demographics |
    | Deaths | `deaths_state` | Demographics |
    | Fertility | `fertility_state` | Demographics |
    | External Trade | Penang Institute (Google Sheets) | Economy |

    All data is filtered for **Pulau Pinang** (Penang) state.
    Data is cached for 1 hour and fetched via the `api.data.gov.my` REST API.
    """)


# ── Footer ───────────────────────────────────────────────────────────────────

st.markdown(
    '<div class="footer-text">'
    'Data source: <a href="https://open.dosm.gov.my/data-catalogue" target="_blank">OpenDOSM</a> · '
    'Department of Statistics Malaysia (DOSM)<br>'
    'Tracking datasets from <a href="https://github.com/Penang-Institute" target="_blank">Penang Institute</a> · '
    '<a href="https://statistics.penanginstitute.org/" target="_blank">statistics.penanginstitute.org</a>'
    '</div>',
    unsafe_allow_html=True,
)
