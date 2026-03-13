# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Single-file Streamlit dashboard (`dashboard_app.py`) displaying key Penang (Pulau Pinang) statistics. Data is sourced live from the Malaysian government's OpenDOSM API (`api.data.gov.my`) and a Penang Institute Google Sheet, mirroring datasets tracked by the Penang Institute.

## Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the dashboard
streamlit run dashboard_app.py

# Install dependencies
pip install -r requirements.txt
```

## Architecture

The entire application lives in `dashboard_app.py` (~960 lines), structured as sequential sections:

1. **Page config & CSS** — Dark theme styling applied via `st.markdown(unsafe_allow_html=True)`
2. **Data fetching** — Four `@st.cache_data(ttl=3600)` functions:
   - `fetch_api()` — Generic DOSM catalogue API fetcher, filtered for Pulau Pinang via `ifilter`
   - `fetch_population_parquet()` — Downloads parquet from DOSM storage
   - `fetch_trade_data()` — CSV from Penang Institute's published Google Sheet
   - `fetch_all_states_api()` — Same as `fetch_api()` but without state filtering
3. **Chart theming** — `CHART_LAYOUT` dict and `COLORS` palette applied via `apply_layout()`
4. **KPI cards** — Top row of 5 metrics (population, GDP, CPI, unemployment, median income)
5. **Three tabs**: Economy (GDP, CPI, labour, trade), Social (population, income), Demographics (births, deaths, fertility, age pyramid)

## Key Data Patterns

- All API calls go through `fetch_api()` with dataset IDs like `gdp_state_real_supply`, `cpi_state_inflation`, etc.
- API base: `https://api.data.gov.my/data-catalogue/`
- State filtering: `ifilter=pulau pinang@state` parameter
- All data is cached for 1 hour (`ttl=3600`)
- Charts use Plotly (`plotly_dark` template) with transparent backgrounds

## Dependencies

Python 3.13 with: `streamlit`, `plotly`, `pandas`, `requests`, `pyarrow` (for parquet support).
