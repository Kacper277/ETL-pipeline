"""Streamlit dashboard for the weather ETL pipeline.

Run locally with:
    streamlit run src/dashboard/app.py
"""

import logging
import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Weather ETL Dashboard", page_icon="🌦️", layout="wide")


@st.cache_resource
def get_engine():
    """Create a cached SQLAlchemy engine from the environment.

    Returns:
        A SQLAlchemy engine built from ``DATABASE_URL``.

    Raises:
        RuntimeError: If ``DATABASE_URL`` is not configured - no
            credentials are ever hardcoded as fallbacks.
    """
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env and fill "
            "in the PostgreSQL section (see .env.example)."
        )
    return create_engine(url)


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Load all records from the weather_daily table."""
    query = (
        "SELECT date, temp_max_c, temp_min_c, temp_range, rain_mm, loaded_at "
        "FROM weather_daily ORDER BY date"
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=300)
def load_readings() -> pd.DataFrame:
    """Load recent station readings from the weather_readings table.

    Returns:
        DataFrame of streamed readings, newest first.
    """
    query = (
        "SELECT station_id, observed_at, temp_max_c, temp_min_c, rain_mm, ingested_at "
        "FROM weather_readings ORDER BY observed_at DESC"
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    df["observed_at"] = pd.to_datetime(df["observed_at"])
    return df


def main() -> None:
    """Render the dashboard UI."""
    st.title("🌦️ Weather ETL Dashboard")
    st.caption("Data sourced from the Open-Meteo API via the ETL pipeline")

    # --- Streaming readings (weather_readings) ---------------------------
    st.subheader("📡 Streaming readings (Kafka path)")
    try:
        readings = load_readings()
        if readings.empty:
            st.info("No streamed readings yet. Run the streaming path: "
                    "`python -m src.streaming.produce_demo` + `python -m src.streaming.consume`.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Readings", len(readings))
            c2.metric("Stations", readings["station_id"].nunique())
            c3.metric("Latest", readings["observed_at"].max().strftime("%Y-%m-%d %H:%M"))
            per_station = (
                readings.pivot_table(index="observed_at", columns="station_id",
                                     values="temp_max_c")
            )
            st.line_chart(per_station)
            st.dataframe(readings.head(100), use_container_width=True)
    except Exception as exc:  # noqa: BLE001 - the streaming table may not exist yet
        st.info(f"Streaming data unavailable: {exc}")

    # --- Batch data (weather_daily) ---------------------------------------
    st.subheader("🗓️ Batch data")

    try:
        df = load_data()
    except Exception as exc:  # noqa: BLE001 - show a friendly UI error
        st.error(f"Could not load data from the database: {exc}")
        st.info(
            "Make sure the database is running and the pipeline has loaded data: "
            "`docker compose up -d postgres` then `python -m src.pipeline`."
        )
        return

    if df.empty:
        st.warning("The weather_daily table is empty. Run `python -m src.pipeline` first.")
        return

    # --- Sidebar filters -------------------------------------------------
    st.sidebar.header("Filters")
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    default_start = max(min_date, max_date - timedelta(days=30))

    start, end = st.sidebar.date_input(
        "Date range",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    show_forecast_only = st.sidebar.checkbox(
        "Future days only", value=False, help="Show only dates later than today"
    )
    st.sidebar.caption(f"Loaded {len(df)} rows · last load: {df['loaded_at'].max()}")

    mask = (df["date"].dt.date >= start) & (df["date"].dt.date <= end)
    if show_forecast_only:
        mask &= df["date"].dt.date > date.today()
    filtered = df.loc[mask]

    if filtered.empty:
        st.warning("No records in the selected range.")
        return

    # --- KPI metrics ------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg max temp", f"{filtered['temp_max_c'].mean():.1f} °C")
    col2.metric("Avg min temp", f"{filtered['temp_min_c'].mean():.1f} °C")
    col3.metric("Total rain", f"{filtered['rain_mm'].sum():.1f} mm")
    col4.metric("Rainy days", f"{(filtered['rain_mm'] > 0).sum()} / {len(filtered)}")

    # --- Temperature chart -------------------------------------------------
    st.subheader("Temperature over time")
    temp_chart = filtered.set_index("date")[["temp_max_c", "temp_min_c"]]
    st.line_chart(temp_chart)

    # --- Rain chart ---------------------------------------------------------
    st.subheader("Daily rainfall")
    st.bar_chart(filtered.set_index("date")["rain_mm"])

    # --- Weekly aggregation -------------------------------------------------
    st.subheader("Weekly averages")
    weekly = (
        filtered.set_index("date")[["temp_max_c", "temp_min_c"]]
        .resample("W")
        .mean()
        .round(2)
    )
    st.line_chart(weekly)

    # --- Data table ---------------------------------------------------------
    st.subheader("Raw records")
    st.dataframe(
        filtered.drop(columns="loaded_at").style.format(
            {"temp_max_c": "{:.1f}", "temp_min_c": "{:.1f}",
             "temp_range": "{:.1f}", "rain_mm": "{:.1f}"}
        ),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
