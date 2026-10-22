"""Airflow orchestration for the BATCH weather ETL path.

Orchestrates the existing ``src/`` batch modules (extract -> transform
-> validate -> load). The DAG contains NO business logic: every task is
a thin callable that resolves parameters, delegates to a ``src/`` class
and passes compact artifacts (file paths, counts, a small stats dict)
through XCom.

The Kafka streaming path is deliberately NOT orchestrated here: it is
a long-running consumer process, not a scheduled task.

Requires the ``weather-db`` Airflow connection (provided via the
``AIRFLOW_CONN_WEATHER_DB`` environment variable by docker-compose) and
a writable directory in ``AIRFLOW_ARTIFACTS_DIR`` for the file handoff
between tasks.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any
from urllib.parse import quote_plus

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DAG_ID = "weather_batch_etl_pipeline"

# Documented defaults - identical to the batch CLI defaults (.env.example).
DEFAULT_BASE_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_LATITUDE = "52.23"
DEFAULT_LONGITUDE = "21.01"
DEFAULT_FORECAST_DAYS = "16"
DEFAULT_PAST_DAYS = "92"
DEFAULT_ARTIFACTS_DIR = "/opt/airflow/artifacts"

RAW_FILENAME = "weather_raw.json"
PROCESSED_FILENAME = "weather_processed.csv"


def _artifacts_dir() -> str:
    """Return the writable directory for inter-task file handoff.

    Returns:
        Value of ``AIRFLOW_ARTIFACTS_DIR`` or the documented default.
    """
    return os.getenv("AIRFLOW_ARTIFACTS_DIR", DEFAULT_ARTIFACTS_DIR)


def _database_url() -> str:
    """Resolve the weather database URL.

    Primary source: the Airflow connection ``weather_db``, provided by
    docker-compose via ``AIRFLOW_CONN_WEATHER_DB`` so it survives clean
    rebuilds. Credentials are URL-quoted before the SQLAlchemy URL is
    assembled.

    Returns:
        A ``postgresql+psycopg2://`` connection string.

    Raises:
        AirflowNotFoundException: If the connection is not configured.
    """
    try:
        conn = BaseHook.get_connection("weather_db")
    except AirflowNotFoundException as exc:
        raise AirflowNotFoundException(
            "Airflow connection 'weather_db' is not configured. "
            "Set AIRFLOW_CONN_WEATHER_DB (see .env.example)."
        ) from exc
    password = quote_plus(conn.password or "")
    login = quote_plus(conn.login or "")
    return (
        f"postgresql+psycopg2://{login}:{password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


def extract_task(**context: Any) -> dict[str, Any]:
    """Fetch the daily weather window from Open-Meteo and store raw JSON.

    HTTP 5xx, timeouts and connection errors are raised so the task
    retries (default_args); client errors (4xx) and malformed payloads
    fail immediately without retrying.

    Args:
        **context: Airflow task context (unused beyond XCom).

    Returns:
        Compact XCom dict: ``raw_path`` (file for downstream tasks)
        and ``records`` (number of daily records fetched).

    Raises:
        AirflowFailException: On non-retryable failures (4xx, bad payload).
    """
    from src.extract.fetch_data import DataExtractor, ExtractionError
    from src.extract.save_raw import save_raw_json
    from requests import exceptions as requests_exceptions

    base_url = os.getenv("OPEN_METEO_BASE_URL", DEFAULT_BASE_URL)
    lat = float(os.getenv("LATITUDE", DEFAULT_LATITUDE))
    lon = float(os.getenv("LONGITUDE", DEFAULT_LONGITUDE))
    days = int(os.getenv("FORECAST_DAYS", DEFAULT_FORECAST_DAYS))
    past_days = int(os.getenv("PAST_DAYS", DEFAULT_PAST_DAYS))

    extractor = DataExtractor(base_url)
    try:
        raw = extractor.fetch_forecast(lat, lon, days, past_days)
    except ExtractionError as exc:
        cause = exc.__cause__
        retryable = isinstance(
            cause,
            (requests_exceptions.Timeout, requests_exceptions.ConnectionError),
        ) or (
            isinstance(cause, requests_exceptions.HTTPError)
            and cause.response is not None
            and cause.response.status_code >= 500
        )
        if retryable:
            logger.warning("Retryable extraction failure: %s", exc)
            raise
        raise AirflowFailException(f"Non-retryable extraction failure: {exc}") from exc

    raw_path = os.path.join(_artifacts_dir(), RAW_FILENAME)
    save_raw_json(raw, raw_path)
    records = len(raw.get("daily", {}).get("time", []))
    logger.info("Fetched %d daily records -> %s", records, raw_path)
    return {"raw_path": raw_path, "records": records}


def transform_task(**context: Any) -> dict[str, Any]:
    """Clean and enrich the raw payload, export a processed CSV.

    Reads the raw JSON by path (never a DataFrame through XCom), runs
    the existing pandas pipeline and persists the result to the
    artifacts volume.

    Args:
        **context: Airflow task context; pulls ``raw_path`` from XCom.

    Returns:
        Compact XCom dict: ``processed_path``, ``rows`` and ``stats``
        (five rounded floats from the batch statistics module).

    Raises:
        AirflowFailException: If upstream returned no raw file.
    """
    from src.transform.process_data import DataTransformer

    raw_path = (context["ti"].xcom_pull(task_ids="extract") or {}).get("raw_path")
    if not raw_path:
        raise AirflowFailException("No raw_path received from the extract task")

    transformer = DataTransformer(raw_path)
    df = transformer.parse_daily_metrics()
    df = transformer.add_derived_columns(df)
    processed_path = os.path.join(_artifacts_dir(), PROCESSED_FILENAME)
    transformer.export_to_csv(df, processed_path)
    stats = transformer.calculate_statistics(df)
    logger.info("Transformed %d rows -> %s (stats: %s)", len(df), processed_path, stats)
    return {"processed_path": processed_path, "rows": len(df), "stats": stats}


def validate_task(**context: Any) -> dict[str, Any]:
    """Run the data-quality gate over the processed CSV.

    Policy (orchestration glue, implemented with the existing
    ``DataValidator`` methods):

    - **Physically impossible values** (outside temperature/rain
      ranges) fail the task: corrupted data must not reach the
      database.
    - **Nulls** are logged as warnings and reported via XCom, but do
      not block: Open-Meteo legitimately returns null metrics at the
      edge of the 92-day past window, and the batch path treats missing
      values as reportable rather than fatal.

    Args:
        **context: Airflow task context; pulls ``processed_path``.

    Returns:
        Compact XCom dict: ``passed`` (range verdict) and ``nulls``
        (column -> has-null mapping).

    Raises:
        AirflowFailException: If a range check fails.
    """
    import pandas as pd

    from src.transform.validate_data import DataValidator

    processed_path = (context["ti"].xcom_pull(task_ids="transform") or {}).get("processed_path")
    if not processed_path:
        raise AirflowFailException("No processed_path received from the transform task")

    df = pd.read_csv(processed_path)
    validator = DataValidator()
    ranges_ok = all(validator.check_value_ranges(df).values())
    nulls = validator.check_null_values(df)
    report = validator.full_validation_report(df)
    logger.info("Validation report:\n%s", report)
    if not ranges_ok:
        raise AirflowFailException("Range validation FAILED - see task logs for the report")
    if any(nulls.values()):
        logger.warning("Nulls present (non-blocking): %s",
                       {k: v for k, v in nulls.items() if v})
    return {"passed": ranges_ok, "nulls": nulls}


def load_task(**context: Any) -> dict[str, Any]:
    """Load the processed records into ``weather_daily`` (idempotent).

    Uses the existing ``DatabaseLoader`` whose UPSERT
    (``ON CONFLICT (date) DO NOTHING``) makes retried runs safe:
    re-loading the same dates never duplicates rows. The inserted count
    is cross-checked against the before/after table counts, so a driver
    that reports unreliable ``rowcount`` for bulk upserts cannot hide a
    discrepancy.

    Args:
        **context: Airflow task context; pulls ``processed_path``.

    Returns:
        Compact XCom dict: ``before``, ``after``, ``inserted``.

    Raises:
        AirflowFailException: If no processed file was received.
    """
    import pandas as pd

    from src.load.store_data import DatabaseLoader

    processed_path = (context["ti"].xcom_pull(task_ids="transform") or {}).get("processed_path")
    if not processed_path:
        raise AirflowFailException("No processed_path received from the transform task")

    df = pd.read_csv(processed_path)
    records = df.to_dict("records")

    loader = DatabaseLoader(_database_url())
    try:
        loader.create_table()
        before = loader.get_record_count()
        inserted = loader.insert_records(records)
        after = loader.get_record_count()
    finally:
        loader.close()

    delta = after - before
    if inserted != delta:
        # rowcount for bulk upserts can be driver-dependent; the table
        # delta is the source of truth for the summary.
        logger.warning(
            "Reported inserts (%d) differ from row delta (%d) - using delta",
            inserted, delta,
        )
        inserted = delta
    logger.info("Load complete: %d rows in table (%d new)", after, delta)
    return {"before": before, "after": after, "inserted": inserted}


def summary_task(**context: Any) -> dict[str, Any]:
    """Emit a compact run summary (counts only, no payloads).

    Pulls the small XComs of all upstream tasks, adds the DAG run
    duration and logs the result.

    Args:
        **context: Airflow task context (dag_run, ti).

    Returns:
        Summary dict: fetched/transformed/inserted counts and duration.
    """
    ti = context["ti"]
    extract_out = ti.xcom_pull(task_ids="extract") or {}
    transform_out = ti.xcom_pull(task_ids="transform") or {}
    load_out = ti.xcom_pull(task_ids="load") or {}

    duration_s = None
    if ti.start_date is not None:
        duration_s = round((pendulum.now("UTC") - ti.start_date).total_seconds(), 1)

    summary = {
        "records_fetched": extract_out.get("records"),
        "rows_transformed": transform_out.get("rows"),
        "avg_temp_max_c": (transform_out.get("stats") or {}).get("avg_temp_max"),
        "rows_before_load": load_out.get("before"),
        "rows_after_load": load_out.get("after"),
        "rows_inserted": load_out.get("inserted"),
        "duration_s": duration_s,
    }
    logger.info("Run summary: %s", summary)
    return summary


with DAG(
    dag_id=DAG_ID,
    description="Scheduled batch ETL: Open-Meteo API -> weather_daily (uses src/ modules as-is)",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 10, 20, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "batch", "etl"],
    doc_md=__doc__,
    default_args={
        "owner": "weather-data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
        "depends_on_past": False,
        "email": None,
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        doc_md="Fetch the weather window (forecast + recent past) from the "
               "Open-Meteo API via src.extract.DataExtractor and persist the "
               "raw JSON to the artifacts volume. Retries only on 5xx, "
               "timeout and connection errors.",
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        doc_md="Clean and enrich the raw JSON with src.transform.DataTransformer "
               "(daily metrics, temp_range, temp_category), export the processed "
               "CSV and pass its path onward.",
    )
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
        doc_md="Run the src.transform.DataValidator quality gate (nulls, "
               "temperature/rain ranges) over the processed CSV. Fails the run "
               "on a FAIL verdict.",
    )
    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        doc_md="Upsert the processed rows into weather_daily via "
               "src.load.DatabaseLoader (ON CONFLICT DO NOTHING -> idempotent "
               "retries). Row counts are cross-checked against the table delta.",
    )
    summary = PythonOperator(
        task_id="summary",
        python_callable=summary_task,
        doc_md="Log and XCom-push a compact run summary: fetched/transformed/"
               "inserted counts and run duration.",
    )

    extract >> transform >> validate >> load >> summary
