"""
Web Analytics Prefect Flow

Pulls clickstream data from the Adventure Works Web Analytics API,
cleans and validates it, loads it into Snowflake via internal stage + COPY INTO.

Tasks:
    1. extract_events        — Fetch raw events from the REST API
    2. validate_events       — Cast types, drop nulls on required fields
    3. deduplicate_events    — Remove duplicate records on natural key
    4. load_to_snowflake     — PUT to internal stage, COPY INTO, REMOVE staged files

Environment variables (set in .env / Docker Compose env_file):
    API_BASE_URL            — Base URL for the web analytics API
    FLOW_SCHEDULE_MINUTES   — How often the flow runs (default: 15)
    SNOWFLAKE_ACCOUNT       — Snowflake account identifier
    SNOWFLAKE_USER          — Snowflake username
    SNOWFLAKE_PASSWORD      — Snowflake password
    SNOWFLAKE_DATABASE      — Target database
    SNOWFLAKE_SCHEMA        — Target schema (default: RAW_EXT)
    SNOWFLAKE_WAREHOUSE     — Compute warehouse
    SNOWFLAKE_ROLE          — Snowflake role
"""

import os
import time
import tempfile
from datetime import timedelta

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
API_BASE_URL          = os.getenv("API_BASE_URL", "https://is566-web-analytics-api.fly.dev")
FLOW_SCHEDULE_MINUTES = int(os.getenv("FLOW_SCHEDULE_MINUTES", "15"))
STAGE_NAME            = "RAW_EXT.WEB_ANALYTICS_STAGE"
TARGET_TABLE          = "RAW_EXT.web_analytics_raw"
REQUIRED_COLUMNS      = ["customer_id", "product_id", "session_id", "event_timestamp"]
VALID_EVENT_TYPES     = {"page_view", "click", "add_to_cart", "purchase"}


# ── Snowflake helper ──────────────────────────────────────────────────────────
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW_EXT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


# ── Task 1: Extract ───────────────────────────────────────────────────────────
@task(retries=3, retry_delay_seconds=[10, 30, 60], name="extract_events")
def extract_events() -> list[dict]:
    """Fetch raw clickstream events from the web analytics API.
    Retries up to 3 times with backoff on HTTP errors and rate limits (429).
    """
    logger = get_run_logger()
    url = f"{API_BASE_URL}/analytics/clickstream"
    logger.info(f"Fetching from {url}")

    response = requests.get(url, timeout=30)

    if response.status_code == 429:
        raise Exception("Rate limited (429) — Prefect will retry with backoff")
    response.raise_for_status()

    events = response.json()
    logger.info(f"Extracted {len(events)} raw events from API")
    return events


# ── Task 2: Validate ──────────────────────────────────────────────────────────
@task(name="validate_events")
def validate_events(events: list[dict]) -> pd.DataFrame:
    """Cast types and drop rows that fail validation on required fields.

    Validation rules:
    - customer_id and product_id must be numeric (coerced, nulls dropped)
    - session_id and event_timestamp must be non-null
    - event_type must be one of the four known values (logged but not dropped)
    - API field 'timestamp' is renamed to 'event_timestamp'
    """
    logger = get_run_logger()

    if not events:
        logger.warning("API returned empty event list — nothing to validate")
        return pd.DataFrame()

    df = pd.DataFrame(events)

    # API uses 'timestamp'; Snowflake table column is 'event_timestamp'
    if "timestamp" in df.columns and "event_timestamp" not in df.columns:
        df = df.rename(columns={"timestamp": "event_timestamp"})

    # Ensure all expected columns are present
    for col in ["customer_id", "product_id", "session_id", "page_url", "event_type", "event_timestamp"]:
        if col not in df.columns:
            df[col] = None

    # Drop rows missing required fields
    initial_count = len(df)
    df = df.dropna(subset=REQUIRED_COLUMNS)
    dropped_nulls = initial_count - len(df)
    if dropped_nulls:
        logger.warning(f"Dropped {dropped_nulls} rows with null required fields")

    # Cast numeric IDs — coerce invalid values to NaN, then drop
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["product_id"]  = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id", "product_id"])

    # Cast remaining columns
    df["session_id"]      = df["session_id"].astype(str)
    df["page_url"]        = df["page_url"].where(df["page_url"].notna(), None)
    df["event_type"]      = df["event_type"].where(df["event_type"].notna(), None)
    df["event_timestamp"] = (
        pd.to_datetime(df["event_timestamp"], utc=True)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    # Flag unexpected event_type values (warn but keep — dbt tests will catch these)
    invalid_types = df[~df["event_type"].isin(VALID_EVENT_TYPES)]["event_type"].unique()
    if len(invalid_types) > 0:
        logger.warning(f"Unexpected event_type values: {list(invalid_types)}")

    # Keep only the 6 columns the CSV will contain
    df = df[["customer_id", "product_id", "session_id", "page_url", "event_type", "event_timestamp"]]

    logger.info(f"Validation complete: {len(df)} rows passed")
    return df


# ── Task 3: Deduplicate ───────────────────────────────────────────────────────
@task(name="deduplicate_events")
def deduplicate_events(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate events on natural key: customer_id + session_id + event_timestamp."""
    logger = get_run_logger()

    if df.empty:
        logger.warning("Empty DataFrame — nothing to deduplicate")
        return df

    before = len(df)
    df = df.drop_duplicates(subset=["customer_id", "session_id", "event_timestamp"])
    removed = before - len(df)

    if removed:
        logger.info(f"Removed {removed} duplicate rows")
    else:
        logger.info("No duplicates found")

    logger.info(f"Deduplication complete: {len(df)} rows ready to load")
    return df


# ── Task 4: Load ──────────────────────────────────────────────────────────────
@task(name="load_to_snowflake")
def load_to_snowflake(df: pd.DataFrame) -> dict:
    """Write CSV → PUT to Snowflake internal stage → COPY INTO raw table → REMOVE staged files."""
    logger = get_run_logger()

    if df.empty:
        logger.warning("No data to load — skipping Snowflake stage/load")
        return {"rows_loaded": 0, "status": "skipped"}

    tmp_path = None
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Write cleaned data to a local temp CSV
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", prefix="web_analytics_", delete=False
        ) as tmp:
            tmp_path = tmp.name
            df.to_csv(tmp_path, index=False)
        logger.info(f"Wrote {len(df)} rows to temp file {tmp_path}")

        # PUT the file into the Snowflake internal stage
        cursor.execute(
            f"PUT file://{tmp_path} @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        put_result = cursor.fetchall()
        logger.info(f"PUT result: {put_result}")

        # COPY INTO with explicit column list + METADATA$FILENAME for _file_name
        # _loaded_at uses its DEFAULT CURRENT_TIMESTAMP() automatically
        cursor.execute(f"""
            COPY INTO {TARGET_TABLE}
                (customer_id, product_id, session_id, page_url, event_type, event_timestamp, _file_name)
            FROM (
                SELECT $1, $2, $3, $4, $5, $6::TIMESTAMP_NTZ, METADATA$FILENAME
                FROM @{STAGE_NAME}
            )
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL', 'None', 'NaN')
            )
            ON_ERROR = CONTINUE
        """)
        copy_result = cursor.fetchall()
        rows_loaded = sum(r[3] for r in copy_result) if copy_result else 0
        logger.info(f"COPY INTO complete — {rows_loaded} rows loaded")

        # Clean up staged files
        cursor.execute(f"REMOVE @{STAGE_NAME}/")
        removed = cursor.fetchall()
        logger.info(f"Removed {len(removed)} staged file(s) from {STAGE_NAME}")

        return {"rows_loaded": rows_loaded, "status": "success"}

    except Exception as e:
        logger.error(f"Snowflake stage/load error: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


# ── Flow ──────────────────────────────────────────────────────────────────────
@flow(name="web-analytics-flow", log_prints=True)
def web_analytics_flow():
    """End-to-end web analytics ingestion: REST API → Snowflake RAW_EXT.

    Pipeline:
        extract_events → validate_events → deduplicate_events → load_to_snowflake
    """
    logger = get_run_logger()
    start = time.time()

    raw_events  = extract_events()
    validated   = validate_events(raw_events)
    deduped     = deduplicate_events(validated)
    result      = load_to_snowflake(deduped)

    elapsed = round(time.time() - start, 1)
    logger.info(
        f"Flow complete | extracted={len(raw_events)} | validated={len(validated)} | "
        f"deduped={len(deduped)} | loaded={result['rows_loaded']} | elapsed={elapsed}s"
    )


if __name__ == "__main__":
    interval = timedelta(minutes=FLOW_SCHEDULE_MINUTES)
    print(f"Starting web-analytics-flow on a {FLOW_SCHEDULE_MINUTES}-minute interval...")
    web_analytics_flow.serve(
        name="web-analytics-scheduled",
        interval=interval,
    )
