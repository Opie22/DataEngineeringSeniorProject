# Product Requirements Document: Web Analytics Clickstream Ingestion

> This PRD is the specification for the AI agent. Written before building.

---

## 1. Problem Statement

Adventure Works has sales, customer, and chat data in Snowflake but no visibility into browsing behavior before purchase. Clickstream data (page views, clicks, add-to-cart events) exists in a REST API but is not integrated with the warehouse, making it impossible to connect browsing patterns to purchasing behavior. This pipeline closes that gap.

---

## 2. Desired Outcome

A Prefect 2.0 flow that runs on a configurable schedule, pulls web analytics events from the Adventure Works API, cleans and validates them, loads them into Snowflake's `RAW_EXT.web_analytics_raw` table via internal stage + COPY INTO, and logs summary statistics. The flow runs inside Docker Compose alongside the existing Milestone 1 pipeline.

---

## 3. Acceptance Criteria

- [ ] Flow successfully connects to `{API_BASE_URL}/analytics/clickstream` and retrieves events
- [ ] HTTP errors (4xx, 5xx), rate limits (429), and timeouts are retried with exponential backoff
- [ ] Data is type-cast correctly (event_timestamp as TIMESTAMP, IDs as INT, strings as VARCHAR)
- [ ] Null records on required fields (customer_id, product_id, session_id, event_timestamp) are dropped with a logged warning
- [ ] Duplicate events (same customer_id + session_id + event_timestamp) are deduplicated before load
- [ ] Data lands in `RAW_EXT.web_analytics_raw` with correct column mapping
- [ ] Staged files are cleaned up after successful COPY INTO
- [ ] Flow logs: records fetched, records after cleaning, records loaded, execution time
- [ ] Flow runs end-to-end in Docker without manual intervention
- [ ] Schedule interval is configurable via `FLOW_SCHEDULE_MINUTES` environment variable

---

## 4. Technical Constraints

- **Orchestration framework:** Prefect 2.0 (prefect>=2.14,<3.0)
- **Target warehouse:** Snowflake — credentials via env vars (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE)
- **Loading pattern:** PUT to internal stage → COPY INTO raw table → REMOVE stage files
- **Containerization:** Runs in `web-analytics-flow` Docker Compose service (already defined in compose.yml)
- **Prefect server:** Connects to `prefect-server` container at `PREFECT_API_URL=http://prefect-server:4200/api`
- **Project location:** All new code goes in `prefect/flows/web_analytics_flow.py`
- **Dependencies:** `prefect`, `snowflake-connector-python`, `requests`, `pandas`, `python-dotenv` (already in `prefect/pyproject.toml`)
- **Environment:** Variables loaded from `.env` via `python-dotenv`; Docker Compose injects them via `env_file`
- **Error handling:** No silent failures — all exceptions must be logged before re-raise

---

## 5. Data Schema

### API Response Schema

Endpoint: `GET /analytics/clickstream`

| Field | Type | Description | Nullable? |
|-------|------|-------------|-----------|
| customer_id | int | Customer identifier (links to stg_adventure_db__customers) | No |
| product_id | int | Product viewed/clicked | No |
| session_id | string | Browser session identifier | No |
| page_url | string | URL of the page (e.g. `https://adventure-works.com/product/42`) | Yes |
| event_type | string | One of: `page_view`, `click`, `add_to_cart`, `purchase` | Yes |
| timestamp | string (ISO 8601 UTC) | When the event occurred, e.g. `2025-01-15T14:32:00Z` | No |

### Target Table Schema (Snowflake: `RAW_EXT.web_analytics_raw`)

| Column | Type | Source |
|--------|------|--------|
| customer_id | INT NOT NULL | API `customer_id` |
| product_id | INT NOT NULL | API `product_id` |
| session_id | VARCHAR(255) NOT NULL | API `session_id` |
| page_url | VARCHAR(1000) | API `page_url` |
| event_type | VARCHAR(50) | API `event_type` |
| event_timestamp | TIMESTAMP_NTZ NOT NULL | API `timestamp` (parsed to UTC) |
| _loaded_at | TIMESTAMP_NTZ | Set by Snowflake DEFAULT CURRENT_TIMESTAMP() |
| _file_name | VARCHAR(255) | Set by COPY INTO METADATA$FILENAME |

---

## 6. Testing Requirements

- [ ] Syntax check: `uv run python -c "from flows.web_analytics_flow import web_analytics_flow; print('Import OK')"`
- [ ] Local run: `uv run python -m flows.web_analytics_flow` exits cleanly and loads rows
- [ ] Snowflake verify: `SELECT COUNT(*), MIN(event_timestamp), MAX(event_timestamp) FROM RAW_EXT.web_analytics_raw;` returns rows
- [ ] Docker run: flow container starts, connects to prefect-server, and runs on schedule
- [ ] Unit test: flow handles empty API response (returns `[]`) without crashing

---

## 7. Out of Scope

- dbt models for web analytics data (handled in Task 3)
- Modifications to existing Milestone 1 models or processor
- Dashboard or visualization of web analytics data
- Authentication/API keys (the API is public)
- Historical backfill (flow ingests current data only)

---

## 8. Questions and Assumptions

- **Assumption:** The API at `/analytics/clickstream` returns a flat JSON array of events (confirmed by `mock_api.py` in repo)
- **Assumption:** The API field `timestamp` maps to `event_timestamp` in the Snowflake table
- **Assumption:** No API authentication is required
- **Assumption:** The `since` query parameter allows incremental pulls — the flow should use this if available to avoid re-loading old events; otherwise load all and rely on Snowflake deduplication
- **Question resolved:** `_loaded_at` and `_file_name` are populated by Snowflake COPY INTO, not by Python — the CSV should only contain the 6 data columns
