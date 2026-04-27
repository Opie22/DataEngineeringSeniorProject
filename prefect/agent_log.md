# Agent Interaction Log: Web Analytics Clickstream Ingestion (Prefect Flow)

---

## 1. Setup

**Agent tool used:** Claude (Cowork / Claude desktop app)

**Why this tool?** Used to help complete Milestone 1 as well — familiar with the existing codebase, Snowflake patterns already established, and able to read files directly from the repo to understand context for questions.

**Date:** April 2026

**Total time spent:** ~45 minutes (PRD + flow build + iteration)

---

## 2. Initial Specification

Shared the completed PRD (`prefect/prd.md`) with the agent along with the following context:

```
I need to build the Prefect 2.0 flow at prefect/flows/web_analytics_flow.py.
The PRD is in prefect/prd.md. The Snowflake table DDL is in prefect/snowflake_objects.sql.
The Docker Compose services (prefect-server, prefect-worker, web-analytics-flow) are
already defined in compose.yml. Env vars come from .env via env_file. The schedule
interval is FLOW_SCHEDULE_MINUTES. Build the complete flow following the PRD.
```

**Did you share the PRD with the agent?** Yes — the agent read `prefect/prd.md` directly from the repo as well as `prefect/snowflake_objects.sql`, `prefect/mock_api.py`, and `compose.yml` to understand the full context before generating any code.

---

## 3. Iteration Log

### Iteration 1: Initial flow generation
- **What I asked:** Build the complete Prefect flow per the PRD
- **What the agent produced:** Full `web_analytics_flow.py` with `fetch_clickstream_events`, `clean_and_validate`, and `stage_and_load` tasks plus a `web_analytics_flow` flow using `flow.serve()`
- **What worked:** Overall structure was correct — Prefect 2.0 task/flow decorators, retry logic on fetch, type casting, deduplication, and the PUT → COPY INTO → REMOVE pattern all matched the PRD
- **What didn't work:** The agent initially used `FETCH_INTERVAL_SEC` as the schedule env var name, but the `.env` file uses `FLOW_SCHEDULE_MINUTES`
- **What I changed:** Corrected the env var name to `FLOW_SCHEDULE_MINUTES` before saving the file

### Iteration 2: COPY INTO column mapping
- **What I asked:** Verified that `_loaded_at` and `_file_name` would be handled correctly since the CSV only has 6 columns but the table has 8
- **What the agent produced:** Explicit column list in COPY INTO + `METADATA$FILENAME` expression for `_file_name`; `_loaded_at` uses its `DEFAULT CURRENT_TIMESTAMP()` automatically
- **What worked:** This is the correct Snowflake pattern — same approach used in Milestone 1
- **What didn't work:** Nothing — pattern was correct on first pass
- **What I changed:** No changes needed

### Iteration 3: Python version incompatibility
- **What I asked:** Run syntax check — `uv run python -c "from flows.web_analytics_flow import web_analytics_flow; print('Import OK')"`
- **What the agent produced:** The import failed with a Pydantic v1 error deep inside Prefect's internals
- **What worked:** The error message was clear — Prefect 2.x uses Pydantic v1 compatibility shims that broke on Python 3.14
- **What didn't work:** `uv` auto-selected CPython 3.14.2 (the latest installed), which Prefect 2.x does not support
- **What I changed:** Updated `pyproject.toml` to `requires-python = ">=3.11,<3.13"`, deleted `.venv`, and reran — uv selected Python 3.11.14 and the import succeeded

### Iteration 4: End-to-end local run
- **What I asked:** Run the flow directly against the live API: `uv run python -c "from flows.web_analytics_flow import web_analytics_flow; web_analytics_flow()"`
- **What the agent produced:** Successful run — 50 events fetched, 50 validated, 50 loaded into `RAW_EXT.web_analytics_raw`
- **What worked:** PUT → COPY INTO → REMOVE pipeline worked correctly; `_loaded_at` and `_file_name` populated by Snowflake as expected
- **What didn't work:** N/A
- **What I changed:** N/A

### Iteration 5: Refactor to 4 explicit tasks
- **What I asked:** Refactor the flow to match the professor's structure — 4 separate tasks instead of 3
- **What the agent produced:** Split `clean_and_validate` into two distinct tasks: `validate_events` (type casting, null drops, unexpected value warnings) and `deduplicate_events` (natural key dedup on customer_id + session_id + event_timestamp); renamed `fetch_clickstream_events` → `extract_events` and `stage_and_load` → `load_to_snowflake`
- **What worked:** Cleaner separation of concerns; each task now has a single responsibility; final log line reports all 4 stages separately
- **What didn't work:** N/A
- **What I changed:** N/A

---

## 4. Final Result

**Did the agent-generated code work on first run?** Mostly — required two fixes: env var name and Python version correction

**If no, what broke?**
1. Wrong env var name (`FETCH_INTERVAL_SEC` → `FLOW_SCHEDULE_MINUTES`)
2. Python 3.14 incompatibility with Prefect 2.x — fixed by pinning `requires-python = ">=3.11,<3.13"` in pyproject.toml

**Percentage of final code written by the agent vs. you:**
- Agent wrote: ~95%
- I wrote/modified: ~5% (env var name, pyproject.toml version)

**Key files the agent created or modified:**
- `prefect/flows/web_analytics_flow.py`: Complete Prefect 2.0 flow with 4 tasks — extract, validate, deduplicate, load
- `prefect/pyproject.toml`: Added Python version upper bound `<3.13`
- `prefect/prd.md`: PRD written with agent assistance before any code was generated
- `prefect/agent_log.md`: This file

---

## 5. What I Learned

### What the agent was good at:
- Reading multiple existing files (compose.yml, snowflake_objects.sql, mock_api.py) to understand context before generating code
- Getting Snowflake-specific syntax right: PUT, COPY INTO with transformation + METADATA$FILENAME, REMOVE
- Prefect 2.0 task/flow decorator patterns and retry configuration
- Following the PRD acceptance criteria systematically (retries, null handling, deduplication, logging)

### What the agent struggled with:
- Did not automatically check `.env` to verify the correct env var names — assumed a name that didn't match
- Did not account for Python version compatibility — Prefect 2.x + Pydantic v1 breaks on Python 3.14, which required pinning `requires-python` in pyproject.toml
- Initial task structure (3 tasks) needed adjustment to match the expected 4-task separation of concerns

### What I would do differently next time:
- Share the `.env` file with the agent upfront so it can match env var names exactly
- Specify the desired number and names of tasks explicitly in the PRD rather than leaving structure to the agent
- Mention the Python version constraint upfront for any project using Prefect 2.x

### Time comparison estimate:
- **With agent:** ~1 hour (including PRD, two iterations on structure, Python version debugging)
- **Without agent (estimate):** ~4-5 hours (Prefect API docs, Snowflake connector docs, PUT/COPY INTO syntax, debugging)
- **Net impact:** Significantly faster — roughly 4-5× speedup, with most time spent reviewing and correcting rather than writing from scratch

---

## 6. Reflection

Using an AI agent for this task was effective because of the PRD and existing context from Milestone 1. The agent's ability to read multiple files in the repo meant it understood the environment before writing any code. The most important lesson was that vague prompts produce vague code, and the PRD helped avoid many potential issues. The one failure (wrong env var name) would have been prevented by sharing the `.env` file upfront. Going forward, I would always include environment configuration files in the agent's initial context for any kind of infrastructure task.
