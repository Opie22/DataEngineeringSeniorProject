# Technical Decisions

Key architectural choices made across this project, with context, alternatives considered, and rationale.

---

## Decision 1: Watermark-Based Incremental Extraction vs. Full Table Scans

**Context:** The ETL processor extracts from PostgreSQL and MongoDB on a recurring schedule. We needed to decide whether to re-extract all records every cycle or only new/changed records.

**Decision:** Watermark-based incremental extraction using a `last_modified` timestamp filter, with watermark state persisted in a Snowflake metadata table.

**Alternatives considered:**
- **Full table scan each cycle:** Simpler (just `SELECT *`), no state to manage. Rejected because extraction time grows linearly with table size and wastes compute on unchanged records — a problem that compounds as data accumulates.
- **Change Data Capture (CDC) via PostgreSQL logical replication:** Most efficient for high-volume, high-frequency changes. Rejected because it requires PostgreSQL superuser access and additional infrastructure (Debezium, Kafka) beyond the project's scope.

**Trade-offs:**
- **Pros:** Efficient (processes only new data), simple to implement (single `WHERE last_modified > ?` clause), metadata table enables auditability and replay
- **Cons:** Requires a reliable `last_modified` column on every source table; watermark state must be persisted and is a failure point; late-arriving records with backdated timestamps would be missed

**Rationale:** Watermark extraction balances efficiency with simplicity. Source tables already had `last_modified` columns, and the metadata table pattern is straightforward to implement and debug. For this project's data volumes, this is more than sufficient. In a production system with millions of rows per cycle or unreliable source timestamps, CDC would be the right call.

---

## Decision 2: Prefect 2.x vs. Apache Airflow for Orchestration

**Context:** The web analytics pipeline (Milestone 2) required scheduled, observable orchestration with task-level retries and structured logging.

**Decision:** Prefect 2.x with a single flow containing four tasks (extract, validate, deduplicate, load).

**Alternatives considered:**
- **Apache Airflow:** Industry standard with a rich ecosystem of providers and a mature UI. Rejected because Airflow's scheduler, webserver, and worker require significantly more memory and configuration overhead. For a single scheduled flow, Airflow's complexity is not justified — it would spend more engineering time on infrastructure than on the pipeline itself.
- **Dagster:** Strong asset-based model with excellent data quality integration. Rejected because Dagster's learning curve and different abstraction model (assets vs. tasks) would slow development relative to the project timeline.

**Trade-offs:**
- **Pros of Prefect:** Minimal local setup (`prefect server start` is a single command), native async support, task-level retries with exponential backoff configured per-task, and a clean Python API that doesn't require XML or YAML DAG definitions
- **Cons of Prefect:** Smaller community than Airflow, fewer provider integrations, and less enterprise adoption — switching to Airflow would be required if deploying to a company running the Airflow standard

**Rationale:** For a single scheduled flow in a Docker Compose environment, Prefect's simplicity and native Python idioms outweigh Airflow's ecosystem advantages. A production migration to Airflow or Dagster would be straightforward since the task logic is decoupled from the orchestrator.

---

## Decision 3: Snowflake PUT + COPY INTO vs. Direct Insert for Loading

**Context:** Both the ETL processor and the Prefect flow needed to load data into Snowflake. We chose between direct row-by-row inserts, bulk `INSERT` statements, and Snowflake's staged load pattern.

**Decision:** Write extracted data to local CSV/JSON files, PUT them to a Snowflake internal stage, then execute `COPY INTO` from the stage.

**Alternatives considered:**
- **Row-by-row inserts via Snowflake connector:** Simple to implement. Rejected because each INSERT is a separate transaction and network round-trip; this approach becomes orders of magnitude slower at any meaningful scale.
- **Bulk INSERT with executemany:** Better than row-by-row but still subject to Snowflake's row-level transaction overhead and query size limits.
- **Snowpipe (continuous ingestion):** Event-driven, near-real-time loading. Rejected because it requires SQS or Azure Event Grid for file event notification, adding infrastructure complexity, and the project's 15-minute poll interval does not justify near-real-time loading latency.

**Trade-offs:**
- **Pros of PUT + COPY INTO:** Designed for bulk throughput (Snowflake's native pattern), atomic per-file load, automatic deduplication via `PURGE`, and easy replay from stage if a load fails
- **Cons:** Adds a local file write step; requires managing internal stage cleanup; two-step process (PUT then COPY) vs. one-step direct insert

**Rationale:** PUT + COPY INTO is the standard Snowflake bulk loading pattern. Using it from the start means the pipeline scales gracefully — the same code that handles 1K rows handles 10M rows without architectural changes.

---

## Decision 4: LEFT JOIN for Web Analytics Customer Enrichment

**Context:** The `int_web_analytics_with_customers` intermediate model joins clickstream events to the customer dimension. Some clickstream events are generated by anonymous users or users not yet in the customer table.

**Decision:** `LEFT JOIN` from web analytics events to the customer dimension, preserving all events even when no matching customer exists.

**Alternatives considered:**
- **INNER JOIN:** Simpler and only returns matched rows. Rejected because it silently drops anonymous or unmatched events, understating traffic volume and skewing conversion rate calculations — a correctness issue, not a performance trade-off.
- **Filtering anonymous events before the join:** Pre-filter events where `customer_id IS NULL`. Rejected because the downstream analyst should decide what to do with anonymous events; the intermediate model should not make that decision.

**Trade-offs:**
- **Pros:** Preserves full event history including anonymous sessions; downstream models can filter as needed; accurately represents total traffic
- **Cons:** NULLs in customer dimension columns for unmatched events require `COALESCE` or NULL handling in downstream queries

**Rationale:** Data loss at the transformation layer is harder to detect and recover from than NULL values in a column. The LEFT JOIN makes the join behavior explicit and preserves analyst optionality. The decision is documented in the model's YAML description so agents and analysts know to expect NULLs.

---

## Decision 5: dbt MCP Server in Docker vs. dbt Cloud Semantic Layer API

**Context:** Milestone 3 required exposing dbt models to AI agents. Two approaches were viable.

**Decision:** Run the open-source `dbt-mcp` package as a Docker service on port 8000, connected to the local dbt project.

**Alternatives considered:**
- **dbt Cloud Semantic Layer API:** Production-grade, managed, with built-in access controls and JDBC connectivity. Rejected because it requires a dbt Cloud Team plan ($100+/month) and is oriented toward BI tool connectivity rather than programmatic agent access via MCP.
- **Custom REST API wrapping dbt CLI:** Build a FastAPI service that shells out to `dbt compile`, `dbt ls`, etc. Rejected because this would duplicate functionality that `dbt-mcp` already provides, and maintaining a custom wrapper creates ongoing work as dbt evolves.

**Trade-offs:**
- **Pros of dbt-mcp in Docker:** Zero additional cost, exposes full dbt CLI surface (compile, list, lineage, docs), integrates with any MCP-compatible client, runs alongside other services in Docker Compose
- **Cons:** `dbt-mcp` is an early-stage package with API changes between versions (encountered breaking changes between 0.1.x releases); not suitable for production multi-tenant access without additional authentication middleware

**Rationale:** For the purpose of demonstrating agent-accessible data infrastructure, `dbt-mcp` in Docker is the right tool — it provides real MCP protocol support with minimal configuration. The production path to dbt Cloud Semantic Layer is clear and documented in Future Improvements.
