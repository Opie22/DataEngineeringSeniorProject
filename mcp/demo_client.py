#!/usr/bin/env python3
"""
dbt MCP Server Demo Client

This script connects to the dbt MCP server running in Docker Compose
and demonstrates how an AI agent can discover and interact with your
data models programmatically.

Usage:
    cd mcp
    uv sync
    uv run python demo_client.py

Requirements:
    - MCP server must be running: docker compose up -d dbt-mcp
    - uv sync run first to install dependencies
"""

import asyncio
import json
import sys
from datetime import datetime

from mcp.client.session import ClientSession
from mcp.client.sse import sse_client

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
MCP_SERVER_URL = "http://localhost:8000/sse"
OUTPUT_LOG = "demo_output.log"

# -------------------------------------------------------------------
# Helper: log to both console and file
# -------------------------------------------------------------------
log_lines = []


def log(message: str):
    """Print to console and capture for log file."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    log_lines.append(line)


def save_log():
    """Write all captured output to the log file."""
    with open(OUTPUT_LOG, "w") as f:
        f.write(f"dbt MCP Demo Output - {datetime.now().isoformat()}\n")
        f.write("=" * 60 + "\n\n")
        for line in log_lines:
            f.write(line + "\n")
    print(f"Output saved to {OUTPUT_LOG}")


def parse_result(result) -> str:
    """
    Safely extract text content from an MCP tool result.
    MCP returns a list of content blocks; we grab the first text block.
    """
    try:
        if result and result.content:
            for block in result.content:
                if hasattr(block, "text"):
                    return block.text
    except Exception:
        pass
    return str(result)


async def call_tool(session: ClientSession, tool_name: str, args: dict) -> str:
    """
    Call an MCP tool and return its text output.
    Returns an error string instead of raising so the demo continues
    past individual step failures.
    """
    try:
        result = await session.call_tool(tool_name, args)
        return parse_result(result)
    except Exception as e:
        return f"[ERROR calling {tool_name}: {e}]"


# -------------------------------------------------------------------
# Demo Steps
# -------------------------------------------------------------------

async def run_demo():

    log("=" * 60)
    log("dbt MCP Server Demo")
    log("=" * 60)
    log("")

    # ------------------------------------------------------------------
    # STEP 1: Connect to the MCP server
    # ------------------------------------------------------------------
    # sse_client opens an HTTP Server-Sent Events connection to the MCP
    # server at localhost:8000/sse (the Docker container we started in
    # Task 1). It returns (read, write) streams that ClientSession wraps
    # into the MCP protocol. session.initialize() performs the handshake
    # so the server registers this client connection.
    # ------------------------------------------------------------------
    log("Step 1: Connecting to dbt MCP server...")
    try:
        async with sse_client(MCP_SERVER_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                log(f"  Connected to: {MCP_SERVER_URL}")
                log("  Handshake complete.")

                # ------------------------------------------------------
                # STEP 2: List available tools
                # ------------------------------------------------------
                # session.list_tools() asks the server what operations it
                # exposes. This is important because tool names differ
                # between dbt-mcp versions. We print every tool name and
                # a short description so you know exactly what is
                # available before calling anything in Steps 3-6.
                # ------------------------------------------------------
                log("")
                log("Step 2: Listing available tools...")
                try:
                    tools_response = await session.list_tools()
                    tool_names = [t.name for t in tools_response.tools]
                    for tool in tools_response.tools:
                        desc = (tool.description or "")[:120]
                        log(f"  - {tool.name}: {desc}")
                    log(f"  Total tools available: {len(tool_names)}")
                except Exception as e:
                    log(f"  [ERROR listing tools: {e}]")
                    tool_names = []

                # ------------------------------------------------------
                # STEP 3: Discover all dbt models
                # ------------------------------------------------------
                # The "list" tool reads from target/manifest.json (built
                # by dbt compile in the container's entrypoint) and
                # returns every model dbt knows about, including the
                # descriptions we upgraded in Task 2. We print each
                # model name so you can see the full project inventory.
                # ------------------------------------------------------
                log("")
                log("Step 3: Discovering dbt models...")
                raw = await call_tool(
                    session, "list", {"resource_type": ["model"]}
                )
                if raw.startswith("[ERROR"):
                    log(f"  {raw}")
                else:
                    try:
                        models = json.loads(raw)
                        if isinstance(models, list):
                            log(f"  Found {len(models)} models:")
                            for m in models:
                                name = m.get("name", m) if isinstance(m, dict) else str(m)
                                desc = (m.get("description", "") or "")[:80] if isinstance(m, dict) else ""
                                log(f"    - {name}: {desc}")
                        else:
                            log(f"  Result: {str(models)[:800]}")
                    except (json.JSONDecodeError, TypeError):
                        # Some versions return plain text
                        for line in raw[:1500].splitlines():
                            log(f"  {line}")

                # ------------------------------------------------------
                # STEP 4: Get details on a specific model
                # ------------------------------------------------------
                # We request the full node details of stg_web_analytics,
                # one of our Milestone 2 models. The server returns its
                # column definitions, data tests, and dependencies. This
                # is what an AI agent calls before writing a query — it
                # needs to know what columns exist, their types, and
                # which models to join. We try get_node_details_dev
                # first (0.2.x name) then get_node_details (newer name).
                # ------------------------------------------------------
                log("")
                log("Step 4: Getting details for stg_web_analytics...")
                raw = await call_tool(
                    session,
                    "get_node_details_dev",
                    {"node_id": "stg_web_analytics"},
                )
                if raw.startswith("[ERROR"):
                    log("  get_node_details_dev not available, trying get_node_details...")
                    raw = await call_tool(
                        session,
                        "get_node_details",
                        {"node_id": "stg_web_analytics"},
                    )
                for line in raw[:1500].splitlines():
                    log(f"  {line}")

                # ------------------------------------------------------
                # STEP 5: Compile SQL for a model
                # ------------------------------------------------------
                # The "compile" tool resolves all Jinja (ref(), source(),
                # macros) in a model and returns the final SQL that dbt
                # would run against Snowflake. We compile
                # int_web_analytics_with_customers because it has a JOIN
                # across two sources (web analytics + customers), making
                # the resolved SQL interesting to inspect.
                # ------------------------------------------------------
                log("")
                log("Step 5: Compiling SQL for int_web_analytics_with_customers...")
                raw = await call_tool(
                    session,
                    "compile",
                    {"select": "int_web_analytics_with_customers"},
                )
                if raw.startswith("[ERROR"):
                    log(f"  {raw}")
                else:
                    log("  Compiled SQL:")
                    for line in raw[:1500].splitlines():
                        log(f"  {line}")

                # ------------------------------------------------------
                # STEP 6: Explore model lineage
                # ------------------------------------------------------
                # get_lineage_dev traces the dependency graph of a model:
                # which sources feed into it (upstream) and which models
                # depend on it (downstream). We trace stg_web_analytics:
                # upstream = raw_ext.web_analytics_raw (source),
                # downstream = int_web_analytics_with_customers.
                # unique_id format: "model.<project_name>.<model_name>"
                # Project name is "adventure" (from dbt_project.yml).
                # depth=2 means trace two hops in each direction.
                # ------------------------------------------------------
                log("")
                log("Step 6: Exploring lineage for stg_web_analytics...")
                raw = await call_tool(
                    session,
                    "get_lineage_dev",
                    {
                        "unique_id": "model.adventure.stg_web_analytics",
                        "depth": 2,
                    },
                )
                if raw.startswith("[ERROR"):
                    log("  get_lineage_dev not available, trying get_lineage...")
                    raw = await call_tool(
                        session,
                        "get_lineage",
                        {
                            "unique_id": "model.adventure.stg_web_analytics",
                            "depth": 2,
                        },
                    )
                for line in raw[:1500].splitlines():
                    log(f"  {line}")

                # ------------------------------------------------------
                log("")
                log("=" * 60)
                log("Demo complete!")
                log("=" * 60)

    except Exception as e:
        log(f"\nFailed to connect to MCP server: {e}")
        log("Make sure the server is running: docker compose up -d dbt-mcp")
        log("Then verify: curl -s http://localhost:8000/sse | head -3")
        sys.exit(1)


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        log("\nDemo interrupted by user.")
    finally:
        save_log()
