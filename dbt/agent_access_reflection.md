# Agent Data Access Reflection

---

## 1. What Worked Well

The most impressive part of this experience was how quickly the MCP server made
the entire dbt project discoverable. Within a single `session.list_tools()` call,
the demo client found 12 operations it could invoke, from listing models to
tracing lineage, without any custom API code. The lineage query for
`stg_web_analytics` was particularly striking: the server returned a complete
parent-child graph showing that `raw_ext.web_analytics_raw` feeds into
`stg_web_analytics`, which then feeds into `int_web_analytics_with_customers`,
along with every data test attached to those models. An agent using this output
could understand the full pipeline topology without ever reading a SQL file.

The `get_node_details_dev` call on `stg_web_analytics` also worked well. It
returned the model description, column-level metadata, and test configurations
in structured JSON, exactly what an agent needs to reason about which columns
to select and how to join models safely. This confirmed that the documentation
work in Task 2 pays off directly: the richer the description, the more an agent
can do without guessing.

---

## 2. What Was Difficult or Confusing

The biggest friction came from tool name and parameter changes between dbt-mcp
versions. The starter code referenced `resource_type: "model"` as a string, but
dbt-mcp 1.14.0 expects a list: `resource_type: ["model"]`. Without running
Step 2 (list tools) first and iterating, that validation error would have been
hard to diagnose. The `compile` tool also returned just "OK" rather than the
compiled SQL text, because this version writes compiled output to files inside
the container instead of returning it to the client. These are the kinds of
silent behavioral differences that would trip up an agent relying on tool output
programmatically.

Setting up the Docker container also required more troubleshooting than expected.
The `dbt-mcp` package hardcodes `host=127.0.0.1`, which makes the server
invisible to Docker port mapping. Without the `start_mcp.py` wrapper that patches
the host to `0.0.0.0`, the container would start successfully but never be
reachable, the kind of bug that looks like a networking problem but is actually
an application configuration issue.

---

## 3. Documentation Quality

Before Task 2, most model YAML files had either no descriptions or minimal
one-liners. The column entries for `stg_ecom__sales_orders` had no descriptions
at all. After upgrading, every model states its grain, primary key, key joins,
and important caveats. For example, `int_web_analytics_with_customers` now
explicitly explains that a LEFT JOIN is used because the API can generate
customer IDs outside the known customer dimension, and that customer columns
will be null in those cases.

What surprised me is how much this also improved the model for human readers.
Documenting the grain forces you to think clearly about what the model actually
represents. Writing out join targets (e.g., "Foreign key to
stg_adventure_db__customers.customer_id") makes the data model self-explanatory
in a way that a column named `customer_id` alone never could. Agent-friendly
and human-friendly documentation turn out to be the same thing, just written
with more discipline.

---

## 4. Production Considerations

Deploying this in production would require several safeguards absent here. The
MCP server currently has unrestricted access to the Snowflake warehouse, including
the ability to trigger `dbt run` or `dbt build` through the exposed CLI tools,
meaning an agent could kick off full model rebuilds. In production, the tool set
should be restricted to read-only operations with write operations behind stricter
authorization.

PII exposure is also a real risk. The `stg_adventure_db__customers` model
contains email addresses and mailing addresses. Any agent querying
`int_web_analytics_with_customers` receives those fields by default. A production
deployment would need Snowflake column-level masking policies and explicit
documentation flagging which columns contain sensitive data, so agents know not
to surface them to end users.

Finally, cost management matters. The `show` tool can execute arbitrary SQL
against Snowflake. An agent running repeated or poorly scoped queries could drain
warehouse credits quickly. Query timeouts, row limits, and credit monitoring
would all be necessary before opening this to non-technical users.

---

## 5. Business Use Cases

**Customer browsing-to-purchase analysis:** A merchandising analyst could ask
an agent "Which product categories have the highest add-to-cart to purchase
conversion rate by US region?" The agent could join `int_web_analytics_with_customers`
(for event type and geography) to `stg_adventure_db__products` (for category),
write the aggregation, and return an answer, a task that would otherwise require
a custom query or a pre-built dashboard slice.

**Campaign attribution on demand:** A marketing manager could ask "Which email
campaign segments drove the most revenue last quarter?" The agent could join
`int_sales_orders_with_campaign` to `int_sales_order_line_items` and return
revenue by `customer_segment` and `ad_strategy`, which is ad hoc analysis a traditional
dashboard cannot provide without being built for that exact question in advance.

**Pipeline health monitoring:** A data engineer on call could ask "Are any dbt
tests currently failing, and which models are affected?" The agent could use the
lineage tool to identify which models feed into failing tests and help prioritize
investigation, faster than manually scanning dbt Cloud logs.

---

## 6. The Bigger Picture

This project changed how I think about what a data engineer actually builds.
Traditionally the deliverable is a pipeline: data moves from source to warehouse
to dashboard, and the dashboard is the interface. With MCP, the dbt project
itself becomes an interface that AI agents can navigate, query, and reason
about. The data engineer is no longer just building for Tableau or Looker; they
are building for agents that need structured, well-documented, semantically rich
models to function correctly.

This expands the role in a meaningful way. Writing good model descriptions is
no longer just documentation hygiene; it directly determines whether an agent
answers a business question accurately. A vague description leads to a wrong
join or a misunderstood grain, which leads to a wrong answer that someone might
act on. Data engineers will need to think like API designers: clear contracts,
explicit grain declarations, documented edge cases, and intentional exposure of
only what downstream consumers should see. That is a more demanding standard
than what most data teams hold themselves to today, and I think it is a better one.
