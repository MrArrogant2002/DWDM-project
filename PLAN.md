# Autonomous SQL Agent Project Plan

## Summary
- Build a from-scratch, single-repo academic demo for a **retail / e-commerce data warehouse** with a **fully autonomous multi-agent SQL workflow**.
- Use **Python 3.11**, **Streamlit** for the UI, **PostgreSQL** as the local warehouse, and **Hugging Face `defog/sqlcoder-7b-2`** as the primary NL-to-SQL model.
- Scope for v1: natural-language question input, autonomous planning, schema-aware SQL generation, SQL validation, warehouse execution, pattern discovery, one minimal chart, and **CSV / XLSX / PDF downloads**.
- Architecture choice: a **single Streamlit app with internal service modules**, not microservices. The agents are implemented as Python classes coordinated by one orchestrator.

## Implementation Steps
1. **Bootstrap the repository**
- Create `app/`, `src/`, `data/`, `tests/`, `exports/`, and `docs/`.
- Add `requirements.txt` with: `streamlit`, `sqlalchemy`, `psycopg2-binary`, `pandas`, `plotly`, `scikit-learn`, `transformers`, `accelerate`, `bitsandbytes`, `sqlglot`, `faker`, `reportlab`, `xlsxwriter`, `python-dotenv`.
- Add `.env` support for `DATABASE_URL`, `HF_MODEL_ID`, `DEVICE`, `STATEMENT_TIMEOUT_MS`, and `EXPORT_DIR`.
- Add structured logging and a simple config loader so every module uses the same runtime settings.

2. **Build the retail / e-commerce warehouse**
- Create a star schema with `dim_customer`, `dim_product`, `dim_date`, `dim_region`, `dim_channel`, `fact_orders`, `fact_order_items`, and `fact_returns`.
- Keep business metrics explicit: revenue, quantity, discount, return amount, return reason, shipping time, and order status.
- Generate synthetic but realistic seed data with seasonality and anomalies: at least `100k` orders, uneven regional demand, promotional spikes, and return outliers.
- Add SQL scripts or Python seed scripts to create tables and load sample data deterministically.

3. **Implement schema intelligence**
- Build a metadata loader that reads tables, columns, primary keys, foreign keys, and basic column stats from PostgreSQL.
- Create a small business glossary that maps user terms like “sales”, “revenue”, “returns”, “top products”, and “region” to warehouse fields.
- Produce a compact schema summary string for prompt grounding so the model does not receive the full raw schema every time.

4. **Integrate the Hugging Face model**
- Load `defog/sqlcoder-7b-2` locally with `transformers` in 4-bit mode using `bitsandbytes`; run on GPU if available, else CPU with reduced concurrency.
- Use deterministic generation defaults: `temperature=0.1`, `top_p=0.9`, `max_new_tokens=512`.
- Define one SQL-generation prompt template containing: user question, schema summary, business glossary, SQL rules, and required output fields.
- Require the model output to include `sql`, `analysis_goal`, `tables_used`, and `chart_hint` in a structured JSON-like block.

5. **Implement the autonomous multi-agent pipeline**
- Create these agents as separate classes: `IntentAgent`, `PlanningAgent`, `SchemaAgent`, `SQLGenerationAgent`, `SQLValidationAgent`, `ExecutionAgent`, `PatternDiscoveryAgent`, and `ReportAgent`.
- Add one orchestrator that passes a shared state object through the agents in order.
- Retry policy: allow up to `2` regenerate-and-revalidate cycles if SQL parsing fails or execution returns a schema-related error.
- Low-confidence behavior for v1: do **not** stop for clarification; continue with best-effort execution and return warnings plus suggested follow-up questions.

6. **Add SQL safety and execution controls**
- Parse all generated SQL with `sqlglot` before execution.
- Allow only `SELECT` queries; reject DDL, DML, multi-statement SQL, and unsafe functions.
- Run `EXPLAIN` before the real query and reject obviously broken SQL before warehouse execution.
- Enforce a statement timeout and preview-row limit; show only the first `200` rows in the UI.
- Export full results up to `50,000` rows; above that, require the query to be narrowed automatically with a warning.

7. **Implement analytics, pattern discovery, and minimal charts**
- Run trend analysis for date-based results using moving averages and growth-rate comparisons.
- Run anomaly detection with z-score for simple series and `IsolationForest` for multi-feature result sets.
- Run clustering only when the result set has enough numeric records and the business question implies segmentation.
- Use chart-selection rules: line chart for time series, bar chart for category-vs-value, scatter plot for two numeric measures, otherwise a formatted table.
- Generate plain-English insights with 3 parts: direct answer, notable pattern, and one recommended follow-up query.

8. **Implement downloads and session history**
- CSV export: raw query result only.
- XLSX export: result table plus a second sheet for insight summary and generated SQL.
- PDF export: title, user question, generated SQL, key insights, one chart image, and execution timestamp.
- Store session history with question, approved SQL, row count, chart type, and export paths in a lightweight app metadata table.

9. **Build the Streamlit application**
- Main screen: question box, “Run Analysis” action, and sample retail/e-commerce prompts.
- Output area: autonomous plan summary, generated SQL, validation warnings, result preview, insight text, chart, and download buttons.
- Secondary panels: schema snapshot, query history, and recent export links.
- Keep the UI intentionally minimal and demo-friendly; prioritize clarity over dashboard complexity.

10. **Document and package the project**
- Add a README with setup, local PostgreSQL instructions, seed-data generation, model setup, and demo walkthrough.
- Add a short architecture diagram showing the agent flow from query to report.
- Add a curated list of demo questions for retail/e-commerce analytics and anomaly detection.
- Record known limits: local model latency, synthetic data realism, and read-only warehouse scope.

## Interfaces and Behavior
- `AnalysisRequest`: `question`, optional `session_id`.
- `AgentState`: `question`, `intent`, `analysis_plan`, `schema_context`, `sql_candidates`, `approved_sql`, `result_df`, `insights`, `chart_spec`, `warnings`, `artifacts`.
- `AnalysisResponse`: `answer_markdown`, `approved_sql`, `preview_rows`, `chart_spec`, `downloads`, `warnings`, `follow_up_questions`.
- `ChartSpec`: `chart_type`, `x_field`, `y_field`, optional `group_field`, `title`.
- `ExportFormat`: `csv | xlsx | pdf`.
- Public-app decision: v1 exposes these through internal Streamlit actions, not a separate HTTP API.

## Test Plan
- Unit tests for schema summarization, glossary matching, SQL validation, chart selection, exporter generation, and anomaly-detection helpers.
- Integration tests that run sample retail/e-commerce questions end to end against seeded PostgreSQL data.
- Failure-case tests for ambiguous questions, zero-row results, invalid first-pass SQL, database timeout, and unavailable model load.
- Acceptance checks: every successful run shows a preview table, at most one auto-selected chart, and working CSV/XLSX/PDF downloads.
- Demo questions must include: top products by revenue, monthly sales trend, return-rate anomalies by region, and customer-segment outliers.

## Assumptions and Defaults
- The warehouse domain is **confirmed as retail / e-commerce**.
- The project is a **single-user academic demo**, not a production multi-tenant system.
- The primary model is **`defog/sqlcoder-7b-2` from Hugging Face**, running locally in quantized mode.
- If the machine cannot host that model reliably, the fallback is **`Qwen/Qwen2.5-3B-Instruct`** with the same prompt contract and no architecture changes.
- Language support for v1 is **English only**.
- Warehouse access is **read-only** from the agent’s perspective.
