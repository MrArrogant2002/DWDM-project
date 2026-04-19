# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env

# Database — SQLite (zero setup, default)
PYTHONPATH=src python -m autonomous_sql_agent.cli init-db
PYTHONPATH=src python -m autonomous_sql_agent.cli seed-db --orders 10000

# Run app
streamlit run app/streamlit_app.py

# Tests
PYTHONPATH=src pytest tests/
PYTHONPATH=src pytest tests/test_sql_validator.py   # single file
PYTHONPATH=src pytest tests/ -k "test_name"         # single test

# Lint / format
ruff check src/ tests/
black src/ tests/
```

`PYTHONPATH=src` is required for all CLI and test commands — the package is not installed.

## Architecture

Single-repo M.Tech prototype. No microservices — all agents are Python classes coordinated by `AnalysisOrchestrator`.

### Two data modes

**Mode 1 — Built-in warehouse**: run `init-db` + `seed-db` to populate a synthetic retail star schema (`fact_orders`, `dim_customer`, etc.) defined in `data/warehouse_schema.sql`.

**Mode 2 — CSV upload**: user uploads a CSV in the Streamlit UI. `CSVIngestor` (`csv_ingestion.py`) auto-builds a star schema from it (fact table + one dim table per low-cardinality column) and writes it to the same SQLite database. After upload, `orchestrator.set_active_blueprint(blueprint)` scopes all queries to only those tables.

### Two LLM modes

**API mode** (when `HF_TOKEN` is set): calls `HF_INFERENCE_MODEL` (default `Qwen/Qwen2.5-Coder-7B-Instruct`) via HuggingFace Inference API. No local GPU needed.

**Fallback mode** (no token): `HuggingFaceSQLGenerator._fallback_candidate()` uses hand-written rule-based SQL templates. Covers top-N, trend, returns-by-region, segments, and a generic aggregation. Always works offline.

### Agent pipeline (`orchestrator.py`)

```
IntentAgent → PlanningAgent
  → [metadata_service directly, not SchemaAgent] → schema_context + glossary
  → retry loop (MAX_GENERATION_RETRIES):
      SQLGenerationAgent → SQLValidationAgent → ExecutionAgent
  → PatternDiscoveryAgent → ReportAgent → ExportService → session log
```

`SchemaAgent` is instantiated but **not called** in `analyze()` — the orchestrator calls `metadata_service.build_schema_summary(table_filter=self._active_table_filter)` directly to honour the CSV upload scope. `SchemaAgent.run()` exists only for standalone testing.

### Key non-obvious behaviours

- **SQL dialect**: `SQLValidator._try_parse()` uses `read="sqlite"` then falls back to dialect-agnostic. Never use `read="postgres"` — prompts explicitly request SQLite syntax (`strftime`, `CAST AS INTEGER`).
- **`execute_sql()`** in `DatabaseManager` is a raw write-path method called only by `seed.py`. It has no validation — do not expose it to user input.
- **`_active_table_filter`** on the orchestrator is set per Streamlit session via `set_active_blueprint()`. The cached `@st.cache_resource` orchestrator singleton persists this across reruns; `streamlit_app.py` re-applies it from `session_state` on every rerun.
- **`models.py`** uses standard `@dataclass(slots=True)`, not Pydantic.
- **Export failures** are non-fatal — each of CSV/XLSX/PDF is wrapped in its own try/except in `ExportService.export_analysis()`.

## Environment variables (`.env`)

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | `sqlite:///data/warehouse.db` | Switch to `postgresql+psycopg2://...` for Postgres |
| `HF_TOKEN` | *(empty)* | Required for LLM API mode; omit for fallback |
| `HF_INFERENCE_MODEL` | `Qwen/Qwen2.5-Coder-7B-Instruct` | Model used via HF Inference API |
| `HF_MODEL_ID` | `defog/sqlcoder-7b-2` | Reserved for future local loading |
| `DEVICE` | `auto` | `cpu` / `cuda` / `auto` |
| `STATEMENT_TIMEOUT_MS` | `10000` | Applied via `SET LOCAL statement_timeout` (Postgres only) |
| `EXPORT_DIR` | `exports` | Auto-created at startup |
| `PREVIEW_ROW_LIMIT` | `200` | Rows shown in Streamlit table |
| `EXPORT_ROW_LIMIT` | `50000` | Hard cap on downloaded exports |
| `MAX_GENERATION_RETRIES` | `2` | SQL retry attempts before returning partial failure |
| `USE_FALLBACK_ONLY` | `false` | Force rule-based SQL regardless of token |

## Constraints

- **SQL safety**: `SQLValidator` enforces SELECT-only via keyword scan + `sqlglot` parse + `EXPLAIN` before execution. DDL, DML, multi-statement, and unsafe functions (`pg_sleep`, `dblink`, `copy`) are all rejected.
- **Read-only analysis path**: `DatabaseManager.query_dataframe()` and `explain_query()` are the only methods used during agent analysis. `execute_sql()` and `write_dataframe()` are write-path only.
- **Model fallback**: if HF API fails for any reason, `generate_candidate()` silently falls back to rule-based SQL. The pipeline must never crash due to model unavailability.
- **Export cap**: results above `EXPORT_ROW_LIMIT` are truncated with a warning appended to `state.warnings`.
