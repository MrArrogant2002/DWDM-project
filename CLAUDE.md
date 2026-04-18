# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit DATABASE_URL if needed

# Database
PYTHONPATH=src python3 -m autonomous_sql_agent.cli init-db
PYTHONPATH=src python3 -m autonomous_sql_agent.cli seed-db --orders 100000

# Run app
streamlit run app/streamlit_app.py

# Tests
PYTHONPATH=src pytest tests/
PYTHONPATH=src pytest tests/test_sql_validator.py   # single test file
```

## Architecture

Single-repo academic demo. No microservices — all agents are Python classes coordinated by one `AnalysisOrchestrator`.

**Agent pipeline** (`orchestrator.py`): `IntentAgent → PlanningAgent → SchemaAgent → SQLGenerationAgent → SQLValidationAgent → ExecutionAgent → PatternDiscoveryAgent → ReportAgent`. The orchestrator loops `SQLGenerationAgent → SQLValidationAgent → ExecutionAgent` up to `MAX_GENERATION_RETRIES` on failure before raising.

**Key modules** (`src/autonomous_sql_agent/`):
- `models.py` — Pydantic dataclasses: `AnalysisRequest`, `AgentState`, `AnalysisResponse`, `ChartSpec`
- `config.py` — `AppConfig.from_env()` reads all settings from `.env`; no hardcoded values elsewhere
- `model.py` — `HuggingFaceSQLGenerator` wraps `defog/sqlcoder-7b-2` in 4-bit quantization; falls back to rule-based generator if model load fails
- `metadata.py` — `SchemaMetadataService` introspects PostgreSQL and builds the schema summary + business glossary injected into prompts
- `sql_validation.py` — `SQLValidator` uses `sqlglot` to parse and enforce SELECT-only policy before any execution
- `database.py` — `DatabaseManager`: read-only analysis path + write path only for init/seed/export/session logging
- `analytics.py` / `charting.py` — `AnalyticsService` (z-score, IsolationForest, moving average) and `ChartService` (Plotly)
- `exporters.py` — CSV, XLSX (two-sheet), PDF (reportlab) exports written to `EXPORT_DIR`

**Warehouse schema** (`data/warehouse_schema.sql`): star schema — `dim_customer`, `dim_product`, `dim_date`, `dim_region`, `dim_channel`, `fact_orders`, `fact_order_items`, `fact_returns`.

## Environment variables (`.env`)

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgresql+psycopg2://postgres:postgres@localhost:5432/autonomous_sql_dw` |
| `HF_MODEL_ID` | `defog/sqlcoder-7b-2` |
| `DEVICE` | `auto` |
| `STATEMENT_TIMEOUT_MS` | `10000` |
| `EXPORT_DIR` | `exports` |
| `PREVIEW_ROW_LIMIT` | `200` |
| `EXPORT_ROW_LIMIT` | `50000` |
| `MAX_GENERATION_RETRIES` | `2` |

## Constraints

- **SQL safety**: only `SELECT` is allowed — reject DDL, DML, multi-statement, and unsafe functions. `sqlglot` parses before any DB call; `EXPLAIN` runs before execution.
- **Warehouse access**: `DatabaseManager` must remain read-only on the analysis path. Write access is only for `init-db`, `seed-db`, export artifacts, and session history.
- **Model fallback**: if `defog/sqlcoder-7b-2` cannot load, fall back to `Qwen/Qwen2.5-3B-Instruct` (same prompt contract). Do not break the pipeline.
- **Export cap**: full exports are capped at 50,000 rows; auto-narrow with a warning above that limit.
- **`PYTHONPATH=src`** must be set when running tests or the CLI because the package is not installed.
