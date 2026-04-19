# Autonomous SQL Agent — Project Info

## What Is This Project?

This is an **M.Tech final project prototype** built for the DWDM (Data Warehousing and
Data Mining) course at SRM University.

**The core idea:**
A user uploads one or more retail / e-commerce CSV files. The system automatically
builds a relational star-schema warehouse from those files. The user then asks
questions in plain English — no SQL knowledge needed. A multi-agent AI pipeline
activates, plans the query, generates and validates SQL, runs it against the
warehouse, detects patterns, and returns results with an auto-selected chart and
downloadable exports (CSV / XLSX / PDF).

**In one sentence:**
> Natural-language → autonomous agents → SQL → warehouse → insights, all from a CSV upload.

---

## Who Is It Built For?

A business analyst or data analyst who has raw sales / order / returns data in CSV
format and wants to ask questions like:

- "What are the top 5 products by revenue last month?"
- "Show monthly sales trend for Electronics."
- "Are there any return-rate anomalies by region?"
- "Which customer segments have the highest average order value?"

No coding. No SQL. The agent handles everything.

---

## Project Structure

```
DWDM-proj/
│
├── app/
│   └── streamlit_app.py          ← Main UI — the only file users interact with
│
├── src/autonomous_sql_agent/     ← All backend logic
│   ├── config.py                 ← Reads .env settings into AppConfig
│   ├── models.py                 ← Data classes: AnalysisRequest, AgentState, AnalysisResponse, ChartSpec
│   ├── database.py               ← PostgreSQL / SQLite connection, query execution, session history
│   ├── seed.py                   ← Generates 100k+ synthetic retail orders for demo data
│   ├── csv_ingestion.py          ← CSV → star-schema auto-builder (CSVIngestor)
│   ├── metadata.py               ← Reads schema from DB, builds business glossary + schema summary
│   ├── model.py                  ← HuggingFace LLM wrapper (sqlcoder-7b-2 + Qwen fallback)
│   ├── prompts.py                ← Prompt templates for SQL generation and result summarisation
│   ├── agents.py                 ← 8 agent classes (see Agent Pipeline below)
│   ├── orchestrator.py           ← Coordinates all agents in sequence with retry logic
│   ├── sql_validation.py         ← sqlglot-based SELECT-only enforcer (safety layer)
│   ├── analytics.py              ← Trend analysis, anomaly detection (z-score, IsolationForest)
│   ├── charting.py               ← Auto-selects chart type and builds Plotly figure
│   ├── exporters.py              ← Produces CSV / XLSX / PDF downloads
│   ├── logging_utils.py          ← Shared logger setup
│   └── cli.py                    ← Command-line: init-db and seed-db commands
│
├── data/
│   └── warehouse_schema.sql      ← Star-schema DDL (dim + fact tables)
│
├── tests/
│   ├── test_sql_validator.py     ← Tests for SELECT-only enforcement
│   ├── test_analytics.py         ← Tests for anomaly detection helpers
│   ├── test_charting.py          ← Tests for chart selection logic
│   └── test_fallback_generator.py← Tests for rule-based SQL fallback
│
├── exports/                      ← Auto-created; holds CSV/XLSX/PDF downloads
├── .env                          ← Secret config (never committed to git)
├── .env.example                  ← Template showing every env variable
├── requirements.txt              ← All Python dependencies
├── README.md                     ← Setup and quickstart guide
└── PLAN.md                       ← Original architecture and implementation plan
```

---

## What Each Source File Does

### `config.py`
Reads all settings from the `.env` file using `os.getenv()`. Produces a single
`AppConfig` dataclass used everywhere. Nothing is hardcoded — all tunable values
(model name, row limits, timeouts, DB URL) come from here.

### `models.py`
Pure data classes — no logic. Defines the shapes that flow through the system:
- `AnalysisRequest` — what the user asked
- `AgentState` — the shared state object passed between agents
- `AnalysisResponse` — what the UI receives back
- `ChartSpec` — chart type + axis fields for Plotly
- `DownloadArtifacts` — file paths for CSV/XLSX/PDF

### `database.py`
Handles all database communication. Has two distinct paths:
- **Read-only path** (`explain_query`, `query_dataframe`) — used by the agent pipeline
- **Write path** (`execute_script`, `write_dataframe`, `save_session`) — used only by
  init-db, seed-db, CSV ingestion, and session history logging
- Supports both **SQLite** (default, zero-setup) and **PostgreSQL**
- Enforces statement timeout on every query

### `seed.py`
Generates synthetic but realistic retail data deterministically (same seed = same data).
Creates 6,000 customers, 300 products, 100k+ orders with:
- Seasonality (December sales 28% higher)
- Black Friday / promotional spikes
- Regional demand variation
- Return outliers (Apparel returns more; January return spike)

Run once with: `PYTHONPATH=src python -m autonomous_sql_agent.cli seed-db`

### `csv_ingestion.py`
The **data upload engine**. Takes any CSV file and automatically:
1. Reads and cleans it (removes empty rows, duplicates)
2. Detects column types (date, numeric, categorical, ID)
3. Labels each column as: `measure | dimension | date | id | text`
4. Builds a star-schema blueprint (fact table + one dim table per low-cardinality column)
5. Writes everything to the database
6. Streams progress updates to the UI in real time

### `metadata.py`
Reads the live database schema and builds two things:
- **Schema summary** — compact text description of tables + columns, injected into
  the LLM prompt so the model knows what tables exist
- **Business glossary** — maps user words like "sales", "revenue", "returns" to
  actual column names

### `model.py`
Wraps the HuggingFace LLM. Two modes:
- **LLM mode** (when `HF_TOKEN` is set) — calls `Qwen2.5-Coder-7B-Instruct` via
  HuggingFace Inference API
- **Fallback mode** (no token) — uses rule-based SQL templates that cover the most
  common retail queries (top products, trends, returns by region, segments)

The fallback ensures the demo always works, even without a GPU or API key.

### `prompts.py`
Defines the two prompt templates:
- `SQL_USER_TEMPLATE` — tells the LLM the schema, user question, and analysis plan,
  and asks for SQL + metadata in JSON format
- `SUMMARY_USER_TEMPLATE` — asks the LLM to explain query results in plain English

### `agents.py`
Eight agent classes — each does one job:

| Agent | Job |
|---|---|
| `IntentAgent` | Classifies the question: anomaly / trend / segmentation / comparison / summary |
| `PlanningAgent` | Builds a step-by-step analysis plan based on intent |
| `SchemaAgent` | Loads schema context and business glossary into state |
| `SQLGenerationAgent` | Calls the LLM (or fallback) to generate candidate SQL |
| `SQLValidationAgent` | Parses SQL with sqlglot, rejects anything that isn't a safe SELECT |
| `ExecutionAgent` | Runs the validated SQL against the warehouse, applies row limits |
| `PatternDiscoveryAgent` | Detects trends, anomalies, clusters; selects the right chart type |
| `ReportAgent` | Generates the plain-English insight summary |

### `orchestrator.py`
The conductor. Calls the agents in order and manages the **retry loop**:
```
IntentAgent → PlanningAgent → SchemaAgent
  └─ retry loop (up to MAX_GENERATION_RETRIES) ─┐
       SQLGenerationAgent → SQLValidationAgent   │
       ExecutionAgent ──────────────────────────┘
PatternDiscoveryAgent → ReportAgent → ExportService → SessionSave
```
If SQL fails validation or execution, it feeds the error back to SQLGenerationAgent
and tries again. If all retries fail, it returns a partial response with warnings.

### `sql_validation.py`
Security and safety layer. Uses `sqlglot` to parse generated SQL and reject:
- `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE` (DDL/DML)
- Multiple statements in one string
- Dangerous functions
- Anything that is not a plain `SELECT`

Also runs `EXPLAIN` against the database before real execution to catch broken SQL early.

### `analytics.py`
Pattern detection on query results:
- **Trend analysis** — moving averages, growth rate on time-series results
- **Anomaly detection** — z-score for simple numeric series; IsolationForest for
  multi-column result sets
- **Clustering** — applied when the question implies segmentation and result set is
  large enough

### `charting.py`
Picks the right chart automatically based on result shape:
- Time-based results → **line chart**
- Category vs. value → **bar chart**
- Two numeric columns → **scatter plot**
- Everything else → **formatted table**

Builds a Plotly figure ready for Streamlit.

### `exporters.py`
Produces three download formats:
- **CSV** — raw query result only
- **XLSX** — two sheets: result table + insight summary + generated SQL
- **PDF** — title, question, SQL, key insights, chart image, timestamp

### `logging_utils.py`
Thin wrapper that returns a standard Python logger for each module.
All logging goes to stdout — no `print()` statements anywhere.

### `cli.py`
Two command-line commands for setup:
- `init-db` — creates all warehouse tables from `data/warehouse_schema.sql`
- `seed-db --orders N` — fills the warehouse with N synthetic orders

---

## The Complete Flow

### Flow 1 — CSV Upload (one-time setup per dataset)

```
User uploads CSV in UI
        │
        ▼
streamlit_app.py → CSVIngestor.process()
        │
        ├─ Read CSV → clean (drop nulls/duplicates)
        ├─ Coerce types (detect dates, numbers)
        ├─ Profile columns (measure / dimension / date / id)
        ├─ Build SchemaBlueprint (fact table + dim tables)
        ├─ Write fact table to DB
        ├─ Write each dim table to DB
        └─ Return blueprint → stored in Streamlit session_state
                │
                ▼
        UI shows: fact table name, row count, column pills,
                  dimension table cards
```

### Flow 2 — Natural Language Query

```
User types question → clicks "Run Analysis"
        │
        ▼
AnalysisOrchestrator.analyze(AnalysisRequest)
        │
        ├─ IntentAgent         → classifies intent (anomaly/trend/etc.)
        ├─ PlanningAgent       → builds analysis plan (list of steps)
        ├─ SchemaAgent         → loads schema summary + glossary
        │
        │   ┌─── retry loop (up to MAX_GENERATION_RETRIES) ────┐
        ├─ SQLGenerationAgent  → calls LLM or fallback → SQL   │
        ├─ SQLValidationAgent  → sqlglot parse + SELECT-only    │
        ├─ ExecutionAgent      → EXPLAIN → query → DataFrame    │
        │   └── on failure: feed error back, retry ────────────┘
        │
        ├─ PatternDiscoveryAgent → trends, anomalies, chart spec
        ├─ ReportAgent          → plain-English insight text
        ├─ ExportService        → writes CSV + XLSX + PDF to exports/
        └─ DatabaseManager      → saves session to app_analysis_sessions
                │
                ▼
        AnalysisResponse returned to UI:
          - Analysis plan (what the agents decided to do)
          - Generated SQL (shown in expandable block)
          - Validation warnings (if any)
          - Result preview table (first 200 rows)
          - Auto-selected Plotly chart
          - Plain-English insight text
          - Download buttons (CSV / XLSX / PDF)
```

---

## How to Run

### Quickstart (SQLite — zero setup)

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy env file (SQLite is the default — no DB setup needed)
cp .env.example .env

# 4. (Optional) Seed the built-in retail warehouse
PYTHONPATH=src python -m autonomous_sql_agent.cli init-db
PYTHONPATH=src python -m autonomous_sql_agent.cli seed-db --orders 10000

# 5. Run the app
streamlit run app/streamlit_app.py
```

Then open http://localhost:8501

### With HuggingFace LLM (better SQL quality)

Add your HF token to `.env`:
```
HF_TOKEN=hf_your_token_here
```

Without it, the system uses the **rule-based fallback** — which still works for
common retail queries (top products, trends, returns, segments).

### With PostgreSQL (optional)

```
DATABASE_URL=postgresql+psycopg2://postgres:yourpassword@localhost:5432/autonomous_sql_dw
```

---

## Environment Variables (`.env`)

| Variable | Default | What it controls |
|---|---|---|
| `DATABASE_URL` | `sqlite:///data/warehouse.db` | Database connection |
| `HF_MODEL_ID` | `defog/sqlcoder-7b-2` | Local HF model (unused if token set) |
| `HF_TOKEN` | *(empty)* | Enables HF Inference API |
| `HF_INFERENCE_MODEL` | `Qwen/Qwen2.5-Coder-7B-Instruct` | Model used via API |
| `DEVICE` | `auto` | `cpu` / `cuda` / `auto` |
| `STATEMENT_TIMEOUT_MS` | `10000` | Max query time (10 seconds) |
| `EXPORT_DIR` | `exports` | Where CSV/XLSX/PDF files are saved |
| `PREVIEW_ROW_LIMIT` | `200` | Rows shown in UI preview |
| `EXPORT_ROW_LIMIT` | `50000` | Max rows in downloaded exports |
| `MAX_GENERATION_RETRIES` | `2` | SQL retry attempts before giving up |

---

## Known Limitations (Prototype Scope)

| Limitation | Reason |
|---|---|
| One CSV file active at a time | Streamlit session holds one blueprint |
| LLM runs via API, not locally | Local 7B model needs GPU; CPU is too slow |
| English questions only | No multilingual support |
| Read-only warehouse | Agents cannot modify data — by design |
| Synthetic data only | Real PII data not in scope |
| No user authentication | Single-user localhost demo |

---

## Demo Questions to Try

After seeding the built-in warehouse (`seed-db`):

1. `What are the top 5 products by revenue?`
2. `Show monthly sales trend for 2024`
3. `Which region has the highest return rate?`
4. `Compare revenue across sales channels`
5. `Are there any anomalies in order values?`
6. `What is the average order value by customer loyalty tier?`
7. `Show me the top 10 customers by total spend`
8. `Which product category has the most returns in January?`
