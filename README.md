# Autonomous SQL Agent for Retail / E-Commerce Analytics

This project implements a multi-agent SQL analytics assistant for a retail and e-commerce warehouse. It accepts natural-language business questions, generates warehouse-safe SQL with a Hugging Face model, validates and executes the query against PostgreSQL, discovers patterns in the result, produces one minimal chart, and exports the answer as CSV, XLSX, and PDF.

## Stack

- Python 3.11+
- Streamlit UI
- PostgreSQL warehouse
- Hugging Face `defog/sqlcoder-7b-2`
- Pandas, Plotly, SQLAlchemy, sqlglot, reportlab

## Quick Start

1. Create a virtual environment and install dependencies.
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and update `DATABASE_URL` if needed.
3. Initialize or seed the warehouse.
   - `PYTHONPATH=src python3 -m autonomous_sql_agent.cli init-db`
   - `PYTHONPATH=src python3 -m autonomous_sql_agent.cli seed-db --orders 100000`
4. Run the Streamlit app.
   - `streamlit run app/streamlit_app.py`

## Project Layout

- `app/streamlit_app.py`: Streamlit interface and warehouse controls
- `src/autonomous_sql_agent/`: core configuration, agents, orchestration, model wrapper, analytics, exports
- `data/warehouse_schema.sql`: retail/e-commerce star schema
- `tests/`: lightweight unit tests for validator, charting, and fallback generation

## Demo Questions

- `Show the top 10 products by revenue.`
- `Analyze the monthly sales trend over the last year.`
- `Find unusual return activity by region.`
- `Compare customer segments by revenue and average order value.`

## Current Notes

- The Hugging Face model is the primary SQL generator. If local model loading fails, the app falls back to a rule-based generator so the rest of the workflow still runs.
- The warehouse is read-only from the agent's analysis path. The write path exists only for schema setup, seeding, exports, and session logging.
- The current app assumes a local PostgreSQL instance and English-language prompts.
