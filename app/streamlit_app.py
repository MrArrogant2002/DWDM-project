from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd
import streamlit as st

from autonomous_sql_agent.charting import ChartService
from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.metadata import SchemaMetadataService
from autonomous_sql_agent.models import AnalysisRequest
from autonomous_sql_agent.orchestrator import AnalysisOrchestrator
from autonomous_sql_agent.seed import WarehouseSeeder


st.set_page_config(page_title="Autonomous SQL Agent", layout="wide")


@st.cache_resource
def get_app_services() -> tuple[AppConfig, AnalysisOrchestrator, WarehouseSeeder]:
    config = AppConfig.from_env()
    orchestrator = AnalysisOrchestrator(config)
    seeder = WarehouseSeeder(config, orchestrator.database)
    return config, orchestrator, seeder


def _display_download_button(label: str, path: str | None, mime: str) -> None:
    if not path:
        return
    file_path = Path(path)
    if not file_path.exists():
        return
    with file_path.open("rb") as file_handle:
        st.download_button(
            label=label,
            data=file_handle.read(),
            file_name=file_path.name,
            mime=mime,
        )


def main() -> None:
    config, orchestrator, seeder = get_app_services()
    chart_service = ChartService()
    metadata_service = SchemaMetadataService(orchestrator.database)

    st.title("Autonomous SQL Agent for Retail / E-Commerce Analytics")
    st.caption("A multi-agent warehouse assistant powered by Hugging Face SQL generation and autonomous pattern discovery.")

    with st.sidebar:
        st.subheader("Warehouse Setup")
        st.write("Use the local PostgreSQL retail warehouse configured in `.env`.")
        if st.button("Seed Demo Warehouse"):
            with st.spinner("Generating retail/e-commerce warehouse data. This can take time for 100k orders."):
                seeder.seed_all()
            st.success("Warehouse schema and seed data are ready.")

        st.subheader("Schema Snapshot")
        try:
            st.code(metadata_service.build_schema_summary(), language="text")
        except Exception as exc:
            st.warning(f"Schema metadata is unavailable until the warehouse is initialized: {exc}")

        st.subheader("Recent Sessions")
        try:
            sessions = orchestrator.database.recent_sessions()
        except Exception:
            sessions = []
        for session in sessions:
            st.write(f"- {session['created_at']}: {session['question'][:40]}")

    sample_prompts = [
        "Show the top 10 products by revenue.",
        "Analyze the monthly sales trend over the last year.",
        "Find unusual return activity by region.",
        "Compare customer segments by revenue and average order value.",
    ]

    quick_prompt = st.selectbox("Quick prompt", [""] + sample_prompts, index=0)
    default_question = quick_prompt or sample_prompts[0]
    question = st.text_area(
        "Ask a warehouse question",
        value=default_question,
        height=120,
        placeholder="Why did sales drop in February for the West region?",
    )

    run_clicked = st.button("Run Analysis", type="primary")
    if run_clicked:
        with st.spinner("The agent is planning, generating SQL, and analyzing the warehouse result."):
            response = orchestrator.analyze(AnalysisRequest(question=question))
        st.session_state["latest_response"] = response

    response = st.session_state.get("latest_response")
    if response is None:
        return

    st.markdown(response.answer_markdown)

    st.subheader("Autonomous Plan")
    for item in response.analysis_plan:
        st.write(f"- {item}")

    st.subheader("Generated SQL")
    st.code(response.approved_sql, language="sql")

    if response.warnings:
        st.subheader("Warnings")
        for warning in response.warnings:
            st.warning(warning)

    st.subheader("Result Preview")
    preview_df = pd.DataFrame(response.preview_rows)
    st.dataframe(preview_df, use_container_width=True)

    if response.chart_spec is not None and response.result_df is not None:
        figure = chart_service.build_figure(response.result_df, response.chart_spec)
        if figure is not None:
            st.subheader("Minimal Chart")
            st.plotly_chart(figure, use_container_width=True)

    st.subheader("Downloads")
    download_columns = st.columns(3)
    with download_columns[0]:
        _display_download_button("Download CSV", response.downloads.csv_path, "text/csv")
    with download_columns[1]:
        _display_download_button(
            "Download Excel",
            response.downloads.xlsx_path,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with download_columns[2]:
        _display_download_button("Download PDF", response.downloads.pdf_path, "application/pdf")


if __name__ == "__main__":
    main()
