from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd
import streamlit as st

from autonomous_sql_agent.charting import ChartService
from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.csv_ingestion import CSVIngestor, SchemaBlueprint
from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.metadata import SchemaMetadataService
from autonomous_sql_agent.models import AnalysisRequest
from autonomous_sql_agent.orchestrator import AnalysisOrchestrator

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Autonomous SQL Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Shared CSS ─────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .step-done  { color: #22c55e; font-weight: 500; }
    .step-active{ color: #f59e0b; font-weight: 600; }
    .dim-card   { background:#1e293b; border-radius:8px; padding:10px 14px;
                  margin:4px 0; font-family:monospace; font-size:0.82rem; }
    .schema-header { font-size:1.05rem; font-weight:700; margin-bottom:4px; color:#7dd3fc; }
    .pill       { display:inline-block; padding:2px 8px; border-radius:12px;
                  font-size:0.75rem; margin:2px; }
    .pill-measure   { background:#1d4ed8; color:#fff; }
    .pill-dimension { background:#7c3aed; color:#fff; }
    .pill-date      { background:#065f46; color:#fff; }
    .pill-id        { background:#374151; color:#ccc; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Cached singletons ──────────────────────────────────────────────────────────
@st.cache_resource
def _get_config() -> AppConfig:
    return AppConfig.from_env()


@st.cache_resource
def _get_orchestrator(config: AppConfig) -> AnalysisOrchestrator:
    return AnalysisOrchestrator(config)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _pill(label: str, kind: str) -> str:
    return f'<span class="pill pill-{kind}">{label}</span>'


def _render_blueprint(bp: SchemaBlueprint) -> None:
    """Render the generated star-schema as styled cards."""
    st.markdown(
        f'<div class="schema-header">📋 Fact table &nbsp;→&nbsp; <code>{bp.fact_table}</code>'
        f"&nbsp; ({bp.row_count:,} rows)</div>",
        unsafe_allow_html=True,
    )
    pills = "".join(
        [_pill(c, "measure") for c in bp.measure_columns]
        + [_pill(c, "dimension") for c in bp.dimension_columns]
        + [_pill(c, "date") for c in bp.date_columns]
        + [_pill(c, "id") for c in bp.id_columns]
    )
    st.markdown(pills, unsafe_allow_html=True)

    if bp.dim_tables:
        st.markdown("**Dimension tables extracted:**")
        cols = st.columns(min(len(bp.dim_tables), 3))
        for i, (dim_tbl, src_col) in enumerate(bp.dim_tables.items()):
            with cols[i % 3]:
                st.markdown(
                    f'<div class="dim-card">🗂 <b>{dim_tbl}</b><br/>'
                    f'<span style="color:#94a3b8">← {src_col}</span></div>',
                    unsafe_allow_html=True,
                )


def _display_download_button(label: str, path: str | None, mime: str) -> None:
    if not path:
        return
    file_path = Path(path)
    if not file_path.exists():
        return
    with file_path.open("rb") as fh:
        st.download_button(
            label=label, data=fh.read(), file_name=file_path.name, mime=mime
        )


def _run_ingestion(
    uploaded_file, stem: str, config: AppConfig
) -> SchemaBlueprint | None:
    """Drive CSVIngestor with an animated step list."""
    db = DatabaseManager(config)
    ingestor = CSVIngestor(db)

    progress_bar = st.progress(0.0, text="Starting…")
    steps_box = st.empty()
    done_steps: list[str] = []
    blueprint: SchemaBlueprint | None = None

    for update in ingestor.process(uploaded_file, table_prefix=stem):
        frac = float(update["progress"])
        step = update["step"]
        progress_bar.progress(min(frac, 1.0), text=step)

        done_steps.append(step)
        html_lines = []
        for i, s in enumerate(done_steps):
            if i < len(done_steps) - 1:
                html_lines.append(f'<div class="step-done">✅ {s}</div>')
            else:
                html_lines.append(f'<div class="step-active">⚙️ {s}</div>')
        steps_box.markdown("\n".join(html_lines), unsafe_allow_html=True)
        time.sleep(0.05)  # slight pause so animation is visible

        if "result" in update:
            blueprint = update["result"]

    # Replace spinner with all-done list
    html_lines = [f'<div class="step-done">✅ {s}</div>' for s in done_steps]
    steps_box.markdown("\n".join(html_lines), unsafe_allow_html=True)
    progress_bar.empty()
    return blueprint


# ── Main app ───────────────────────────────────────────────────────────────────
def main() -> None:
    config = _get_config()
    orchestrator = _get_orchestrator(config)
    chart_service = ChartService()

    # Restore active blueprint on every Streamlit rerun
    if "blueprint" in st.session_state:
        orchestrator.set_active_blueprint(st.session_state["blueprint"])

    st.title("🤖 Autonomous SQL Agent")
    st.caption(
        "Upload any retail / e-commerce CSV → auto-build a star schema → query in plain English."
    )

    # HF token status banner
    if config.hf_token:
        st.success(
            f"✅ HF Inference API active &nbsp;|&nbsp; model: `{config.hf_inference_model}`",
            icon="🤗",
        )
    else:
        st.warning(
            "⚠️ No HF token found — running in **fallback mode** (rule-based SQL). "
            "Add `HF_TOKEN=hf_…` to your `.env` to enable full LLM planning.",
            icon="💡",
        )

    tab_upload, tab_query = st.tabs(["📂  Upload & Process", "🔍  Query Warehouse"])

    # ── TAB 1 — Upload & Process ───────────────────────────────────────────────
    with tab_upload:
        st.subheader("Upload a retail / e-commerce CSV")
        st.markdown(
            "The agent will **clean the data**, infer column roles, and build a "
            "**star schema** (fact table + dimension tables) automatically."
        )

        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            help="Any CSV with retail/e-commerce data — orders, products, returns, etc.",
        )

        if uploaded_file is not None:
            stem = Path(uploaded_file.name).stem
            stem_clean = stem.replace(" ", "_").replace("-", "_").lower()

            col_info, col_btn = st.columns([3, 1])
            with col_info:
                st.markdown(
                    f"**File:** `{uploaded_file.name}` &nbsp;|&nbsp; "
                    f"**Size:** {uploaded_file.size / 1024:.1f} KB"
                )
            with col_btn:
                process_btn = st.button(
                    "⚡ Process & Load", type="primary", use_container_width=True
                )

            if process_btn:
                st.markdown("---")
                st.markdown("### Schema Generation Pipeline")

                with st.container():
                    blueprint = _run_ingestion(uploaded_file, stem_clean, config)

                if blueprint:
                    st.session_state["blueprint"] = blueprint
                    st.session_state["data_loaded"] = True
                    orchestrator.set_active_blueprint(blueprint)
                    st.success(
                        f"🎉 Warehouse ready! "
                        f"**{blueprint.row_count:,} rows** loaded into "
                        f"`{blueprint.fact_table}` + "
                        f"**{len(blueprint.dim_tables)} dimension tables**."
                    )
                    st.markdown("---")
                    st.markdown("### Generated Schema")
                    _render_blueprint(blueprint)

        # Show schema if already loaded this session
        elif st.session_state.get("data_loaded") and "blueprint" in st.session_state:
            bp: SchemaBlueprint = st.session_state["blueprint"]
            st.info(
                "Data already loaded this session. Switch to the **Query** tab to analyse it."
            )
            _render_blueprint(bp)

        else:
            st.markdown(
                """
                **What happens when you upload:**
                1. 🔍 Detect and coerce column data types
                2. 🧹 Remove empty rows and duplicates
                3. 🏷️ Classify columns as measures / dimensions / dates / IDs
                4. 🏗️ Build a star schema (fact table + dimension look-ups)
                5. 💾 Load everything into the local SQLite warehouse
                """
            )

    # ── TAB 2 — Query ─────────────────────────────────────────────────────────
    with tab_query:
        if not st.session_state.get("data_loaded"):
            st.info(
                "👆 Please upload and process a CSV in the **Upload & Process** tab first.",
                icon="📂",
            )
            return

        bp: SchemaBlueprint = st.session_state["blueprint"]

        # Schema sidebar panel
        with st.expander("📋 Current schema", expanded=False):
            _render_blueprint(bp)
            st.markdown("---")
            metadata_service = SchemaMetadataService(orchestrator.database)
            try:
                st.code(metadata_service.build_schema_summary(), language="text")
            except Exception:
                pass

        st.subheader("Ask a question")

        sample_prompts = [
            f"Show the top 10 items by total revenue from {bp.fact_table}.",
            "What is the monthly sales trend?",
            "Find unusual patterns by region or category.",
            "Compare segments by revenue and record count.",
            "Summarize the key findings from the data.",
        ]
        chosen = st.selectbox("Quick prompt", [""] + sample_prompts, index=0)
        question = st.text_area(
            "Your question",
            value=chosen or "",
            height=100,
            placeholder="e.g. Which category had the highest revenue last quarter?",
        )

        run_btn = st.button("▶  Run Analysis", type="primary")

        if run_btn and question.strip():
            with st.spinner(
                "Agent is planning, generating SQL, and querying the warehouse…"
            ):
                response = orchestrator.analyze(AnalysisRequest(question=question))
                st.session_state["latest_response"] = response
                st.session_state["latest_question"] = question

        response = st.session_state.get("latest_response")
        if response is None:
            return

        st.markdown("---")

        # Detect failure: no rows returned despite having SQL
        execution_failed = len(response.preview_rows) == 0 and bool(
            response.approved_sql
        )

        # ── Agent plan ────────────────────────────────────────────────────────
        with st.expander("🧠 Agent plan", expanded=True):
            for step in response.analysis_plan:
                st.markdown(f"- {step}")

        # ── Generated SQL (always shown — critical for debugging) ──────────────
        sql_label = (
            "🖥️ Generated SQL"
            if not execution_failed
            else "🖥️ Generated SQL (failed — see warnings)"
        )
        with st.expander(sql_label, expanded=True):
            st.code(response.approved_sql, language="sql")

        # ── Warnings (shown prominently on failure) ────────────────────────────
        if response.warnings:
            if execution_failed:
                st.error(
                    "**SQL execution failed.** The model-generated SQL could not run against your data. Warnings:"
                )
            for w in response.warnings:
                st.warning(w)

        if execution_failed:
            st.info(
                "💡 **Tip:** Try rephrasing as a simpler question, e.g. "
                f"*'Show all columns from {bp.fact_table} LIMIT 10'* "
                "to verify the table loaded correctly."
            )
            return

        # ── Result table ──────────────────────────────────────────────────────
        st.subheader(f"Results — {len(response.preview_rows):,} rows")
        preview_df = pd.DataFrame(response.preview_rows)
        st.dataframe(preview_df, use_container_width=True, height=320)

        # ── Chart ─────────────────────────────────────────────────────────────
        if (
            response.chart_spec
            and response.result_df is not None
            and not response.result_df.empty
        ):
            figure = chart_service.build_figure(response.result_df, response.chart_spec)
            if figure is not None:
                st.subheader("📊 Chart")
                st.plotly_chart(figure, use_container_width=True)

        # ── Summary (only when user asked for it) ─────────────────────────────
        if response.needs_summary and response.answer_markdown:
            st.subheader("📝 Summary")
            st.markdown(response.answer_markdown)
        elif response.needs_summary and not response.answer_markdown:
            st.info("Summary requires a valid HF token. The data is shown above.")

        # ── Follow-up ideas ───────────────────────────────────────────────────
        if response.follow_up_questions:
            with st.expander("💡 Follow-up ideas"):
                for fq in response.follow_up_questions:
                    st.markdown(f"- {fq}")

        # ── Downloads ─────────────────────────────────────────────────────────
        st.subheader("⬇️ Downloads")
        dl_cols = st.columns(3)
        with dl_cols[0]:
            _display_download_button("📄 CSV", response.downloads.csv_path, "text/csv")
        with dl_cols[1]:
            _display_download_button(
                "📊 Excel",
                response.downloads.xlsx_path,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with dl_cols[2]:
            _display_download_button(
                "📑 PDF", response.downloads.pdf_path, "application/pdf"
            )


if __name__ == "__main__":
    main()
