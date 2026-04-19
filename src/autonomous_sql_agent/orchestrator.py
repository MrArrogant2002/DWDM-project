from __future__ import annotations

from autonomous_sql_agent.csv_ingestion import SchemaBlueprint

from autonomous_sql_agent.agents import (
    ExecutionAgent,
    IntentAgent,
    PatternDiscoveryAgent,
    PlanningAgent,
    ReportAgent,
    SQLGenerationAgent,
    SQLValidationAgent,
    SchemaAgent,
)
from autonomous_sql_agent.analytics import AnalyticsService
from autonomous_sql_agent.charting import ChartService
from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.exporters import ExportService
from autonomous_sql_agent.logging_utils import get_logger
from autonomous_sql_agent.metadata import SchemaMetadataService
from autonomous_sql_agent.model import HuggingFaceSQLGenerator
import pandas as pd

from autonomous_sql_agent.models import (
    AnalysisRequest,
    AnalysisResponse,
    AgentState,
    DownloadArtifacts,
)
from autonomous_sql_agent.sql_validation import SQLValidator

logger = get_logger(__name__)


class AnalysisOrchestrator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.database = DatabaseManager(config)
        self.metadata_service = SchemaMetadataService(self.database)
        self.chart_service = ChartService()
        self.export_service = ExportService(config, self.chart_service)

        generator = HuggingFaceSQLGenerator(config)

        self.intent_agent = IntentAgent()
        self.planning_agent = PlanningAgent()
        self.schema_agent = SchemaAgent(self.metadata_service)
        self.sql_generation_agent = SQLGenerationAgent(generator)
        self.sql_validation_agent = SQLValidationAgent(SQLValidator())
        self.execution_agent = ExecutionAgent(
            self.database,
            preview_limit=config.preview_row_limit,
            export_limit=config.export_row_limit,
        )
        self.pattern_agent = PatternDiscoveryAgent(
            AnalyticsService(), self.chart_service
        )
        self.report_agent = ReportAgent(generator)
        self._active_table_filter: set[str] | None = None

    def set_active_blueprint(self, blueprint: SchemaBlueprint | None) -> None:
        """Restrict schema context to the tables created by this blueprint."""
        if blueprint is None:
            self._active_table_filter = None
        else:
            tables = {blueprint.fact_table} | set(blueprint.dim_tables.keys())
            self._active_table_filter = tables

    def analyze(self, request: AnalysisRequest) -> AnalysisResponse:
        state = AgentState(question=request.question, session_id=request.session_id)
        state = self.intent_agent.run(state)
        state = self.planning_agent.run(state)
        state.glossary = self.metadata_service.get_business_glossary()
        state.schema_context = self.metadata_service.build_schema_summary(
            state.question, table_filter=self._active_table_filter
        )

        execution_error: str | None = None
        attempts = self.config.max_generation_retries + 1
        for _ in range(attempts):
            state = self.sql_generation_agent.run(state, error_feedback=execution_error)
            state = self.sql_validation_agent.run(state)

            if not state.validation or not state.validation.is_valid:
                execution_error = (
                    "; ".join(state.validation.errors)
                    if state.validation
                    else "SQL validation failed."
                )
                state.warnings.append(execution_error)
                continue

            try:
                state = self.execution_agent.run(state)
                break
            except Exception as exc:
                execution_error = str(exc)
                state.warnings.append(f"Execution attempt failed: {execution_error}")
        else:
            # All retries exhausted — return partial response so the UI can show what failed
            last_sql = state.sql_candidates[-1].sql if state.sql_candidates else ""
            state.warnings.append(
                "All retry attempts exhausted. The SQL above could not execute. "
                "Try rephrasing your question or check the error details in warnings."
            )
            logger.warning("All retries failed. Last SQL:\n%s", last_sql)
            return AnalysisResponse(
                answer_markdown="",
                approved_sql=last_sql,
                preview_rows=[],
                chart_spec=None,
                downloads=DownloadArtifacts(),
                warnings=state.warnings,
                follow_up_questions=[],
                needs_summary=False,
                result_df=pd.DataFrame(),
                analysis_plan=state.analysis_plan,
            )

        state = self.pattern_agent.run(state)
        state = self.report_agent.run(state)
        state.artifacts = self.export_service.export_analysis(
            question=state.question,
            approved_sql=state.approved_sql or "",
            dataframe=state.result_df,
            insights=state.insights,
            chart_spec=state.chart_spec,
        )

        try:
            state.session_id = self.database.save_session(
                question=state.question,
                approved_sql=state.approved_sql or "",
                row_count=len(state.result_df.index),
                chart_type=state.chart_spec.chart_type if state.chart_spec else None,
                artifacts=state.artifacts,
                warnings=state.warnings,
                session_id=state.session_id,
            )
        except Exception as exc:
            logger.warning("Failed to save session: %s", exc)

        return AnalysisResponse(
            answer_markdown=state.model_debug.get("answer_markdown", ""),
            approved_sql=state.approved_sql or "",
            preview_rows=(
                state.preview_df.to_dict("records")
                if state.preview_df is not None
                else []
            ),
            chart_spec=state.chart_spec,
            downloads=state.artifacts,
            warnings=state.warnings,
            follow_up_questions=state.follow_up_questions,
            needs_summary=state.needs_summary,
            result_df=state.result_df,
            analysis_plan=state.analysis_plan,
        )
