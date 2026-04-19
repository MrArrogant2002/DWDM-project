from __future__ import annotations

import pandas as pd

from autonomous_sql_agent.analytics import AnalyticsService
from autonomous_sql_agent.charting import ChartService
from autonomous_sql_agent.csv_ingestion import needs_summary as _detect_needs_summary
from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.metadata import SchemaMetadataService
from autonomous_sql_agent.model import HuggingFaceSQLGenerator
from autonomous_sql_agent.models import AgentState
from autonomous_sql_agent.sql_validation import SQLValidator


class IntentAgent:
    def run(self, state: AgentState) -> AgentState:
        q = state.question.lower()

        # Detect summarization intent first
        state.needs_summary = _detect_needs_summary(state.question)

        if any(
            w in q for w in ("anomaly", "outlier", "unusual", "spike", "drop", "weird")
        ):
            state.intent = "anomaly"
        elif any(
            w in q
            for w in (
                "trend",
                "monthly",
                "quarter",
                "over time",
                "by month",
                "per month",
            )
        ):
            state.intent = "trend"
        elif any(
            w in q
            for w in ("segment", "cluster", "group of customers", "customer type")
        ):
            state.intent = "segmentation"
        elif any(w in q for w in ("compare", " vs ", "versus", "difference between")):
            state.intent = "comparison"
        else:
            state.intent = "summary"

        return state


class PlanningAgent:
    def run(self, state: AgentState) -> AgentState:
        plan = [
            "Understand the business question and identify the relevant tables.",
            "Select appropriate columns, aggregations, and filters.",
            "Generate a SQLite SELECT query with correct joins and groupings.",
            "Validate and execute the query, then discover patterns.",
        ]
        if state.intent == "anomaly":
            plan.append("Highlight statistical outliers in the result.")
        elif state.intent == "trend":
            plan.append("Preserve time ordering and measure directional movement.")
        elif state.intent == "segmentation":
            plan.append("Group entities and compare key metrics across segments.")
        elif state.intent == "comparison":
            plan.append("Compute side-by-side metrics for the compared entities.")

        if state.needs_summary:
            plan.append("Generate a plain-English summary of the findings.")

        state.analysis_plan = plan
        return state


class SchemaAgent:
    def __init__(self, metadata_service: SchemaMetadataService) -> None:
        self.metadata_service = metadata_service

    def run(self, state: AgentState) -> AgentState:
        state.glossary = self.metadata_service.get_business_glossary()
        state.schema_context = self.metadata_service.build_schema_summary(
            state.question
        )
        return state


class SQLGenerationAgent:
    def __init__(self, generator: HuggingFaceSQLGenerator) -> None:
        self.generator = generator

    def run(self, state: AgentState, error_feedback: str | None = None) -> AgentState:
        candidate = self.generator.generate_candidate(
            question=state.question,
            analysis_plan=state.analysis_plan,
            schema_context=state.schema_context,
            glossary=state.glossary,
            error_feedback=error_feedback,
        )
        state.sql_candidates.append(candidate)
        return state


class SQLValidationAgent:
    def __init__(self, validator: SQLValidator) -> None:
        self.validator = validator

    def run(self, state: AgentState) -> AgentState:
        candidate = state.sql_candidates[-1]
        validation = self.validator.validate(candidate.sql)
        state.validation = validation
        state.warnings.extend(validation.warnings)
        if validation.is_valid and validation.normalized_sql:
            state.approved_sql = validation.normalized_sql
        else:
            state.approved_sql = None
        return state


class ExecutionAgent:
    def __init__(
        self, database: DatabaseManager, preview_limit: int, export_limit: int
    ) -> None:
        self.database = database
        self.preview_limit = preview_limit
        self.export_limit = export_limit

    def run(self, state: AgentState) -> AgentState:
        if not state.approved_sql:
            raise ValueError("No validated SQL available for execution.")

        query_plan = self.database.explain_query(state.approved_sql)
        state.model_debug["query_plan"] = query_plan

        result_df = self.database.query_dataframe(
            state.approved_sql, limit=self.export_limit + 1
        )
        if len(result_df) > self.export_limit:
            state.warnings.append(
                f"Query returned more than {self.export_limit} rows; "
                f"only the first {self.export_limit} are kept."
            )
            result_df = result_df.head(self.export_limit)

        state.result_df = result_df
        state.preview_df = result_df.head(self.preview_limit)
        return state


class PatternDiscoveryAgent:
    def __init__(
        self, analytics_service: AnalyticsService, chart_service: ChartService
    ) -> None:
        self.analytics_service = analytics_service
        self.chart_service = chart_service

    def run(self, state: AgentState) -> AgentState:
        df = (
            state.result_df
            if isinstance(state.result_df, pd.DataFrame)
            else pd.DataFrame()
        )
        insights, follow_ups = self.analytics_service.analyze(df, state.intent)
        state.insights = insights
        state.follow_up_questions = follow_ups
        chart_hint = (
            state.sql_candidates[-1].chart_hint if state.sql_candidates else None
        )
        state.chart_spec = self.chart_service.infer_chart_spec(
            df, chart_hint=chart_hint, question=state.question
        )
        return state


class ReportAgent:
    """Generates answer_markdown only when needs_summary is True."""

    def __init__(self, generator: HuggingFaceSQLGenerator) -> None:
        self.generator = generator

    def run(self, state: AgentState) -> AgentState:
        if not state.needs_summary:
            state.model_debug["answer_markdown"] = ""
            return state

        df = (
            state.result_df
            if isinstance(state.result_df, pd.DataFrame)
            else pd.DataFrame()
        )
        data_preview = (
            df.head(10).to_string(index=False) if not df.empty else "(no data)"
        )

        # Try HF API summary first
        summary = self.generator.generate_summary(
            question=state.question,
            data_preview=data_preview,
            row_count=len(df),
        )

        # Fall back to insight-based summary if API unavailable
        if not summary and state.insights:
            summary_line = state.insights[0]
            detail = "\n".join(f"- {i}" for i in state.insights[1:])
            summary = f"{summary_line}\n\n{detail}" if detail else summary_line

        state.model_debug["answer_markdown"] = summary or "Analysis completed."
        return state
