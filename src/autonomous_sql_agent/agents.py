from __future__ import annotations

import pandas as pd

from autonomous_sql_agent.analytics import AnalyticsService
from autonomous_sql_agent.charting import ChartService
from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.metadata import SchemaMetadataService
from autonomous_sql_agent.model import HuggingFaceSQLGenerator
from autonomous_sql_agent.models import AgentState
from autonomous_sql_agent.sql_validation import SQLValidator


class IntentAgent:
    def run(self, state: AgentState) -> AgentState:
        question = state.question.lower()
        if any(word in question for word in ("anomaly", "outlier", "unusual", "spike", "drop")):
            state.intent = "anomaly"
        elif any(word in question for word in ("trend", "monthly", "quarter", "over time")):
            state.intent = "trend"
        elif any(word in question for word in ("segment", "cluster", "group of customers")):
            state.intent = "segmentation"
        elif any(word in question for word in ("compare", "vs", "versus")):
            state.intent = "comparison"
        else:
            state.intent = "summary"
        return state


class PlanningAgent:
    def run(self, state: AgentState) -> AgentState:
        plan = [
            "Interpret the business question in the retail/e-commerce warehouse context.",
            "Select the fact and dimension tables needed for the analysis.",
            "Generate a PostgreSQL SELECT query with business-safe joins and aggregations.",
            "Validate the SQL, execute it, and discover patterns in the result.",
        ]
        if state.intent == "anomaly":
            plan.append("Prioritize anomaly signals in the final explanation.")
        elif state.intent == "trend":
            plan.append("Preserve time ordering and evaluate directional movement over the selected periods.")
        elif state.intent == "segmentation":
            plan.append("Summarize customer groups or comparable entities for follow-up clustering.")
        state.analysis_plan = plan
        return state


class SchemaAgent:
    def __init__(self, metadata_service: SchemaMetadataService) -> None:
        self.metadata_service = metadata_service

    def run(self, state: AgentState) -> AgentState:
        state.glossary = self.metadata_service.get_business_glossary()
        state.schema_context = self.metadata_service.build_schema_summary(state.question)
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
    def __init__(self, database: DatabaseManager, preview_limit: int, export_limit: int) -> None:
        self.database = database
        self.preview_limit = preview_limit
        self.export_limit = export_limit

    def run(self, state: AgentState) -> AgentState:
        if not state.approved_sql:
            raise ValueError("No validated SQL is available for execution.")

        query_plan = self.database.explain_query(state.approved_sql)
        state.model_debug["query_plan"] = query_plan

        result_df = self.database.query_dataframe(state.approved_sql, limit=self.export_limit + 1)
        if len(result_df) > self.export_limit:
            state.warnings.append(
                f"The query returned more than {self.export_limit} rows; only the first {self.export_limit} are kept for export."
            )
            result_df = result_df.head(self.export_limit)

        state.result_df = result_df
        state.preview_df = result_df.head(self.preview_limit)
        return state


class PatternDiscoveryAgent:
    def __init__(self, analytics_service: AnalyticsService, chart_service: ChartService) -> None:
        self.analytics_service = analytics_service
        self.chart_service = chart_service

    def run(self, state: AgentState) -> AgentState:
        result_df = state.result_df if isinstance(state.result_df, pd.DataFrame) else pd.DataFrame()
        insights, follow_ups = self.analytics_service.analyze(result_df, state.intent)
        state.insights = insights
        state.follow_up_questions = follow_ups
        chart_hint = state.sql_candidates[-1].chart_hint if state.sql_candidates else None
        state.chart_spec = self.chart_service.infer_chart_spec(result_df, chart_hint=chart_hint, question=state.question)
        return state


class ReportAgent:
    def run(self, state: AgentState) -> AgentState:
        summary_line = state.insights[0] if state.insights else "The analysis completed successfully."
        detail_lines = "\n".join(f"- {insight}" for insight in state.insights[1:])
        answer = f"### Result\n{summary_line}\n"
        if detail_lines:
            answer += f"\n### Patterns\n{detail_lines}\n"
        if state.follow_up_questions:
            answer += "\n### Follow-up Ideas\n"
            answer += "\n".join(f"- {item}" for item in state.follow_up_questions)
        state.model_debug["answer_markdown"] = answer
        return state
