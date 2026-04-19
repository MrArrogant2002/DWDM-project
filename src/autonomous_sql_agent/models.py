from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AnalysisRequest:
    question: str
    session_id: str | None = None


@dataclass(slots=True)
class ChartSpec:
    chart_type: str
    x_field: str | None = None
    y_field: str | None = None
    group_field: str | None = None
    title: str | None = None


@dataclass(slots=True)
class DownloadArtifacts:
    csv_path: str | None = None
    xlsx_path: str | None = None
    pdf_path: str | None = None


@dataclass(slots=True)
class SQLCandidate:
    sql: str
    analysis_goal: str
    tables_used: list[str] = field(default_factory=list)
    chart_hint: str | None = None
    generator: str = "huggingface"


@dataclass(slots=True)
class ValidationResult:
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    normalized_sql: str | None = None


@dataclass(slots=True)
class AgentState:
    question: str
    session_id: str | None = None
    intent: str | None = None
    needs_summary: bool = False
    analysis_plan: list[str] = field(default_factory=list)
    schema_context: str = ""
    glossary: dict[str, str] = field(default_factory=dict)
    sql_candidates: list[SQLCandidate] = field(default_factory=list)
    approved_sql: str | None = None
    validation: ValidationResult | None = None
    result_df: Any = None
    preview_df: Any = None
    insights: list[str] = field(default_factory=list)
    chart_spec: ChartSpec | None = None
    warnings: list[str] = field(default_factory=list)
    follow_up_questions: list[str] = field(default_factory=list)
    artifacts: DownloadArtifacts = field(default_factory=DownloadArtifacts)
    model_debug: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AnalysisResponse:
    answer_markdown: str
    approved_sql: str
    preview_rows: list[dict[str, Any]]
    chart_spec: ChartSpec | None
    downloads: DownloadArtifacts
    warnings: list[str]
    follow_up_questions: list[str]
    needs_summary: bool = False
    result_df: Any = None
    analysis_plan: list[str] = field(default_factory=list)
