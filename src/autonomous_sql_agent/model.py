from __future__ import annotations

import ast
import json
import re

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.logging_utils import get_logger
from autonomous_sql_agent.models import SQLCandidate
from autonomous_sql_agent.prompts import build_sql_messages, build_summary_messages

logger = get_logger(__name__)


class HuggingFaceSQLGenerator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._client = None
        self._client_error: str | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_candidate(
        self,
        question: str,
        analysis_plan: list[str],
        schema_context: str,
        glossary: dict[str, str],
        error_feedback: str | None = None,
    ) -> SQLCandidate:
        if self.config.use_fallback_only or not self.config.hf_token:
            return self._fallback_candidate(question, schema_context)

        if self._client_error is None and self._client is None:
            self._init_client()

        if self._client is None:
            return self._fallback_candidate(question, schema_context)

        effective_question = (
            question
            if not error_feedback
            else f"{question}\nPrevious error: {error_feedback}"
        )
        messages = build_sql_messages(effective_question, analysis_plan, schema_context)

        try:
            payload = self._call_api(messages)
            return SQLCandidate(
                sql=payload["sql"].strip(),
                analysis_goal=payload.get("analysis_goal", "Answer the question."),
                tables_used=list(payload.get("tables_used", [])),
                chart_hint=payload.get("chart_hint"),
                generator=self.config.hf_inference_model,
            )
        except Exception as exc:
            logger.warning("HF API inference failed, using fallback: %s", exc)
            return self._fallback_candidate(question, schema_context)

    def generate_summary(self, question: str, data_preview: str, row_count: int) -> str:
        """Generate a natural-language summary of results. Only called when needs_summary=True."""
        if self.config.use_fallback_only or not self.config.hf_token:
            return ""
        if self._client_error is None and self._client is None:
            self._init_client()
        if self._client is None:
            return ""

        messages = build_summary_messages(question, data_preview, row_count)
        try:
            response = self._client.chat_completion(
                model=self.config.hf_inference_model,
                messages=messages,
                max_tokens=512,
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            logger.warning("Summary generation failed: %s", exc)
            return ""

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _init_client(self) -> None:
        try:
            from huggingface_hub import InferenceClient
        except ImportError as exc:
            self._client_error = str(exc)
            logger.warning("huggingface_hub is not installed: %s", exc)
            return
        try:
            self._client = InferenceClient(token=self.config.hf_token)
            logger.info(
                "HF InferenceClient initialised (model: %s)",
                self.config.hf_inference_model,
            )
        except Exception as exc:
            self._client_error = str(exc)
            logger.warning("Failed to create HF InferenceClient: %s", exc)

    def _call_api(self, messages: list[dict[str, str]]) -> dict[str, object]:
        response = self._client.chat_completion(
            model=self.config.hf_inference_model,
            messages=messages,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content
        return self._parse_json(raw)

    @staticmethod
    def _parse_json(raw: str) -> dict[str, object]:
        # Strip markdown fences if present
        clean = (
            re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE)
            .strip()
            .strip("`")
            .strip()
        )
        # Try direct parse first
        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            pass
        # Try extracting the first {...} block
        match = re.search(r"\{.*\}", clean, flags=re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                try:
                    return ast.literal_eval(match.group(0))
                except Exception:
                    pass
        raise ValueError(f"Cannot parse model output as JSON:\n{raw[:400]}")

    # ------------------------------------------------------------------
    # Rule-based fallback (works with any schema, uses the fact table)
    # ------------------------------------------------------------------

    def _fallback_candidate(self, question: str, schema_context: str) -> SQLCandidate:
        fact_table = self._infer_fact_table(schema_context)
        q = question.lower()

        if "top" in q and ("product" in q or "item" in q):
            return self._candidate_top_products(fact_table, schema_context)
        if any(
            w in q for w in ("monthly", "trend", "over time", "by month", "per month")
        ):
            return self._candidate_trend(fact_table, schema_context)
        if "return" in q and "region" in q:
            return self._candidate_returns_by_region(fact_table, schema_context)
        if any(w in q for w in ("segment", "customer", "cluster", "group")):
            return self._candidate_segments(fact_table, schema_context)
        # Generic fallback: aggregate numeric columns by the first dimension
        return self._candidate_generic(fact_table, schema_context)

    @staticmethod
    def _infer_fact_table(schema_context: str) -> str:
        """Prefer tables with a _fact suffix; fall back to the first table in the schema."""
        # Pass 1: explicit _fact suffix (uploaded tables)
        for line in schema_context.splitlines():
            m = re.match(r"-\s*(\w+_fact)\s*:", line)
            if m:
                return m.group(1)
        # Pass 2: tables named fact_* (old seeded warehouse style)
        for line in schema_context.splitlines():
            m = re.match(r"-\s*(fact_\w+)\s*:", line)
            if m:
                return m.group(1)
        # Pass 3: any first table
        for line in schema_context.splitlines():
            m = re.match(r"-\s*(\w+)\s*:", line)
            if m:
                return m.group(1)
        return "fact_orders"

    @staticmethod
    def _first_col(schema_context: str, roles: tuple[str, ...]) -> str | None:
        """Find the first column name hinting at a given role."""
        hints = {
            "revenue": (
                "revenue",
                "total_amount",
                "net_amount",
                "amount",
                "sales",
                "price",
            ),
            "date": ("date", "month", "year", "day", "time", "created_at"),
            "category": (
                "category",
                "product",
                "region",
                "segment",
                "channel",
                "status",
                "type",
            ),
        }
        for role in roles:
            for hint in hints.get(role, ()):
                if hint in schema_context.lower():
                    # try to find exact column name in schema lines
                    for line in schema_context.splitlines():
                        for token in re.split(r"[\s,|()]+", line):
                            if hint in token.lower():
                                clean = token.strip().rstrip(",")
                                if re.match(r"^\w+$", clean):
                                    return clean
        return None

    def _candidate_top_products(
        self, fact_table: str, schema_context: str
    ) -> SQLCandidate:
        rev = self._first_col(schema_context, ("revenue",)) or "total_amount"
        cat = self._first_col(schema_context, ("category",)) or "product_name"
        sql = f"""
SELECT {cat}, SUM({rev}) AS total_revenue
FROM {fact_table}
GROUP BY {cat}
ORDER BY total_revenue DESC
LIMIT 10
""".strip()
        return SQLCandidate(
            sql=sql,
            analysis_goal="Top items by revenue.",
            chart_hint="bar",
            tables_used=[fact_table],
            generator="rule_based_fallback",
        )

    def _candidate_trend(self, fact_table: str, schema_context: str) -> SQLCandidate:
        rev = self._first_col(schema_context, ("revenue",)) or "total_amount"
        dt = self._first_col(schema_context, ("date",)) or "order_date"
        sql = f"""
SELECT {dt}, SUM({rev}) AS total_revenue
FROM {fact_table}
GROUP BY {dt}
ORDER BY {dt}
""".strip()
        return SQLCandidate(
            sql=sql,
            analysis_goal="Sales trend over time.",
            chart_hint="line",
            tables_used=[fact_table],
            generator="rule_based_fallback",
        )

    def _candidate_returns_by_region(
        self, fact_table: str, schema_context: str
    ) -> SQLCandidate:
        cat = self._first_col(schema_context, ("category",)) or "region"
        sql = f"""
SELECT {cat}, COUNT(*) AS return_count
FROM {fact_table}
GROUP BY {cat}
ORDER BY return_count DESC
""".strip()
        return SQLCandidate(
            sql=sql,
            analysis_goal="Return activity by region.",
            chart_hint="bar",
            tables_used=[fact_table],
            generator="rule_based_fallback",
        )

    def _candidate_segments(self, fact_table: str, schema_context: str) -> SQLCandidate:
        cat = self._first_col(schema_context, ("category",)) or "segment"
        rev = self._first_col(schema_context, ("revenue",)) or "total_amount"
        sql = f"""
SELECT {cat}, COUNT(*) AS orders, SUM({rev}) AS revenue, AVG({rev}) AS avg_value
FROM {fact_table}
GROUP BY {cat}
ORDER BY revenue DESC
""".strip()
        return SQLCandidate(
            sql=sql,
            analysis_goal="Customer or segment breakdown.",
            chart_hint="bar",
            tables_used=[fact_table],
            generator="rule_based_fallback",
        )

    def _candidate_generic(self, fact_table: str, schema_context: str) -> SQLCandidate:
        cat = self._first_col(schema_context, ("category",)) or "category"
        rev = self._first_col(schema_context, ("revenue",)) or "total_amount"
        sql = f"""
SELECT {cat}, SUM({rev}) AS total, COUNT(*) AS records
FROM {fact_table}
GROUP BY {cat}
ORDER BY total DESC
""".strip()
        return SQLCandidate(
            sql=sql,
            analysis_goal="General aggregation.",
            chart_hint="bar",
            tables_used=[fact_table],
            generator="rule_based_fallback",
        )
