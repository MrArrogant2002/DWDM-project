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

        if error_feedback:
            valid_tables = self._extract_table_names(schema_context)
            fact = self._infer_fact_table(schema_context)
            schema_cols = self._parse_schema_columns(schema_context)
            fact_cols = [c for c, _ in schema_cols.get(fact, [])]
            parts: list[str] = [f"{question}\nPrevious SQL error: {error_feedback}"]
            if valid_tables:
                parts.append(f"Valid tables: {', '.join(valid_tables)}.")
            if fact_cols:
                parts.append(f"Columns in {fact}: {', '.join(fact_cols)}.")
            parts.append(
                "Use ONLY the exact table and column names listed above — do NOT invent names."
            )
            effective_question = "\n".join(parts)
        else:
            effective_question = question
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
            max_tokens=2048,  # 1024 caused mid-JSON truncation on complex schemas
        )
        raw = response.choices[0].message.content
        return self._parse_json(raw)

    @staticmethod
    def _parse_json(raw: str) -> dict[str, object]:
        # Strip ANSI escape sequences some HF models emit (e.g. \x1b[4m, \x1b[0m)
        raw = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", raw)
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
        # Last resort: extract the sql field from a truncated/malformed JSON response.
        # Models sometimes hit max_tokens mid-JSON — the SQL is almost always present.
        sql_match = re.search(r'"sql"\s*:\s*"((?:[^"\\]|\\.)*)"', clean, re.DOTALL)
        if sql_match:
            logger.warning(
                "Model JSON was malformed/truncated; extracted sql field directly."
            )
            return {
                "sql": sql_match.group(1).replace("\\n", "\n").replace('\\"', '"'),
                "analysis_goal": "Answer the question.",
                "tables_used": [],
                "chart_hint": None,
                "needs_summary": False,
            }
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
    def _extract_table_names(schema_context: str) -> list[str]:
        tables: list[str] = []
        for line in schema_context.splitlines():
            m = re.match(r"-\s*(\w+)\s*:", line)
            if m:
                tables.append(m.group(1))
        return tables

    @staticmethod
    def _parse_schema_columns(schema_context: str) -> dict[str, list[tuple[str, str]]]:
        """Return {table_name: [(col_name, col_type_lower), ...]} parsed from schema text."""
        result: dict[str, list[tuple[str, str]]] = {}
        for line in schema_context.splitlines():
            m = re.match(r"-\s*(\w+)\s*:(.*)", line)
            if not m:
                continue
            table_name = m.group(1)
            col_section = m.group(2).split("|")[0]
            cols: list[tuple[str, str]] = []
            for cm in re.finditer(r"(\w+)\s*\(([^)]*)\)", col_section):
                cols.append((cm.group(1), cm.group(2).lower()))
            if cols:
                result[table_name] = cols
        return result

    @classmethod
    def _find_date_col(cls, fact_table: str, schema_context: str) -> str | None:
        """Find the best date column in fact_table: TIMESTAMP type first, then name hints."""
        schema = cls._parse_schema_columns(schema_context)
        cols = schema.get(fact_table, [])
        for col, typ in cols:
            if any(t in typ for t in ("timestamp", "datetime", "date")):
                return col
        for col, _ in cols:
            if any(
                h in col.lower()
                for h in ("date", "time", "month", "year", "day", "week", "period")
            ):
                return col
        return None

    @classmethod
    def _find_metric_col(cls, fact_table: str, schema_context: str) -> str | None:
        """Find the best numeric revenue/metric column: name hints first, then REAL type."""
        schema = cls._parse_schema_columns(schema_context)
        cols = schema.get(fact_table, [])
        _id_hints = ("_id", "_key", "uuid", "sku")
        for col, _ in cols:
            if any(
                h in col.lower()
                for h in (
                    "revenue",
                    "total",
                    "amount",
                    "sales",
                    "price",
                    "income",
                    "value",
                    "cost",
                    "profit",
                    "turnover",
                )
            ):
                return col
        for col, typ in cols:
            if any(t in typ for t in ("real", "float", "numeric", "double", "decimal")):
                if not any(h in col.lower() for h in _id_hints):
                    return col
        for col, typ in cols:
            if "int" in typ and not any(h in col.lower() for h in _id_hints):
                return col
        return None

    @classmethod
    def _find_dim_col(cls, fact_table: str, schema_context: str) -> str | None:
        """Find the best categorical dimension column by name hints then TEXT type."""
        schema = cls._parse_schema_columns(schema_context)
        cols = schema.get(fact_table, [])
        _id_hints = ("_id", "_key", "uuid", "sku")
        for col, _ in cols:
            if any(
                h in col.lower()
                for h in (
                    "category",
                    "product",
                    "region",
                    "segment",
                    "channel",
                    "type",
                    "status",
                    "brand",
                    "dept",
                    "city",
                    "state",
                    "country",
                    "gender",
                    "payment",
                )
            ):
                return col
        for col, typ in cols:
            if "text" in typ and not any(h in col.lower() for h in _id_hints):
                return col
        return None

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
        rev = self._find_metric_col(fact_table, schema_context) or "total_amount"
        cat = self._find_dim_col(fact_table, schema_context) or "product_name"
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
        rev = self._find_metric_col(fact_table, schema_context) or "total_amount"
        dt = self._find_date_col(fact_table, schema_context) or "order_date"
        # Always aggregate to month buckets — using the raw date column returns one
        # row per transaction and produces a meaningless spaghetti line chart.
        sql = f"""
SELECT strftime('%Y-%m', {dt}) AS month, SUM({rev}) AS total_revenue
FROM {fact_table}
WHERE {dt} IS NOT NULL
GROUP BY month
ORDER BY month
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
        cat = self._find_dim_col(fact_table, schema_context) or "region"
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
        cat = self._find_dim_col(fact_table, schema_context) or "segment"
        rev = self._find_metric_col(fact_table, schema_context) or "total_amount"
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
        cat = self._find_dim_col(fact_table, schema_context) or "category"
        rev = self._find_metric_col(fact_table, schema_context) or "total_amount"
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
