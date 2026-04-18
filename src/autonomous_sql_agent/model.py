from __future__ import annotations

import ast
import json
import re

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.logging_utils import get_logger
from autonomous_sql_agent.models import SQLCandidate
from autonomous_sql_agent.prompts import build_sql_prompt

logger = get_logger(__name__)


class HuggingFaceSQLGenerator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._tokenizer = None
        self._model = None
        self._load_error: str | None = None

    def generate_candidate(
        self,
        question: str,
        analysis_plan: list[str],
        schema_context: str,
        glossary: dict[str, str],
        error_feedback: str | None = None,
    ) -> SQLCandidate:
        if self._load_error is None and self._model is None:
            self._load_model()

        prompt = build_sql_prompt(
            question=question if not error_feedback else f"{question}\nPrevious failure: {error_feedback}",
            analysis_plan=analysis_plan,
            schema_context=schema_context,
            glossary=glossary,
        )

        if self._model is None or self._tokenizer is None:
            return self._fallback_candidate(question)

        try:
            payload = self._run_model(prompt)
            return SQLCandidate(
                sql=payload["sql"].strip(),
                analysis_goal=payload.get("analysis_goal", "Answer the business question."),
                tables_used=list(payload.get("tables_used", [])),
                chart_hint=payload.get("chart_hint"),
                generator="huggingface",
            )
        except Exception as exc:  # pragma: no cover - depends on large-model runtime
            logger.warning("Falling back to rule-based SQL generation because model inference failed: %s", exc)
            return self._fallback_candidate(question)

    def _load_model(self) -> None:
        try:
            from transformers import AutoModelForCausalLM, AutoTokenizer
        except ImportError as exc:  # pragma: no cover - depends on optional runtime deps
            self._load_error = str(exc)
            logger.warning("Transformers is unavailable. The app will use fallback SQL generation.")
            return

        kwargs = {"trust_remote_code": True}
        if self.config.device == "auto":
            kwargs["device_map"] = "auto"

        try:
            self._tokenizer = AutoTokenizer.from_pretrained(self.config.hf_model_id)
            self._model = AutoModelForCausalLM.from_pretrained(self.config.hf_model_id, **kwargs)
        except Exception as exc:  # pragma: no cover - depends on runtime model availability
            self._load_error = str(exc)
            logger.warning("Unable to load Hugging Face model `%s`: %s", self.config.hf_model_id, exc)
            self._tokenizer = None
            self._model = None

    def _run_model(self, prompt: str) -> dict[str, object]:
        inputs = self._tokenizer(prompt, return_tensors="pt")
        output = self._model.generate(
            **inputs,
            max_new_tokens=512,
            do_sample=False,
        )
        generated = self._tokenizer.decode(output[0], skip_special_tokens=True)
        if generated.startswith(prompt):
            generated = generated[len(prompt) :]
        payload = self._parse_payload(generated)
        if "sql" not in payload:
            raise ValueError("Model output did not contain a `sql` field.")
        return payload

    def _parse_payload(self, raw_text: str) -> dict[str, object]:
        json_match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
        if not json_match:
            raise ValueError(f"Could not find a JSON object in the model output: {raw_text[:200]}")

        candidate = json_match.group(0)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return ast.literal_eval(candidate)

    def _fallback_candidate(self, question: str) -> SQLCandidate:
        question_lower = question.lower()
        if "top" in question_lower and "product" in question_lower:
            return SQLCandidate(
                sql="""
                SELECT
                    p.product_name,
                    p.category,
                    SUM(oi.net_amount) AS revenue
                FROM fact_order_items oi
                JOIN dim_product p ON p.product_id = oi.product_id
                GROUP BY p.product_name, p.category
                ORDER BY revenue DESC
                LIMIT 10
                """,
                analysis_goal="Find the highest-revenue products.",
                tables_used=["fact_order_items", "dim_product"],
                chart_hint="bar",
                generator="rule_based_fallback",
            )

        if any(word in question_lower for word in ("monthly", "trend", "over time")):
            return SQLCandidate(
                sql="""
                SELECT
                    d.year,
                    d.month,
                    d.month_name,
                    SUM(o.total_amount) AS revenue
                FROM fact_orders o
                JOIN dim_date d ON d.date_id = o.order_date_id
                WHERE o.order_status <> 'cancelled'
                GROUP BY d.year, d.month, d.month_name
                ORDER BY d.year, d.month
                """,
                analysis_goal="Show the sales trend over time.",
                tables_used=["fact_orders", "dim_date"],
                chart_hint="line",
                generator="rule_based_fallback",
            )

        if "return" in question_lower and "region" in question_lower:
            return SQLCandidate(
                sql="""
                SELECT
                    r.region_name,
                    COUNT(fr.return_id) AS return_count,
                    SUM(fr.return_amount) AS return_amount
                FROM fact_returns fr
                JOIN fact_orders o ON o.order_id = fr.order_id
                JOIN dim_region r ON r.region_id = o.region_id
                GROUP BY r.region_name
                ORDER BY return_amount DESC
                """,
                analysis_goal="Compare return activity by region.",
                tables_used=["fact_returns", "fact_orders", "dim_region"],
                chart_hint="bar",
                generator="rule_based_fallback",
            )

        if any(word in question_lower for word in ("segment", "customer", "cluster")):
            return SQLCandidate(
                sql="""
                SELECT
                    c.segment,
                    c.loyalty_tier,
                    COUNT(DISTINCT o.order_id) AS orders,
                    SUM(o.total_amount) AS revenue,
                    AVG(o.total_amount) AS avg_order_value
                FROM fact_orders o
                JOIN dim_customer c ON c.customer_id = o.customer_id
                WHERE o.order_status <> 'cancelled'
                GROUP BY c.segment, c.loyalty_tier
                ORDER BY revenue DESC
                """,
                analysis_goal="Summarize customer segments for follow-up pattern discovery.",
                tables_used=["fact_orders", "dim_customer"],
                chart_hint="bar",
                generator="rule_based_fallback",
            )

        return SQLCandidate(
            sql="""
            SELECT
                p.category,
                SUM(oi.net_amount) AS revenue,
                SUM(oi.quantity) AS units_sold
            FROM fact_order_items oi
            JOIN dim_product p ON p.product_id = oi.product_id
            GROUP BY p.category
            ORDER BY revenue DESC
            """,
            analysis_goal="Provide a high-level revenue view by category.",
            tables_used=["fact_order_items", "dim_product"],
            chart_hint="bar",
            generator="rule_based_fallback",
        )
