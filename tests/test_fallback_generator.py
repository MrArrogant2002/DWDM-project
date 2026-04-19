from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.model import HuggingFaceSQLGenerator


def _fallback_config() -> AppConfig:
    """Return a config that forces the rule-based fallback (no token, no model)."""
    cfg = AppConfig.from_env()
    return AppConfig(
        database_url=cfg.database_url,
        hf_model_id=cfg.hf_model_id,
        hf_token=None,
        hf_inference_model=cfg.hf_inference_model,
        device=cfg.device,
        statement_timeout_ms=cfg.statement_timeout_ms,
        export_dir=cfg.export_dir,
        preview_row_limit=cfg.preview_row_limit,
        export_row_limit=cfg.export_row_limit,
        max_generation_retries=cfg.max_generation_retries,
        default_order_count=cfg.default_order_count,
        use_fallback_only=True,
        project_root=cfg.project_root,
        data_dir=cfg.data_dir,
        docs_dir=cfg.docs_dir,
    )


class FallbackGeneratorTests(unittest.TestCase):
    def test_returns_product_ranking_sql_when_model_is_unavailable(self) -> None:
        generator = HuggingFaceSQLGenerator(_fallback_config())
        candidate = generator.generate_candidate(
            question="Show the top 10 products by revenue.",
            analysis_plan=["Rank products by revenue."],
            schema_context="- fact_orders: order_id, product_name, category, total_amount",
            glossary={"revenue": "Use total_amount."},
        )
        sql_lower = candidate.sql.lower()
        self.assertIn("select", sql_lower)
        self.assertIn("limit", sql_lower)
        self.assertEqual(candidate.chart_hint, "bar")
        self.assertEqual(candidate.generator, "rule_based_fallback")

    def test_returns_trend_sql_for_monthly_question(self) -> None:
        generator = HuggingFaceSQLGenerator(_fallback_config())
        candidate = generator.generate_candidate(
            question="Show the monthly sales trend.",
            analysis_plan=["Group by month and sum revenue."],
            schema_context="- fact_orders: order_id, order_date, total_amount",
            glossary={},
        )
        self.assertIn("select", candidate.sql.lower())
        self.assertEqual(candidate.chart_hint, "line")
        self.assertEqual(candidate.generator, "rule_based_fallback")


if __name__ == "__main__":
    unittest.main()
