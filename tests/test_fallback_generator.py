from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.model import HuggingFaceSQLGenerator


class FallbackGeneratorTests(unittest.TestCase):
    def test_returns_product_ranking_sql_when_model_is_unavailable(self) -> None:
        generator = HuggingFaceSQLGenerator(AppConfig.from_env())
        generator._load_error = "simulate missing runtime"
        candidate = generator.generate_candidate(
            question="Show the top 10 products by revenue.",
            analysis_plan=["Rank products by revenue."],
            schema_context="- fact_order_items ...",
            glossary={"revenue": "Use net_amount."},
        )
        self.assertIn("dim_product", candidate.sql)
        self.assertEqual(candidate.chart_hint, "bar")
        self.assertEqual(candidate.generator, "rule_based_fallback")


if __name__ == "__main__":
    unittest.main()
