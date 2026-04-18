from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autonomous_sql_agent.charting import ChartService


class ChartServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ChartService()

    def test_prefers_line_chart_for_monthly_trend(self) -> None:
        frame = pd.DataFrame(
            {
                "month_name": ["January", "February", "March"],
                "revenue": [1000, 1250, 1175],
            }
        )
        spec = self.service.infer_chart_spec(frame)
        self.assertEqual(spec.chart_type, "line")
        self.assertEqual(spec.y_field, "revenue")

    def test_prefers_bar_chart_for_category_metric(self) -> None:
        frame = pd.DataFrame(
            {
                "category": ["Electronics", "Apparel", "Home"],
                "revenue": [5000, 2800, 1900],
            }
        )
        spec = self.service.infer_chart_spec(frame)
        self.assertEqual(spec.chart_type, "bar")
        self.assertEqual(spec.x_field, "category")


if __name__ == "__main__":
    unittest.main()
