from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autonomous_sql_agent.analytics import AnalyticsService


class AnalyticsServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = AnalyticsService()

    def test_detects_anomaly_signal(self) -> None:
        frame = pd.DataFrame(
            {
                "region_name": ["West", "South", "Midwest", "Northeast", "Coastal"],
                "return_amount": [100, 110, 98, 102, 500],
            }
        )
        insights, followups = self.service.analyze(frame, "anomaly")
        self.assertGreaterEqual(len(insights), 2)
        self.assertTrue(any("anomaly" in insight.lower() for insight in insights))
        self.assertGreaterEqual(len(followups), 1)


if __name__ == "__main__":
    unittest.main()
