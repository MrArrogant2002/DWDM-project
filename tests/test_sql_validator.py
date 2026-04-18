from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autonomous_sql_agent.sql_validation import SQLValidator


class SQLValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validator = SQLValidator()

    def test_allows_single_select_query(self) -> None:
        result = self.validator.validate("SELECT category, SUM(net_amount) AS revenue FROM fact_order_items GROUP BY category")
        self.assertTrue(result.is_valid)
        self.assertEqual(result.errors, [])

    def test_blocks_delete_queries(self) -> None:
        result = self.validator.validate("DELETE FROM fact_orders")
        self.assertFalse(result.is_valid)
        self.assertTrue(any("delete" in error.lower() for error in result.errors))


if __name__ == "__main__":
    unittest.main()
