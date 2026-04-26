"""Tests for CSVIngestor — covers every bug fixed in the rewrite."""

from __future__ import annotations

import io
import unittest

import pandas as pd
from sqlalchemy import create_engine

from autonomous_sql_agent.csv_ingestion import (
    CSVIngestor,
    SchemaBlueprint,
    _ID_NAME_RE,
    _to_snake,
)

# ---------------------------------------------------------------------------
# In-memory SQLite DB — pandas to_sql works; nothing touches disk
# ---------------------------------------------------------------------------


class _FakeDB:
    engine = create_engine("sqlite:///:memory:")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _csv_bytes(text: str, encoding: str = "utf-8") -> io.BytesIO:
    return io.BytesIO(text.encode(encoding))


def _run(csv_bytes: io.BytesIO, prefix: str = "test") -> SchemaBlueprint:
    """Drive CSVIngestor.process() and return the blueprint or raise on error."""
    ingestor = CSVIngestor(_FakeDB())  # type: ignore[arg-type]  # engine attr matches DatabaseManager
    blueprint: SchemaBlueprint | None = None
    for update in ingestor.process(csv_bytes, table_prefix=prefix):
        if "error" in update:
            raise RuntimeError(update["error"])
        if "result" in update:
            blueprint = update["result"]
    assert blueprint is not None, "process() finished without yielding a result"
    return blueprint


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class ToSnakeTests(unittest.TestCase):
    def test_basic_spaces(self):
        assert _to_snake("First Name") == "first_name"

    def test_leading_digit(self):
        assert _to_snake("2024_revenue").startswith("col_")

    def test_sql_keyword_guarded(self):
        result = _to_snake("select")
        assert result == "col_select"

    def test_empty_string_returns_column(self):
        assert _to_snake("") == "column"

    def test_special_chars(self):
        assert _to_snake("Unit$Price!") == "unit_price"

    def test_unicode_symbols_stripped(self):
        # Non-word unicode outside \w gets replaced with _
        result = _to_snake("Revenue (€)")
        assert "€" not in result
        assert result.replace("_", "").isalnum()


class IDNameDetectionTests(unittest.TestCase):
    def test_suffix_id(self):
        assert _ID_NAME_RE.search("customer_id")

    def test_suffix_key(self):
        assert _ID_NAME_RE.search("product_key")

    def test_exact_id(self):
        assert _ID_NAME_RE.search("id")

    def test_phone(self):
        assert _ID_NAME_RE.search("phone_number")

    def test_zip(self):
        assert _ID_NAME_RE.search("postal_code")

    def test_revenue_not_id(self):
        assert not _ID_NAME_RE.search("total_revenue")

    def test_price_not_id(self):
        assert not _ID_NAME_RE.search("unit_price")


class ReadCSVTests(unittest.TestCase):
    def test_comma_delimited(self):
        csv = "a,b,c\n1,2,3\n"
        bp = _run(_csv_bytes(csv))
        assert bp.row_count == 1

    def test_tab_delimited(self):
        csv = "a\tb\tc\n1\t2\t3\n"
        bp = _run(_csv_bytes(csv))
        assert bp.row_count == 1

    def test_semicolon_delimited(self):
        csv = "a;b;c\n1;2;3\n"
        bp = _run(_csv_bytes(csv))
        assert bp.row_count == 1

    def test_pipe_delimited(self):
        csv = "a|b|c\n1|2|3\n"
        bp = _run(_csv_bytes(csv))
        assert bp.row_count == 1

    def test_latin1_encoding(self):
        # "café" contains a non-UTF-8 byte in latin-1
        csv = "name,value\ncafé,10\n"
        bp = _run(_csv_bytes(csv, encoding="latin-1"))
        assert bp.row_count == 1

    def test_utf8_bom_encoding(self):
        csv = "name,value\nalpha,1\n"
        bp = _run(_csv_bytes(csv, encoding="utf-8-sig"))
        assert bp.row_count == 1


class ColumnSanitizationTests(unittest.TestCase):
    def test_spaces_become_underscores(self):
        csv = "First Name,Last Name,Age\nAlice,Smith,30\n"
        bp = _run(_csv_bytes(csv))
        assert (
            "first_name"
            in bp.dimension_columns
            + bp.id_columns
            + bp.measure_columns
            + bp.text_columns
        )

    def test_duplicate_column_names_get_suffix(self):
        csv = "value,value,value\n1,2,3\n"
        bp = _run(_csv_bytes(csv))
        # Should not crash and should have 3 distinct columns
        total = (
            len(bp.measure_columns)
            + len(bp.dimension_columns)
            + len(bp.id_columns)
            + len(bp.date_columns)
            + len(bp.text_columns)
        )
        assert total == 3

    def test_rename_map_populated(self):
        csv = "Order ID,Unit Price\n1,9.99\n"
        bp = _run(_csv_bytes(csv))
        assert len(bp.rename_map) == 2


class TypeCoercionTests(unittest.TestCase):
    def test_currency_column_becomes_measure(self):
        rows = "\n".join(f"item_{i},${i * 10:.2f}" for i in range(1, 12))
        csv = f"name,price\n{rows}\n"
        bp = _run(_csv_bytes(csv))
        assert "price" in bp.measure_columns

    def test_percent_column_becomes_measure(self):
        rows = "\n".join(f"item_{i},{i * 5} %" for i in range(1, 12))
        csv = f"name,discount\n{rows}\n"
        bp = _run(_csv_bytes(csv))
        assert "discount" in bp.measure_columns

    def test_date_column_detected(self):
        dates = "\n".join(f"2024-01-{i:02d},100" for i in range(1, 12))
        csv = f"order_date,amount\n{dates}\n"
        bp = _run(_csv_bytes(csv))
        assert "order_date" in bp.date_columns

    def test_na_strings_normalised(self):
        csv = "name,value\nAlice,100\nBob,N/A\nCarol,null\n"
        bp = _run(_csv_bytes(csv))
        # Should not crash; value column should be measure
        assert "value" in bp.measure_columns

    def test_all_null_column_dropped(self):
        csv = "name,empty,score\nAlice,,90\nBob,,85\n"
        bp = _run(_csv_bytes(csv))
        all_cols = (
            bp.measure_columns
            + bp.dimension_columns
            + bp.id_columns
            + bp.date_columns
            + bp.text_columns
        )
        assert "empty" not in all_cols


class ColumnRoleTests(unittest.TestCase):
    def test_id_column_not_a_measure(self):
        rows = "\n".join(f"{i},{i * 10}" for i in range(1, 12))
        csv = f"order_id,total\n{rows}\n"
        bp = _run(_csv_bytes(csv))
        assert "order_id" in bp.id_columns
        assert "order_id" not in bp.measure_columns

    def test_numeric_measure_not_misclassified_as_id(self):
        # Two columns ensure the sniffer picks comma as the delimiter correctly.
        rows = "\n".join(f"item_{i},{i * 9.99:.2f}" for i in range(1, 12))
        csv = f"product_name,unit_price\n{rows}\n"
        bp = _run(_csv_bytes(csv))
        assert "unit_price" in bp.measure_columns
        assert "unit_price" not in bp.id_columns

    def test_low_cardinality_string_is_dimension(self):
        categories = ["Electronics", "Clothing", "Books"]
        rows = "\n".join(f"item_{i},{categories[i % 3]},{i * 5}" for i in range(15))
        csv = f"product_id,category,price\n{rows}\n"
        bp = _run(_csv_bytes(csv))
        assert "category" in bp.dimension_columns

    def test_high_cardinality_string_is_text(self):
        # 200 rows, each description unique and long → cardinality > 50 ceiling
        rows = "\n".join(f"item_{i},{'x' * 80 + str(i)},{i * 5}" for i in range(200))
        csv = f"product_id,description,price\n{rows}\n"
        bp = _run(_csv_bytes(csv))
        assert "description" in bp.text_columns
        assert "description" not in bp.dimension_columns


class BlueprintTests(unittest.TestCase):
    def test_fact_table_name_derived_from_prefix(self):
        csv = "a,b\n1,2\n"
        bp = _run(_csv_bytes(csv), prefix="my_sales")
        assert bp.fact_table == "my_sales_fact"

    def test_dim_tables_have_unique_names(self):
        # Two columns that would both map to dim_col after sanitisation
        # (they have different content but same snake_case base)
        csv = "col,COL,value\nA,X,1\nB,Y,2\nC,Z,3\n"
        bp = _run(_csv_bytes(csv))
        dim_names = list(bp.dim_tables.keys())
        assert len(dim_names) == len(set(dim_names)), "Dim table names must be unique"

    def test_empty_file_yields_error(self):
        csv = "name,value\n"  # header only, no data rows
        with self.assertRaises(RuntimeError):
            _run(_csv_bytes(csv))

    def test_schema_summary_string(self):
        csv = "order_id,category,price,order_date\n1,Books,9.99,2024-01-01\n"
        bp = _run(_csv_bytes(csv))
        summary = bp.schema_summary()
        assert bp.fact_table in summary


if __name__ == "__main__":
    unittest.main()
