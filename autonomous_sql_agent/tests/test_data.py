"""Tests for data loading and preprocessing."""

from __future__ import annotations

import pandas as pd

from autonomous_sql_agent.data.preprocessing import (
    drop_high_null_columns,
    normalize_column_names,
)


def test_normalize_column_names() -> None:
    df = pd.DataFrame({"Order ID": [1], "Total Revenue ($)": [100.0]})
    result = normalize_column_names(df)
    assert "order_id" in result.columns
    assert "Order ID" not in result.columns


def test_drop_high_null_columns_removes_full_null() -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "b": [None, None, None]})
    result = drop_high_null_columns(df, threshold=0.9)
    assert "b" not in result.columns
    assert "a" in result.columns


def test_drop_high_null_columns_keeps_partial() -> None:
    df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, None]})
    result = drop_high_null_columns(df, threshold=0.9)
    assert "a" in result.columns
    assert "b" not in result.columns


def test_normalize_preserves_row_count() -> None:
    df = pd.DataFrame({"Col A": [1, 2], "Col B": [3, 4]})
    result = normalize_column_names(df)
    assert len(result) == 2
