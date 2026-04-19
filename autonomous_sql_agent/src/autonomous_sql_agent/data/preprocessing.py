"""Text and tabular preprocessing helpers."""

from __future__ import annotations

import pandas as pd
import structlog

logger: structlog.BoundLogger = structlog.get_logger(__name__)


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case all column names."""
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[\s\-/]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )
    logger.debug("columns_normalized", columns=list(df.columns))
    return df


def drop_high_null_columns(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    """Drop columns where null fraction exceeds threshold."""
    null_frac = df.isnull().mean()
    drop_cols = null_frac[null_frac > threshold].index.tolist()
    if drop_cols:
        logger.info("dropping_high_null_columns", columns=drop_cols, threshold=threshold)
        df = df.drop(columns=drop_cols)
    return df
