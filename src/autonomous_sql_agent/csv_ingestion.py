from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Generator

import pandas as pd

from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.logging_utils import get_logger

logger = get_logger(__name__)

DIM_CARDINALITY_THRESHOLD = 50
SUMMARY_TRIGGER_WORDS = {
    "summarize",
    "summary",
    "explain the",
    "explain why",
    "explain how",
    "describe the",
    "describe what",
    "tell me about",
    "interpret",
    "give me a summary",
    "give me an overview",
    "break it down",
    "in plain english",
    "what does this mean",
    "what does that mean",
}


def needs_summary(question: str) -> bool:
    q = question.lower()
    return any(trigger in q for trigger in SUMMARY_TRIGGER_WORDS)


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    n_unique: int
    null_pct: float
    sample_values: list[Any]
    role: str  # "measure" | "dimension" | "date" | "id" | "text"


@dataclass
class SchemaBlueprint:
    fact_table: str
    dim_tables: dict[str, str]  # table_name -> original column name
    date_columns: list[str]
    measure_columns: list[str]
    dimension_columns: list[str]
    id_columns: list[str]
    row_count: int
    column_profiles: list[ColumnProfile] = field(default_factory=list)

    def schema_summary(self) -> str:
        lines: list[str] = []
        lines.append(f"Table: {self.fact_table}  ({self.row_count:,} rows)")
        if self.measure_columns:
            lines.append(f"  Numeric measures : {', '.join(self.measure_columns)}")
        if self.date_columns:
            lines.append(f"  Date columns     : {', '.join(self.date_columns)}")
        if self.dimension_columns:
            lines.append(f"  Categorical dims : {', '.join(self.dimension_columns)}")
        if self.id_columns:
            lines.append(f"  ID columns       : {', '.join(self.id_columns)}")
        for dim_tbl, src_col in self.dim_tables.items():
            lines.append(f"Table: {dim_tbl}  (dimension for '{src_col}')")
        return "\n".join(lines)


class CSVIngestor:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    def process(
        self, file: Any, table_prefix: str = "data"
    ) -> Generator[dict[str, Any], None, None]:
        """
        Yields progress dicts: {"step": str, "progress": float}.
        The final dict also carries {"result": SchemaBlueprint}.
        """
        yield {"step": "Reading CSV file...", "progress": 0.05}
        df = pd.read_csv(file, low_memory=False)
        n_original = len(df)

        yield {
            "step": f"Loaded {n_original:,} rows × {len(df.columns)} columns.",
            "progress": 0.12,
        }

        yield {
            "step": "Removing fully-empty rows and exact duplicates...",
            "progress": 0.18,
        }
        df = df.dropna(how="all").drop_duplicates().reset_index(drop=True)
        n_clean = len(df)
        removed = n_original - n_clean

        yield {
            "step": f"Cleaned: {n_clean:,} rows kept, {removed:,} rows removed.",
            "progress": 0.26,
        }

        yield {"step": "Inferring and coercing column data types...", "progress": 0.34}
        df = self._coerce_types(df)

        yield {
            "step": "Profiling columns — detecting roles (measure / dimension / date / id)...",
            "progress": 0.45,
        }
        profiles = self._profile_columns(df)
        role_summary = ", ".join(f"{p.role}:{p.name}" for p in profiles[:6])
        yield {
            "step": f"Roles detected — {role_summary}{'...' if len(profiles) > 6 else ''}",
            "progress": 0.54,
        }

        yield {"step": "Building star-schema blueprint...", "progress": 0.60}
        blueprint = self._build_blueprint(profiles, df, table_prefix)

        yield {
            "step": f"Writing fact table `{blueprint.fact_table}` to warehouse ({n_clean:,} rows)...",
            "progress": 0.68,
        }
        df.to_sql(
            blueprint.fact_table, self.database.engine, if_exists="replace", index=False
        )

        n_dims = len(blueprint.dim_tables)
        for i, (dim_table, src_col) in enumerate(blueprint.dim_tables.items()):
            frac = 0.68 + 0.26 * (i + 1) / max(n_dims, 1)
            yield {
                "step": f"Extracting dimension table `{dim_table}` from column '{src_col}'...",
                "progress": round(frac, 3),
            }
            self._write_dim_table(df, dim_table, src_col)

        yield {
            "step": "Warehouse ready! All tables loaded.",
            "progress": 1.0,
            "result": blueprint,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    _DATE_HINTS = {
        "date",
        "time",
        "_dt",
        "_at",
        "day",
        "month",
        "year",
        "created",
        "updated",
        "timestamp",
    }

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            col_lower = col.lower()
            if any(hint in col_lower for hint in self._DATE_HINTS):
                try:
                    parsed = pd.to_datetime(df[col], errors="coerce")
                    if parsed.notna().mean() > 0.5:
                        df[col] = parsed
                        continue
                except Exception:
                    pass
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().mean() > 0.80:
                df[col] = converted
        return df

    def _profile_columns(self, df: pd.DataFrame) -> list[ColumnProfile]:
        profiles: list[ColumnProfile] = []
        n = max(len(df), 1)
        for col in df.columns:
            series = df[col]
            n_unique = int(series.nunique())
            null_pct = round(float(series.isna().mean()) * 100, 1)
            sample = series.dropna().head(3).tolist()
            is_numeric = pd.api.types.is_numeric_dtype(series)
            is_date = pd.api.types.is_datetime64_any_dtype(series)
            col_lower = col.lower()
            is_id = (
                col_lower.endswith("_id")
                or col_lower == "id"
                or (is_numeric and n_unique / n > 0.85 and n_unique > 100)
            )

            if is_date:
                role = "date"
            elif is_id:
                role = "id"
            elif is_numeric:
                role = "measure"
            elif n_unique <= DIM_CARDINALITY_THRESHOLD:
                role = "dimension"
            else:
                role = "text"

            profiles.append(
                ColumnProfile(
                    name=col,
                    dtype=str(series.dtype),
                    n_unique=n_unique,
                    null_pct=null_pct,
                    sample_values=sample,
                    role=role,
                )
            )
        return profiles

    def _build_blueprint(
        self,
        profiles: list[ColumnProfile],
        df: pd.DataFrame,
        prefix: str,
    ) -> SchemaBlueprint:
        safe = re.sub(r"[^a-z0-9_]", "_", prefix.lower()).strip("_") or "data"
        fact_table = f"{safe}_fact"

        dim_tables: dict[str, str] = {}
        for p in profiles:
            if p.role == "dimension":
                col_safe = re.sub(r"[^a-z0-9]", "_", p.name.lower()).strip("_")
                dim_tables[f"dim_{col_safe}"] = p.name

        return SchemaBlueprint(
            fact_table=fact_table,
            dim_tables=dim_tables,
            date_columns=[p.name for p in profiles if p.role == "date"],
            measure_columns=[p.name for p in profiles if p.role == "measure"],
            dimension_columns=[p.name for p in profiles if p.role == "dimension"],
            id_columns=[p.name for p in profiles if p.role == "id"],
            row_count=len(df),
            column_profiles=profiles,
        )

    def _write_dim_table(self, df: pd.DataFrame, dim_table: str, src_col: str) -> None:
        if src_col not in df.columns:
            return
        dim_df = (
            df[[src_col]]
            .drop_duplicates()
            .dropna()
            .sort_values(src_col)
            .reset_index(drop=True)
        )
        dim_df.insert(0, "id", range(1, len(dim_df) + 1))
        dim_df.to_sql(dim_table, self.database.engine, if_exists="replace", index=False)
