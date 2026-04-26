from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Any, Generator

import pandas as pd

from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.logging_utils import get_logger

logger = get_logger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
_MAX_DIM_CARDINALITY = 50  # hard upper bound on unique values for a dimension
_DIM_CARDINALITY_RATIO = 0.05  # also cap at 5 % of row count (whichever is smaller)
_NUMERIC_COERCE_THRESHOLD = (
    0.90  # ≥ 90 % of non-null values parseable → coerce to float
)
_DATE_PARSE_THRESHOLD = 0.70  # ≥ 70 % of non-null values parseable → coerce to datetime

# ── Column-role patterns ──────────────────────────────────────────────────────
# ID detection is name-based ONLY — no cardinality heuristic.
# The old cardinality heuristic (n_unique/n > 0.85) incorrectly marks numeric
# measures like price, revenue, latitude, longitude as IDs.
_ID_NAME_RE = re.compile(
    r"(_id|_key|_pk|_fk|_code|_num|_no|_number|_ref|_uuid|_sku|_hash)$"
    r"|^(id|key|pk|fk|uuid|sku)$"
    r"|(phone|postal|zip|fax|barcode|isbn|ssn|ein|tin|vat|npi)",
    re.IGNORECASE,
)

# Column names that hint at date / time content
_DATE_NAME_RE = re.compile(
    r"(date|time|_dt|_at|_ts|timestamp|created|updated|modified|"
    r"ordered|shipped|delivered|day|month|year|period|week)",
    re.IGNORECASE,
)

# ── String-cleaning constants ─────────────────────────────────────────────────
# Currency symbols and thousands-separator commas to strip before numeric coercion.
# No backslashes before non-special chars — PyArrow's RE2 engine rejects e.g. \€.
# Inside a character class [...] only ], \, ^, - are special; $ is literal.
_CURRENCY_CHARS = r"[$€£¥₹₩₪₦₫฿﷼,]"
# Trailing % to detect percentage columns
_PERCENT_SUFFIX = r"\s*%\s*$"

# Common "missing value" strings that pandas astype(str) doesn't auto-convert
_NA_STRINGS = frozenset(
    {"nan", "none", "null", "na", "n/a", "nil", "#n/a", "#na", "-", "--", ""}
)

# ── SQL keyword guard ─────────────────────────────────────────────────────────
_SQL_KEYWORDS = frozenset(
    {
        "select",
        "from",
        "where",
        "group",
        "order",
        "by",
        "having",
        "join",
        "on",
        "as",
        "with",
        "table",
        "index",
        "key",
        "primary",
        "foreign",
        "references",
        "default",
        "not",
        "null",
        "and",
        "or",
        "in",
        "is",
        "like",
        "between",
        "case",
        "when",
        "then",
        "else",
        "end",
        "distinct",
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "create",
        "drop",
        "alter",
        "insert",
        "update",
        "delete",
        "into",
        "values",
        "set",
        "limit",
        "offset",
        "union",
        "all",
        "exists",
        "unique",
        "check",
        "constraint",
        "column",
    }
)

# Encodings tried in order when reading a CSV
_ENCODINGS = ("utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1")

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


def _to_snake(name: str) -> str:
    """Convert any string to a unique, SQL-safe snake_case identifier."""
    s = re.sub(r"[^\w]", "_", name.strip())
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if not s:
        return "column"
    if s[0].isdigit():
        s = "col_" + s
    if s in _SQL_KEYWORDS:
        s = "col_" + s
    return s


# ── Data models ───────────────────────────────────────────────────────────────


@dataclass
class ColumnProfile:
    name: str  # SQL-safe column name as stored in the DB
    dtype: str
    n_unique: int
    null_pct: float
    sample_values: list[Any]
    role: str  # "measure" | "dimension" | "date" | "id" | "text"


@dataclass
class SchemaBlueprint:
    fact_table: str
    dim_tables: dict[str, str]  # dim_table_name → sql column name in the fact table
    date_columns: list[str]
    measure_columns: list[str]
    dimension_columns: list[str]
    id_columns: list[str]
    row_count: int
    # Fields added below have defaults so cached/serialised blueprints remain loadable
    text_columns: list[str] = field(default_factory=list)
    column_profiles: list[ColumnProfile] = field(default_factory=list)
    rename_map: dict[str, str] = field(
        default_factory=dict
    )  # original header → sql name

    def schema_summary(self) -> str:
        lines: list[str] = [f"Table: {self.fact_table}  ({self.row_count:,} rows)"]
        if self.measure_columns:
            lines.append(f"  Numeric measures : {', '.join(self.measure_columns)}")
        if self.date_columns:
            lines.append(f"  Date columns     : {', '.join(self.date_columns)}")
        if self.dimension_columns:
            lines.append(f"  Categorical dims : {', '.join(self.dimension_columns)}")
        if self.id_columns:
            lines.append(f"  ID columns       : {', '.join(self.id_columns)}")
        if self.text_columns:
            lines.append(f"  Free-text cols   : {', '.join(self.text_columns)}")
        for dim_tbl, src_col in self.dim_tables.items():
            lines.append(f"Table: {dim_tbl}  (dimension for '{src_col}')")
        return "\n".join(lines)


# ── Main ingestor ─────────────────────────────────────────────────────────────


class CSVIngestor:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    # ── Public API ────────────────────────────────────────────────────────────

    def process(
        self, file: Any, table_prefix: str = "data"
    ) -> Generator[dict[str, Any], None, None]:
        """
        Yields progress dicts: {"step": str, "progress": float}.
        The final successful dict also carries {"result": SchemaBlueprint}.
        On unrecoverable error, the final dict carries {"error": str}.
        """
        yield {"step": "Reading CSV file...", "progress": 0.05}

        try:
            df, encoding = self._read_csv(file)
        except ValueError as exc:
            yield {
                "step": f"Error reading file: {exc}",
                "progress": 1.0,
                "error": str(exc),
            }
            return

        n_original = len(df)
        yield {
            "step": (
                f"Loaded {n_original:,} rows × {len(df.columns)} columns"
                f" (encoding: {encoding})."
            ),
            "progress": 0.12,
        }

        yield {
            "step": "Sanitising column names to SQL-safe identifiers...",
            "progress": 0.18,
        }
        df, rename_map = self._sanitize_columns(df)

        yield {
            "step": "Removing fully-empty rows and exact duplicates...",
            "progress": 0.24,
        }
        df = df.dropna(how="all").drop_duplicates().reset_index(drop=True)
        n_clean = len(df)
        removed = n_original - n_clean

        if n_clean == 0:
            msg = (
                "No data remains after cleaning — "
                "the file may be empty or consist entirely of duplicate rows."
            )
            yield {"step": f"Error: {msg}", "progress": 1.0, "error": msg}
            return

        yield {
            "step": f"Cleaned: {n_clean:,} rows kept, {removed:,} rows removed.",
            "progress": 0.32,
        }

        yield {"step": "Inferring and coercing column data types...", "progress": 0.40}
        df = self._coerce_types(df)

        yield {
            "step": "Profiling columns — detecting roles (measure / dimension / date / id)...",
            "progress": 0.50,
        }
        profiles = self._profile_columns(df)
        role_summary = ", ".join(f"{p.role}:{p.name}" for p in profiles[:6])
        yield {
            "step": f"Roles detected — {role_summary}{'...' if len(profiles) > 6 else ''}",
            "progress": 0.58,
        }

        yield {"step": "Building star-schema blueprint...", "progress": 0.64}
        blueprint = self._build_blueprint(profiles, df, table_prefix, rename_map)

        yield {
            "step": (
                f"Writing fact table `{blueprint.fact_table}`"
                f" to warehouse ({n_clean:,} rows)..."
            ),
            "progress": 0.70,
        }
        df.to_sql(
            blueprint.fact_table,
            self.database.engine,
            if_exists="replace",
            index=False,
        )

        n_dims = len(blueprint.dim_tables)
        for i, (dim_table, src_col) in enumerate(blueprint.dim_tables.items()):
            frac = 0.70 + 0.26 * (i + 1) / max(n_dims, 1)
            yield {
                "step": (
                    f"Extracting dimension table `{dim_table}`"
                    f" from column '{src_col}'..."
                ),
                "progress": round(frac, 3),
            }
            self._write_dim_table(df, dim_table, src_col)

        yield {
            "step": "Warehouse ready! All tables loaded.",
            "progress": 1.0,
            "result": blueprint,
        }

    # ── Private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _read_csv(file: Any) -> tuple[pd.DataFrame, str]:
        """
        Read a CSV-like file, trying multiple encodings and auto-detecting the
        field delimiter (comma, tab, semicolon, pipe, …).
        Returns (DataFrame, encoding_used).
        Raises ValueError with a descriptive message on failure.
        """
        # Buffer into bytes so we can retry with different encodings
        if hasattr(file, "read"):
            raw: bytes = file.read()
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
        else:
            raw = bytes(file)

        last_err: Exception | None = None
        for enc in _ENCODINGS:
            try:
                text = raw.decode(enc)
                df = pd.read_csv(
                    io.StringIO(text),
                    sep=None,  # auto-detect delimiter
                    engine="python",  # python engine required for sep=None; no low_memory
                )
                if len(df.columns) == 0:
                    raise ValueError("CSV parsed to a table with no columns.")
                return df, enc
            except (UnicodeDecodeError, pd.errors.ParserError) as exc:
                last_err = exc

        raise ValueError(
            f"Could not parse the file with any supported encoding "
            f"({', '.join(_ENCODINGS)}). Last error: {last_err}"
        )

    @staticmethod
    def _sanitize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
        """
        Rename every column to a unique snake_case SQL-safe identifier.
        Duplicate base names get a numeric suffix (_2, _3, …).
        Returns (renamed_df, {original_header: sql_name}).
        """
        rename_map: dict[str, str] = {}
        seen: dict[str, int] = {}
        new_names: list[str] = []

        for orig in df.columns:
            base = _to_snake(str(orig))
            count = seen.get(base, 0)
            seen[base] = count + 1
            final = base if count == 0 else f"{base}_{count + 1}"
            rename_map[str(orig)] = final
            new_names.append(final)

        df = df.copy()
        df.columns = pd.Index(new_names)
        return df, rename_map

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Coerce object-dtype columns to float or datetime where the data supports it.

        Handles:
        - Whitespace stripping and common NA-string normalisation
        - Currency symbols ($ € £ ¥ ₹ …) and thousands-separator commas
        - Percentage suffixes (e.g. "25 %")
        - Date-hinted column names get a lower acceptance threshold
        - Hit-rate is computed over non-null values only (avoids null-heavy
          columns being unfairly penalised)
        - Columns that are 100 % null are dropped
        """
        # Drop columns that carry no information at all
        all_null = [c for c in df.columns if df[c].isna().all()]
        if all_null:
            logger.info("Dropping all-null columns: %s", all_null)
            df = df.drop(columns=all_null)

        for col in list(df.columns):
            s = df[col]

            # Already a typed column — nothing to coerce
            if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(
                s
            ):
                continue

            # Normalise to clean strings; replace common NA representations with NaN
            clean = s.astype(str).str.strip()
            clean = clean.where(~clean.str.lower().isin(_NA_STRINGS), other=None)

            non_null = clean.dropna()
            if non_null.empty:
                df[col] = clean
                continue

            col_lower = col.lower()

            # ── 1. Try datetime ───────────────────────────────────────────
            # format="mixed" (pandas ≥ 2.0) suppresses the per-element
            # dateutil fallback warning while still handling varied formats.
            try:
                parsed_dt = pd.to_datetime(non_null, errors="coerce", format="mixed")
            except TypeError:
                parsed_dt = pd.to_datetime(non_null, errors="coerce")
            date_threshold = (
                0.50 if _DATE_NAME_RE.search(col_lower) else _DATE_PARSE_THRESHOLD
            )
            if parsed_dt.notna().mean() >= date_threshold:
                try:
                    df[col] = pd.to_datetime(clean, errors="coerce", format="mixed")
                except TypeError:
                    df[col] = pd.to_datetime(clean, errors="coerce")
                continue

            # ── 2. Try numeric (with currency / percent stripping) ────────
            is_pct = non_null.str.contains(r"%\s*$", regex=True).mean() > 0.5
            num_clean = clean.fillna("").str.replace(_CURRENCY_CHARS, "", regex=True)
            if is_pct:
                num_clean = num_clean.str.replace(_PERCENT_SUFFIX, "", regex=True)

            parsed_num = pd.to_numeric(num_clean, errors="coerce")
            # Evaluate only over rows that had a non-null value in clean
            non_null_mask = clean.notna()
            hit_rate = (
                parsed_num[non_null_mask].notna().mean() if non_null_mask.any() else 0.0
            )
            if hit_rate >= _NUMERIC_COERCE_THRESHOLD:
                # Restore NaN for originally-null slots
                df[col] = parsed_num.where(non_null_mask, other=float("nan"))
                continue

            # ── 3. Keep as whitespace-stripped string ─────────────────────
            df[col] = clean

        return df

    def _profile_columns(self, df: pd.DataFrame) -> list[ColumnProfile]:
        """
        Assign a role to each column: measure | dimension | date | id | text.

        ID detection uses column names only — the old cardinality-ratio heuristic
        (n_unique / n > 0.85) incorrectly flagged numeric measures such as
        price, revenue, and latitude as IDs.

        A string column with ≤ _MAX_DIM_CARDINALITY unique values is a dimension;
        above that it is free-text.  The fixed ceiling avoids the brittleness of a
        row-count-proportional formula, which breaks for small test datasets.
        """
        profiles: list[ColumnProfile] = []
        dim_ceiling = _MAX_DIM_CARDINALITY

        for col in df.columns:
            series = df[col]
            n_unique = int(series.nunique(dropna=True))
            null_pct = round(float(series.isna().mean()) * 100, 1)
            sample = series.dropna().head(3).tolist()
            is_numeric = pd.api.types.is_numeric_dtype(series)
            is_date = pd.api.types.is_datetime64_any_dtype(series)

            # Name-based ID detection only
            is_id = bool(_ID_NAME_RE.search(col.lower()))

            if is_date:
                role = "date"
            elif is_id:
                role = "id"
            elif is_numeric:
                role = "measure"
            elif n_unique <= dim_ceiling:
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
        rename_map: dict[str, str],
    ) -> SchemaBlueprint:
        safe_prefix = re.sub(r"[^a-z0-9_]", "_", prefix.lower()).strip("_") or "data"
        fact_table = f"{safe_prefix}_fact"

        # Build dim tables, guarding against name collisions
        dim_tables: dict[str, str] = {}
        seen_dim_names: set[str] = set()
        for p in profiles:
            if p.role != "dimension":
                continue
            col_safe = re.sub(r"[^a-z0-9]", "_", p.name.lower()).strip("_") or "col"
            dim_name = f"dim_{col_safe}"
            if dim_name in seen_dim_names:
                suffix = 2
                while f"{dim_name}_{suffix}" in seen_dim_names:
                    suffix += 1
                dim_name = f"{dim_name}_{suffix}"
            seen_dim_names.add(dim_name)
            dim_tables[dim_name] = p.name

        return SchemaBlueprint(
            fact_table=fact_table,
            dim_tables=dim_tables,
            date_columns=[p.name for p in profiles if p.role == "date"],
            measure_columns=[p.name for p in profiles if p.role == "measure"],
            dimension_columns=[p.name for p in profiles if p.role == "dimension"],
            id_columns=[p.name for p in profiles if p.role == "id"],
            row_count=len(df),
            text_columns=[p.name for p in profiles if p.role == "text"],
            column_profiles=profiles,
            rename_map=rename_map,
        )

    def _write_dim_table(self, df: pd.DataFrame, dim_table: str, src_col: str) -> None:
        if src_col not in df.columns:
            logger.warning(
                "Dimension column '%s' not found in dataframe — skipping '%s'.",
                src_col,
                dim_table,
            )
            return
        try:
            dim_df = (
                df[[src_col]]
                .drop_duplicates()
                .dropna()
                .sort_values(src_col)
                .reset_index(drop=True)
            )
            dim_df.insert(0, "id", range(1, len(dim_df) + 1))
            dim_df.to_sql(
                dim_table, self.database.engine, if_exists="replace", index=False
            )
        except Exception as exc:
            logger.warning("Failed to write dimension table '%s': %s", dim_table, exc)
