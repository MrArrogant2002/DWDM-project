from __future__ import annotations

import re

from autonomous_sql_agent.models import ValidationResult

UNSAFE_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "grant",
    "truncate",
    "merge",
}
UNSAFE_FUNCTIONS = {"pg_sleep", "dblink", "copy"}


class SQLValidator:
    def validate(self, sql: str) -> ValidationResult:
        normalized_sql = sql.strip().rstrip(";")
        errors: list[str] = []
        warnings: list[str] = []

        if not normalized_sql:
            return ValidationResult(
                is_valid=False, errors=["The SQL generator returned an empty query."]
            )

        lowered = normalized_sql.lower()
        if ";" in normalized_sql:
            errors.append("Only a single SQL statement is allowed.")

        for keyword in UNSAFE_KEYWORDS:
            if re.search(rf"\b{keyword}\b", lowered):
                errors.append(f"Unsafe SQL keyword detected: {keyword}.")

        for function_name in UNSAFE_FUNCTIONS:
            if function_name in lowered:
                errors.append(f"Unsafe SQL function detected: {function_name}.")

        if not lowered.startswith(("select", "with")):
            errors.append("Only SELECT queries are permitted.")

        if "*" in lowered and "count(*)" not in lowered:
            warnings.append(
                "The query uses SELECT *; explicit columns are preferred for warehouse analytics."
            )

        parser_error = self._try_parse(normalized_sql)
        if parser_error:
            errors.append(parser_error)

        return ValidationResult(
            is_valid=not errors,
            errors=errors,
            warnings=warnings,
            normalized_sql=normalized_sql,
        )

    @staticmethod
    def _try_parse(sql: str) -> str | None:
        try:
            import sqlglot
        except ImportError:
            return None

        # Try SQLite first (default runtime dialect), then dialect-agnostic.
        # Never use read="postgres" — the system generates SQLite syntax (strftime etc.).
        last_exc: Exception | None = None
        for dialect in ("sqlite", None):
            try:
                sqlglot.parse_one(sql, read=dialect)
                return None  # parsed successfully
            except Exception as exc:
                last_exc = exc
        return f"SQL parser rejected the generated query: {last_exc}"
