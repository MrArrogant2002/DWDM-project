from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pandas as pd

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.logging_utils import get_logger
from autonomous_sql_agent.models import DownloadArtifacts

logger = get_logger(__name__)


def _require_sqlalchemy():
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:  # pragma: no cover - requires runtime deps
        raise RuntimeError(
            "SQLAlchemy is required to connect to the database. Install requirements.txt first."
        ) from exc
    return create_engine, text


class DatabaseManager:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._engine = None

    @property
    def _is_sqlite(self) -> bool:
        return self.config.database_url.startswith("sqlite")

    @property
    def engine(self):
        if self._engine is None:
            create_engine, _ = _require_sqlalchemy()
            if self._is_sqlite:
                self._engine = create_engine(
                    self.config.database_url,
                    connect_args={"check_same_thread": False},
                    future=True,
                )
            else:
                self._engine = create_engine(self.config.database_url, future=True)
        return self._engine

    def execute_script(self, script_path: str | Path) -> None:
        sql = Path(script_path).read_text(encoding="utf-8")
        raw_connection = self.engine.raw_connection()
        try:
            if self._is_sqlite:
                raw_connection.executescript(sql)
            else:
                cursor = raw_connection.cursor()
                cursor.execute(sql)
                raw_connection.commit()
        finally:
            raw_connection.close()

    def execute_sql(self, sql: str) -> None:
        raw_connection = self.engine.raw_connection()
        try:
            if self._is_sqlite:
                raw_connection.executescript(sql)
            else:
                cursor = raw_connection.cursor()
                cursor.execute(sql)
                raw_connection.commit()
        finally:
            raw_connection.close()

    def write_dataframe(
        self, table_name: str, dataframe: pd.DataFrame, if_exists: str = "append"
    ) -> None:
        dataframe.to_sql(table_name, self.engine, if_exists=if_exists, index=False)

    def explain_query(self, sql: str, timeout_ms: int | None = None) -> list[str]:
        _, text = _require_sqlalchemy()
        statement = sql.strip().rstrip(";")

        if self._is_sqlite:
            explain_sql = f"EXPLAIN QUERY PLAN {statement}"
            with self.engine.begin() as connection:
                rows = connection.execute(text(explain_sql)).fetchall()
            return [str(row) for row in rows]

        statement_timeout_ms = timeout_ms or self.config.statement_timeout_ms
        explain_sql = f"EXPLAIN {statement}"
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                f"SET LOCAL statement_timeout = {statement_timeout_ms}"
            )
            rows = connection.execute(text(explain_sql)).fetchall()
        return [str(row[0]) for row in rows]

    def query_dataframe(
        self, sql: str, limit: int | None = None, timeout_ms: int | None = None
    ) -> pd.DataFrame:
        _, text = _require_sqlalchemy()
        statement = sql.strip().rstrip(";")
        if limit is not None:
            statement = f"SELECT * FROM ({statement}) AS agent_result LIMIT {limit}"

        with self.engine.begin() as connection:
            if not self._is_sqlite:
                statement_timeout_ms = timeout_ms or self.config.statement_timeout_ms
                connection.exec_driver_sql(
                    f"SET LOCAL statement_timeout = {statement_timeout_ms}"
                )
            return pd.read_sql_query(text(statement), connection)

    def get_schema_metadata(self) -> dict[str, dict[str, list[dict[str, str]]]]:
        _, text = _require_sqlalchemy()

        if self._is_sqlite:
            return self._get_sqlite_metadata(text)

        column_sql = """
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.ordinal_position,
            CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = 'public'
        ) pk
          ON c.table_name = pk.table_name
         AND c.column_name = pk.column_name
        WHERE c.table_schema = 'public'
          AND c.table_name NOT LIKE 'pg_%'
        ORDER BY c.table_name, c.ordinal_position
        """

        fk_sql = """
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
        ORDER BY tc.table_name, kcu.column_name
        """

        metadata: dict[str, dict[str, list[dict[str, str]]]] = {}
        with self.engine.begin() as connection:
            for row in connection.execute(text(column_sql)):
                metadata.setdefault(row.table_name, {"columns": [], "foreign_keys": []})
                metadata[row.table_name]["columns"].append(
                    {
                        "name": row.column_name,
                        "data_type": row.data_type,
                        "is_primary_key": bool(row.is_primary_key),
                    }
                )
            for row in connection.execute(text(fk_sql)):
                metadata.setdefault(row.table_name, {"columns": [], "foreign_keys": []})
                metadata[row.table_name]["foreign_keys"].append(
                    {
                        "column_name": row.column_name,
                        "foreign_table_name": row.foreign_table_name,
                        "foreign_column_name": row.foreign_column_name,
                    }
                )
        return metadata

    def _get_sqlite_metadata(self, text) -> dict[str, dict[str, list[dict[str, str]]]]:
        metadata: dict[str, dict[str, list[dict[str, str]]]] = {}
        with self.engine.begin() as connection:
            table_rows = connection.execute(
                text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            ).fetchall()
            for (table_name,) in table_rows:
                info_rows = connection.execute(
                    text(f"PRAGMA table_info({table_name})")
                ).fetchall()
                fk_rows = connection.execute(
                    text(f"PRAGMA foreign_key_list({table_name})")
                ).fetchall()
                metadata[table_name] = {
                    "columns": [
                        {
                            "name": row[1],
                            "data_type": row[2] or "TEXT",
                            "is_primary_key": bool(row[5]),
                        }
                        for row in info_rows
                    ],
                    "foreign_keys": [
                        {
                            "column_name": row[3],
                            "foreign_table_name": row[2],
                            "foreign_column_name": row[4],
                        }
                        for row in fk_rows
                    ],
                }
        return metadata

    def save_session(
        self,
        question: str,
        approved_sql: str,
        row_count: int,
        chart_type: str | None,
        artifacts: DownloadArtifacts,
        warnings: list[str],
        session_id: str | None = None,
    ) -> str:
        _, text = _require_sqlalchemy()
        current_session_id = session_id or uuid4().hex
        insert_sql = text(
            """
            INSERT INTO app_analysis_sessions (
                session_id, question, approved_sql, row_count, chart_type, csv_path, xlsx_path, pdf_path, warnings
            ) VALUES (
                :session_id, :question, :approved_sql, :row_count, :chart_type, :csv_path, :xlsx_path, :pdf_path, :warnings
            )
            """
        )

        payload = {
            "session_id": current_session_id,
            "question": question,
            "approved_sql": approved_sql,
            "row_count": row_count,
            "chart_type": chart_type,
            "csv_path": artifacts.csv_path,
            "xlsx_path": artifacts.xlsx_path,
            "pdf_path": artifacts.pdf_path,
            "warnings": json.dumps(warnings),
        }
        with self.engine.begin() as connection:
            connection.execute(insert_sql, payload)
        return current_session_id

    def recent_sessions(self, limit: int = 10) -> list[dict[str, str]]:
        _, text = _require_sqlalchemy()
        query = text(
            """
            SELECT session_id, created_at, question, row_count, chart_type, csv_path, xlsx_path, pdf_path
            FROM app_analysis_sessions
            ORDER BY created_at DESC
            LIMIT :limit
            """
        )
        with self.engine.begin() as connection:
            rows = connection.execute(query, {"limit": limit}).mappings().all()
        return [dict(row) for row in rows]
