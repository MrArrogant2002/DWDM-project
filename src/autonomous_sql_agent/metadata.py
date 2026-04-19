from __future__ import annotations

from autonomous_sql_agent.database import DatabaseManager

BUSINESS_GLOSSARY: dict[str, str] = {
    "sales": "Use total_amount, revenue, or net_amount columns for sales aggregations.",
    "revenue": "Revenue is typically total_amount or net_amount — check column names in schema.",
    "returns": "Look for return_amount, return_count, or tables with 'return' in the name.",
    "top products": "Group by product_name or category, order by revenue or quantity DESC.",
    "customer segment": "Group by segment, loyalty_tier, or customer_type columns.",
    "region": "Group by region, region_name, or city columns.",
    "channel": "Group by channel, channel_name, or source columns.",
    "trend": "Join or group by date, month, year columns and order chronologically.",
}

# Tables that are infrastructure — never include in schema summaries shown to the model
_INTERNAL_TABLES = {"app_analysis_sessions"}


class SchemaMetadataService:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    def get_business_glossary(self) -> dict[str, str]:
        return dict(BUSINESS_GLOSSARY)

    def build_schema_summary(
        self,
        question: str | None = None,
        max_columns: int = 10,
        table_filter: set[str] | None = None,
    ) -> str:
        metadata = self.database.get_schema_metadata()
        relevant_tables = self._filter_relevant_tables(question or "", metadata)
        lines: list[str] = []

        for table_name in relevant_tables:
            if table_name in _INTERNAL_TABLES:
                continue
            if table_filter is not None and table_name not in table_filter:
                continue
            table_meta = metadata.get(table_name, {})
            columns = table_meta.get("columns", [])[:max_columns]
            fks = table_meta.get("foreign_keys", [])
            column_summary = ", ".join(
                f"{c['name']} ({c['data_type']}{', pk' if c['is_primary_key'] else ''})"
                for c in columns
            )
            fk_summary = ", ".join(
                f"{fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}"
                for fk in fks[:3]
            )
            line = f"- {table_name}: {column_summary}"
            if fk_summary:
                line += f"  |  joins: {fk_summary}"
            lines.append(line)

        return "\n".join(lines)

    @staticmethod
    def _filter_relevant_tables(
        question: str,
        metadata: dict[str, dict[str, list[dict[str, str]]]],
    ) -> list[str]:
        question_lower = question.lower()
        priority: list[str] = []

        # keyword → preferred tables
        table_map: dict[str, list[str]] = {
            "sale": ["fact_orders", "fact_order_items", "dim_date"],
            "revenue": ["fact_orders", "fact_order_items", "dim_date"],
            "product": ["dim_product", "fact_order_items", "fact_orders"],
            "return": ["fact_returns", "fact_order_items", "fact_orders", "dim_region"],
            "region": ["dim_region", "fact_orders", "fact_returns"],
            "customer": ["dim_customer", "fact_orders", "fact_returns"],
            "segment": ["dim_customer", "fact_orders"],
            "channel": ["dim_channel", "fact_orders"],
            "trend": ["dim_date", "fact_orders"],
            "month": ["dim_date", "fact_orders"],
        }

        for keyword, tables in table_map.items():
            if keyword in question_lower:
                for table in tables:
                    if table in metadata and table not in priority:
                        priority.append(table)

        # Add all remaining non-internal tables
        for table_name in metadata:
            if table_name not in priority and table_name not in _INTERNAL_TABLES:
                priority.append(table_name)

        return priority
