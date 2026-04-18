from __future__ import annotations

from autonomous_sql_agent.database import DatabaseManager


BUSINESS_GLOSSARY = {
    "sales": "Use fact_orders.total_amount for total sales and dim_date for time rollups.",
    "revenue": "Revenue is modeled as fact_orders.total_amount or fact_order_items.net_amount at item detail.",
    "returns": "Returns live in fact_returns and can be joined back to orders, products, customers, and regions.",
    "top products": "Top-product analysis uses dim_product with fact_order_items aggregated by net_amount or quantity.",
    "customer segment": "dim_customer.segment and loyalty_tier support segmentation.",
    "region": "dim_region.region_name and dim_region.city provide market geography.",
    "channel": "dim_channel.channel_name, device_type, and campaign_name describe acquisition and sales channels.",
    "trend": "Time trends should join fact tables to dim_date on order_date_id or return_date_id.",
}


class SchemaMetadataService:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    def get_business_glossary(self) -> dict[str, str]:
        return dict(BUSINESS_GLOSSARY)

    def build_schema_summary(self, question: str | None = None, max_columns: int = 6) -> str:
        metadata = self.database.get_schema_metadata()
        relevant_tables = self._filter_relevant_tables(question or "", metadata)
        lines: list[str] = []

        for table_name in relevant_tables:
            table_meta = metadata.get(table_name, {})
            columns = table_meta.get("columns", [])[:max_columns]
            fks = table_meta.get("foreign_keys", [])
            column_summary = ", ".join(
                f"{column['name']} ({column['data_type']}{', pk' if column['is_primary_key'] else ''})"
                for column in columns
            )
            fk_summary = ", ".join(
                f"{fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}"
                for fk in fks[:3]
            )
            line = f"- {table_name}: {column_summary}"
            if fk_summary:
                line += f" | joins: {fk_summary}"
            lines.append(line)

        return "\n".join(lines)

    @staticmethod
    def _filter_relevant_tables(question: str, metadata: dict[str, dict[str, list[dict[str, str]]]]) -> list[str]:
        question_lower = question.lower()
        priority: list[str] = []
        table_map = {
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

        if not priority:
            priority.extend(
                table for table in (
                    "fact_orders",
                    "fact_order_items",
                    "fact_returns",
                    "dim_product",
                    "dim_customer",
                    "dim_region",
                    "dim_date",
                    "dim_channel",
                )
                if table in metadata
            )

        for table_name in metadata:
            if table_name not in priority:
                priority.append(table_name)

        return priority
