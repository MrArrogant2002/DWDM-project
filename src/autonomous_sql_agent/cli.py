from __future__ import annotations

import argparse

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.logging_utils import configure_logging
from autonomous_sql_agent.seed import WarehouseSeeder


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autonomous SQL warehouse project utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="Create the warehouse schema.")
    init_db.add_argument(
        "--schema-path",
        default=None,
        help="Optional override for the warehouse schema SQL file.",
    )

    seed_db = subparsers.add_parser("seed-db", help="Create schema and seed the warehouse.")
    seed_db.add_argument("--orders", type=int, default=None, help="Number of fact_orders rows to generate.")
    seed_db.add_argument("--customers", type=int, default=6000, help="Number of customers to generate.")
    seed_db.add_argument("--products", type=int, default=300, help="Number of products to generate.")
    return parser


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    config = AppConfig.from_env()
    database = DatabaseManager(config)

    if args.command == "init-db":
        schema_path = args.schema_path or config.data_dir / "warehouse_schema.sql"
        database.execute_script(schema_path)
        return

    if args.command == "seed-db":
        WarehouseSeeder(config, database).seed_all(
            order_count=args.orders,
            customer_count=args.customers,
            product_count=args.products,
        )
        return

    parser.error("Unsupported command.")


if __name__ == "__main__":
    main()
