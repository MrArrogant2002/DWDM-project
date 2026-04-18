from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from autonomous_sql_agent.config import AppConfig
from autonomous_sql_agent.database import DatabaseManager
from autonomous_sql_agent.logging_utils import get_logger

logger = get_logger(__name__)


FIRST_NAMES = [
    "Ava", "Liam", "Noah", "Emma", "Olivia", "Mason", "Sophia", "Isabella", "Aria", "Mia",
    "Lucas", "Elijah", "Harper", "Amelia", "Ethan", "James", "Charlotte", "Benjamin", "Evelyn", "Henry",
]
LAST_NAMES = [
    "Patel", "Smith", "Brown", "Johnson", "Garcia", "Martinez", "Lee", "Clark", "Walker", "Hall",
    "Young", "Allen", "King", "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Carter",
]


@dataclass(slots=True)
class WarehouseSeeder:
    config: AppConfig
    database: DatabaseManager
    seed_value: int = 42

    def seed_all(
        self,
        order_count: int | None = None,
        customer_count: int = 6000,
        product_count: int = 300,
    ) -> None:
        total_orders = order_count or self.config.default_order_count
        rng = random.Random(self.seed_value)

        logger.info("Creating warehouse schema.")
        self.database.execute_script(self.config.data_dir / "warehouse_schema.sql")
        self._truncate_tables()

        logger.info("Generating retail/e-commerce seed data.")
        date_df = self._build_date_dimension(date(2023, 1, 1), date(2025, 12, 31))
        region_df = self._build_regions()
        channel_df = self._build_channels()
        product_df = self._build_products(product_count, rng)
        customer_df = self._build_customers(customer_count, region_df, rng)
        order_df, item_df, return_df = self._build_facts(
            total_orders=total_orders,
            customers=customer_df,
            products=product_df,
            channels=channel_df,
            regions=region_df,
            rng=rng,
        )

        self.database.write_dataframe("dim_date", date_df, if_exists="append")
        self.database.write_dataframe("dim_region", region_df, if_exists="append")
        self.database.write_dataframe("dim_channel", channel_df, if_exists="append")
        self.database.write_dataframe("dim_customer", customer_df, if_exists="append")
        self.database.write_dataframe("dim_product", product_df, if_exists="append")
        self.database.write_dataframe("fact_orders", order_df, if_exists="append")
        self.database.write_dataframe("fact_order_items", item_df, if_exists="append")
        self.database.write_dataframe("fact_returns", return_df, if_exists="append")

        logger.info(
            "Warehouse seeded with %s orders, %s order items, and %s returns.",
            len(order_df),
            len(item_df),
            len(return_df),
        )

    def _truncate_tables(self) -> None:
        self.database.execute_sql(
            """
            TRUNCATE TABLE
                fact_returns,
                fact_order_items,
                fact_orders,
                dim_customer,
                dim_product,
                dim_channel,
                dim_region,
                dim_date,
                app_analysis_sessions
            RESTART IDENTITY CASCADE
            """
        )

    @staticmethod
    def _build_date_dimension(start: date, end: date) -> pd.DataFrame:
        rows = []
        current = start
        while current <= end:
            rows.append(
                {
                    "date_id": int(current.strftime("%Y%m%d")),
                    "full_date": current,
                    "year": current.year,
                    "quarter": ((current.month - 1) // 3) + 1,
                    "month": current.month,
                    "month_name": current.strftime("%B"),
                    "week": int(current.strftime("%U")),
                    "day": current.day,
                    "day_name": current.strftime("%A"),
                    "is_weekend": current.weekday() >= 5,
                }
            )
            current += timedelta(days=1)
        return pd.DataFrame(rows)

    @staticmethod
    def _build_regions() -> pd.DataFrame:
        rows = [
            (1, "United States", "West", "San Francisco", "tier_1"),
            (2, "United States", "West", "Seattle", "tier_1"),
            (3, "United States", "South", "Austin", "tier_2"),
            (4, "United States", "South", "Atlanta", "tier_2"),
            (5, "United States", "Midwest", "Chicago", "tier_1"),
            (6, "United States", "Northeast", "New York", "tier_1"),
            (7, "United States", "Northeast", "Boston", "tier_1"),
            (8, "United States", "Midwest", "Denver", "tier_2"),
        ]
        return pd.DataFrame(
            rows,
            columns=["region_id", "country", "region_name", "city", "market_tier"],
        )

    @staticmethod
    def _build_channels() -> pd.DataFrame:
        rows = [
            (1, "website", "desktop", "organic_search"),
            (2, "website", "mobile", "email_campaign"),
            (3, "mobile_app", "mobile", "push_notification"),
            (4, "marketplace", "desktop", "sponsored_listing"),
            (5, "social_commerce", "mobile", "influencer_drop"),
        ]
        return pd.DataFrame(rows, columns=["channel_id", "channel_name", "device_type", "campaign_name"])

    def _build_products(self, product_count: int, rng: random.Random) -> pd.DataFrame:
        catalog = {
            "Electronics": {
                "subcategories": ["Headphones", "Laptop", "Smartwatch", "Tablet", "Gaming"],
                "brands": ["NovaTech", "Pulse", "Skylab", "Vertex"],
                "price_range": (80, 1800),
            },
            "Apparel": {
                "subcategories": ["Tops", "Bottoms", "Outerwear", "Shoes", "Athleisure"],
                "brands": ["Urban Loom", "Mistral", "Stride", "Peak"],
                "price_range": (20, 250),
            },
            "Home": {
                "subcategories": ["Kitchen", "Decor", "Furniture", "Storage", "Lighting"],
                "brands": ["Hearth", "Nook", "Lumen", "Oakline"],
                "price_range": (15, 600),
            },
            "Beauty": {
                "subcategories": ["Skincare", "Haircare", "Fragrance", "Cosmetics"],
                "brands": ["Aura", "Bloom", "Satin", "Luxe"],
                "price_range": (10, 180),
            },
        }

        rows = []
        categories = list(catalog.keys())
        for product_id in range(1, product_count + 1):
            category = rng.choice(categories)
            spec = catalog[category]
            subcategory = rng.choice(spec["subcategories"])
            brand = rng.choice(spec["brands"])
            price = round(rng.uniform(*spec["price_range"]), 2)
            rows.append(
                {
                    "product_id": product_id,
                    "sku": f"SKU-{product_id:05d}",
                    "product_name": f"{brand} {subcategory} {product_id}",
                    "category": category,
                    "subcategory": subcategory,
                    "brand": brand,
                    "supplier_name": f"{brand} Supply Co",
                    "unit_price": price,
                }
            )
        return pd.DataFrame(rows)

    def _build_customers(self, customer_count: int, region_df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
        tiers = ["bronze", "silver", "gold", "platinum"]
        segments = ["value", "core", "premium", "vip"]
        genders = ["female", "male", "non_binary"]
        region_rows = region_df.to_dict("records")

        rows = []
        for customer_id in range(1, customer_count + 1):
            region = rng.choice(region_rows)
            age = rng.randint(18, 70)
            if age < 25:
                age_band = "18-24"
            elif age < 35:
                age_band = "25-34"
            elif age < 45:
                age_band = "35-44"
            elif age < 55:
                age_band = "45-54"
            else:
                age_band = "55+"

            rows.append(
                {
                    "customer_id": customer_id,
                    "customer_code": f"CUST-{customer_id:06d}",
                    "first_name": rng.choice(FIRST_NAMES),
                    "last_name": rng.choice(LAST_NAMES),
                    "gender": rng.choice(genders),
                    "age": age,
                    "age_band": age_band,
                    "loyalty_tier": rng.choices(tiers, weights=[35, 32, 22, 11], k=1)[0],
                    "signup_date": date(2022, 1, 1) + timedelta(days=rng.randint(0, 1000)),
                    "city": region["city"],
                    "region_id": region["region_id"],
                    "segment": rng.choices(segments, weights=[25, 45, 20, 10], k=1)[0],
                }
            )
        return pd.DataFrame(rows)

    def _build_facts(
        self,
        total_orders: int,
        customers: pd.DataFrame,
        products: pd.DataFrame,
        channels: pd.DataFrame,
        regions: pd.DataFrame,
        rng: random.Random,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        customer_records = customers.to_dict("records")
        product_records = products.to_dict("records")
        channel_records = channels.to_dict("records")
        region_lookup = {row["region_id"]: row for row in regions.to_dict("records")}

        order_rows = []
        item_rows = []
        return_rows = []
        item_id = 1
        return_id = 1
        start_date = datetime(2023, 1, 1)
        total_days = (datetime(2025, 12, 31) - start_date).days

        product_weights = [1.6 if product["category"] == "Electronics" else 1.0 for product in product_records]

        for order_id in range(1, total_orders + 1):
            customer = rng.choice(customer_records)
            channel = rng.choice(channel_records)
            region = region_lookup[customer["region_id"]]
            order_datetime = start_date + timedelta(days=rng.randint(0, total_days))
            order_date = order_datetime.date()
            month_multiplier = self._seasonality_multiplier(order_date.month)
            shipping_days = rng.randint(1, 7)
            ship_date = order_date + timedelta(days=shipping_days)
            order_status = rng.choices(
                ["delivered", "processing", "cancelled"],
                weights=[92, 6, 2],
                k=1,
            )[0]

            item_count = rng.choices([1, 2, 3, 4], weights=[20, 44, 24, 12], k=1)[0]
            subtotal = 0.0
            discount_total = 0.0
            return_candidates: list[tuple[int, dict[str, object], int, float]] = []

            for _ in range(item_count):
                product = rng.choices(product_records, weights=product_weights, k=1)[0]
                quantity = rng.choices([1, 2, 3], weights=[64, 27, 9], k=1)[0]
                line_price = float(product["unit_price"]) * quantity
                promo_boost = 0.0
                if product["category"] == "Electronics" and order_date.month in {11, 12}:
                    promo_boost += 0.08
                if channel["channel_name"] == "social_commerce":
                    promo_boost += 0.05
                discount_rate = rng.uniform(0.0, 0.18) + promo_boost
                discount_amount = round(line_price * min(discount_rate, 0.35), 2)
                net_amount = round(line_price - discount_amount, 2)

                subtotal += line_price
                discount_total += discount_amount
                item_rows.append(
                    {
                        "order_item_id": item_id,
                        "order_id": order_id,
                        "product_id": product["product_id"],
                        "quantity": quantity,
                        "unit_price": round(float(product["unit_price"]), 2),
                        "discount_amount": discount_amount,
                        "net_amount": round(net_amount * month_multiplier, 2),
                    }
                )

                return_probability = 0.045
                if product["category"] == "Apparel":
                    return_probability += 0.05
                if region["region_name"] == "West":
                    return_probability += 0.01
                if order_date.month == 1:
                    return_probability += 0.03
                return_candidates.append((item_id, product, quantity, return_probability))
                item_id += 1

            adjusted_subtotal = round(subtotal * month_multiplier, 2)
            adjusted_discount = round(discount_total * month_multiplier, 2)
            tax_amount = round((adjusted_subtotal - adjusted_discount) * 0.075, 2)
            shipping_amount = round(rng.uniform(4.99, 19.99), 2)
            total_amount = round(adjusted_subtotal - adjusted_discount + tax_amount + shipping_amount, 2)

            order_rows.append(
                {
                    "order_id": order_id,
                    "order_number": f"ORD-{order_id:08d}",
                    "customer_id": customer["customer_id"],
                    "order_date_id": int(order_date.strftime("%Y%m%d")),
                    "ship_date_id": int(ship_date.strftime("%Y%m%d")),
                    "region_id": customer["region_id"],
                    "channel_id": channel["channel_id"],
                    "order_status": order_status,
                    "payment_method": rng.choice(["credit_card", "paypal", "gift_card", "upi"]),
                    "shipping_days": shipping_days,
                    "subtotal": adjusted_subtotal,
                    "discount_amount": adjusted_discount,
                    "tax_amount": tax_amount,
                    "shipping_amount": shipping_amount,
                    "total_amount": total_amount,
                }
            )

            if order_status == "cancelled":
                continue

            for candidate_item_id, product, quantity, return_probability in return_candidates:
                if rng.random() > return_probability:
                    continue
                return_qty = rng.randint(1, quantity)
                reason = rng.choice(
                    ["damaged", "wrong_size", "late_delivery", "quality_issue", "changed_mind"]
                )
                return_date = min(order_date + timedelta(days=rng.randint(5, 35)), date(2025, 12, 31))
                return_rows.append(
                    {
                        "return_id": return_id,
                        "order_id": order_id,
                        "order_item_id": candidate_item_id,
                        "return_date_id": int(return_date.strftime("%Y%m%d")),
                        "return_reason": reason,
                        "return_quantity": return_qty,
                        "return_amount": round(float(product["unit_price"]) * return_qty * 0.92, 2),
                        "resolution_status": rng.choice(["refunded", "store_credit", "replacement"]),
                    }
                )
                return_id += 1

        order_columns = [
            "order_id",
            "order_number",
            "customer_id",
            "order_date_id",
            "ship_date_id",
            "region_id",
            "channel_id",
            "order_status",
            "payment_method",
            "shipping_days",
            "subtotal",
            "discount_amount",
            "tax_amount",
            "shipping_amount",
            "total_amount",
        ]
        item_columns = [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "net_amount",
        ]
        return_columns = [
            "return_id",
            "order_id",
            "order_item_id",
            "return_date_id",
            "return_reason",
            "return_quantity",
            "return_amount",
            "resolution_status",
        ]

        return (
            pd.DataFrame(order_rows, columns=order_columns),
            pd.DataFrame(item_rows, columns=item_columns),
            pd.DataFrame(return_rows, columns=return_columns),
        )

    @staticmethod
    def _seasonality_multiplier(month: int) -> float:
        seasonal_map = {
            1: 0.92,
            2: 0.96,
            3: 1.0,
            4: 1.02,
            5: 1.03,
            6: 1.04,
            7: 1.1,
            8: 1.02,
            9: 0.98,
            10: 1.05,
            11: 1.2,
            12: 1.28,
        }
        return seasonal_map.get(month, 1.0)
