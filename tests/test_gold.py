"""Unit tests for Gold Layer: Sales Orders — Business Aggregations."""

import pytest
from collections import defaultdict


# ===========================================================================
# Sample Silver Data (mirrors fallback data after Silver transformations)
# ===========================================================================

SILVER_ORDERS = [
    {"cart_id": 1, "user_id": 1, "order_date": "2026-04-15", "product_id": 1,
     "product_title": "Essence Mascara Lash Princess", "category": "beauty",
     "price": 9.99, "quantity": 2, "line_total": 19.98},
    {"cart_id": 1, "user_id": 1, "order_date": "2026-04-15", "product_id": 2,
     "product_title": "Eyeshadow Palette with Mirror", "category": "beauty",
     "price": 19.99, "quantity": 1, "line_total": 19.99},
    {"cart_id": 1, "user_id": 1, "order_date": "2026-04-15", "product_id": 3,
     "product_title": "Powder Canister", "category": "beauty",
     "price": 14.99, "quantity": 2, "line_total": 29.98},
    {"cart_id": 2, "user_id": 2, "order_date": "2026-04-15", "product_id": 6,
     "product_title": "Calvin Klein CK One", "category": "fragrances",
     "price": 49.99, "quantity": 1, "line_total": 49.99},
    {"cart_id": 2, "user_id": 2, "order_date": "2026-04-15", "product_id": 5,
     "product_title": "Red Nail Polish", "category": "beauty",
     "price": 8.99, "quantity": 2, "line_total": 17.98},
    {"cart_id": 3, "user_id": 3, "order_date": "2026-04-15", "product_id": 8,
     "product_title": "iPhone 15 Pro", "category": "smartphones",
     "price": 999.99, "quantity": 1, "line_total": 999.99},
    {"cart_id": 3, "user_id": 3, "order_date": "2026-04-15", "product_id": 9,
     "product_title": "HP Pavilion 15", "category": "laptops",
     "price": 499.99, "quantity": 1, "line_total": 499.99},
]


# ===========================================================================
# Helper: Compute gold aggregations in pure Python
# ===========================================================================

def compute_revenue_by_category(orders: list) -> dict:
    """Aggregate revenue metrics grouped by category."""
    groups = defaultdict(list)
    for o in orders:
        groups[o["category"]].append(o)

    result = {}
    for cat, items in groups.items():
        result[cat] = {
            "total_revenue": round(sum(i["line_total"] for i in items), 2),
            "total_items_sold": sum(i["quantity"] for i in items),
            "total_orders": len(set(i["cart_id"] for i in items)),
        }
    return result


def compute_order_summary(orders: list) -> dict:
    """Aggregate order metrics grouped by order_date."""
    groups = defaultdict(list)
    for o in orders:
        groups[o["order_date"]].append(o)

    result = {}
    for date, items in groups.items():
        result[date] = {
            "total_orders": len(set(i["cart_id"] for i in items)),
            "unique_customers": len(set(i["user_id"] for i in items)),
            "total_revenue": round(sum(i["line_total"] for i in items), 2),
            "total_items": sum(i["quantity"] for i in items),
        }
    return result


# ===========================================================================
# Tests
# ===========================================================================

class TestRevenueByCategory:
    """Tests for revenue_by_category aggregation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.revenue = compute_revenue_by_category(SILVER_ORDERS)

    def test_category_count(self):
        assert len(self.revenue) == 4

    def test_beauty_revenue(self):
        # 19.98 + 19.99 + 29.98 + 17.98 = 87.93
        assert self.revenue["beauty"]["total_revenue"] == 87.93


class TestOrderSummary:
    """Tests for order_summary aggregation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.summary = compute_order_summary(SILVER_ORDERS)

    def test_total_revenue(self):
        day = self.summary["2026-04-15"]
        expected = 19.98 + 19.99 + 29.98 + 49.99 + 17.98 + 999.99 + 499.99
        assert day["total_revenue"] == round(expected, 2)

    def test_total_orders(self):
        day = self.summary["2026-04-15"]
        assert day["total_orders"] == 3

    def test_unique_customers(self):
        day = self.summary["2026-04-15"]
        assert day["unique_customers"] == 3
