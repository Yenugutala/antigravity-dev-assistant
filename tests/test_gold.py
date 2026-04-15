"""Unit tests for Gold Layer: Sales Orders — Business Aggregations."""

import pytest
from collections import defaultdict


# ===========================================================================
# Sample Silver Data (mirrors fallback data after Silver transformations)
# ===========================================================================

SILVER_ORDERS = [
    # Cart 1 — User 1 (beauty products)
    {"cart_id": 1, "user_id": 1, "order_date": "2026-04-15", "product_id": 1,
     "product_title": "Essence Mascara Lash Princess", "category": "beauty",
     "price": 9.99, "quantity": 2, "line_total": 19.98},
    {"cart_id": 1, "user_id": 1, "order_date": "2026-04-15", "product_id": 2,
     "product_title": "Eyeshadow Palette with Mirror", "category": "beauty",
     "price": 19.99, "quantity": 1, "line_total": 19.99},
    {"cart_id": 1, "user_id": 1, "order_date": "2026-04-15", "product_id": 3,
     "product_title": "Powder Canister", "category": "beauty",
     "price": 14.99, "quantity": 2, "line_total": 29.98},
    # Cart 2 — User 2 (fragrances + beauty)
    {"cart_id": 2, "user_id": 2, "order_date": "2026-04-15", "product_id": 6,
     "product_title": "Calvin Klein CK One", "category": "fragrances",
     "price": 49.99, "quantity": 1, "line_total": 49.99},
    {"cart_id": 2, "user_id": 2, "order_date": "2026-04-15", "product_id": 5,
     "product_title": "Red Nail Polish", "category": "beauty",
     "price": 8.99, "quantity": 2, "line_total": 17.98},
    # Cart 3 — User 3 (smartphones + laptops)
    {"cart_id": 3, "user_id": 3, "order_date": "2026-04-15", "product_id": 8,
     "product_title": "Samsung Galaxy S24", "category": "smartphones",
     "price": 799.99, "quantity": 1, "line_total": 799.99},
    {"cart_id": 3, "user_id": 3, "order_date": "2026-04-15", "product_id": 10,
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
            "avg_price": round(sum(i["price"] for i in items) / len(items), 2),
            "unique_products": len(set(i["product_id"] for i in items)),
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
            "avg_order_line_value": round(
                sum(i["line_total"] for i in items) / len(items), 2
            ),
        }
    return result


# ===========================================================================
# Revenue by Category Tests
# ===========================================================================

class TestRevenueByCategory:
    """Tests for g_salesorders.revenue_by_category aggregation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.revenue = compute_revenue_by_category(SILVER_ORDERS)

    def test_category_count(self):
        assert len(self.revenue) == 4  # beauty, fragrances, smartphones, laptops

    def test_beauty_revenue(self):
        # 19.98 + 19.99 + 29.98 + 17.98 = 87.93
        assert self.revenue["beauty"]["total_revenue"] == 87.93

    def test_fragrances_revenue(self):
        assert self.revenue["fragrances"]["total_revenue"] == 49.99

    def test_smartphones_revenue(self):
        assert self.revenue["smartphones"]["total_revenue"] == 799.99

    def test_laptops_revenue(self):
        assert self.revenue["laptops"]["total_revenue"] == 499.99

    def test_beauty_items_sold(self):
        # 2 + 1 + 2 + 2 = 7
        assert self.revenue["beauty"]["total_items_sold"] == 7

    def test_beauty_total_orders(self):
        # Cart 1 and Cart 2
        assert self.revenue["beauty"]["total_orders"] == 2

    def test_beauty_unique_products(self):
        # Products 1, 2, 3, 5
        assert self.revenue["beauty"]["unique_products"] == 4

    def test_highest_revenue_is_smartphones(self):
        sorted_cats = sorted(self.revenue.items(), key=lambda x: x[1]["total_revenue"], reverse=True)
        assert sorted_cats[0][0] == "smartphones"


# ===========================================================================
# Order Summary Tests
# ===========================================================================

class TestOrderSummary:
    """Tests for g_salesorders.order_summary aggregation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.summary = compute_order_summary(SILVER_ORDERS)

    def test_single_date(self):
        assert len(self.summary) == 1
        assert "2026-04-15" in self.summary

    def test_total_orders(self):
        day = self.summary["2026-04-15"]
        assert day["total_orders"] == 3  # 3 distinct cart_ids

    def test_unique_customers(self):
        day = self.summary["2026-04-15"]
        assert day["unique_customers"] == 3  # 3 distinct user_ids

    def test_total_revenue(self):
        day = self.summary["2026-04-15"]
        expected = 19.98 + 19.99 + 29.98 + 49.99 + 17.98 + 799.99 + 499.99
        assert day["total_revenue"] == round(expected, 2)

    def test_total_items(self):
        day = self.summary["2026-04-15"]
        # 2 + 1 + 2 + 1 + 2 + 1 + 1 = 10
        assert day["total_items"] == 10
