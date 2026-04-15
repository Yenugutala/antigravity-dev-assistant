"""Unit tests for Gold layer — aggregation logic."""
import pytest
from collections import defaultdict


SAMPLE_ORDERS = [
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-01", "product_id": 1,
     "category": "beauty", "price": 9.99, "quantity": 4, "line_total": 39.96},
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-01", "product_id": 9,
     "category": "smartphones", "price": 799.99, "quantity": 1, "line_total": 799.99},
    {"cart_id": 2, "user_id": 2, "order_date": "2024-01-02", "product_id": 2,
     "category": "beauty", "price": 19.99, "quantity": 3, "line_total": 59.97},
    {"cart_id": 3, "user_id": 1, "order_date": "2024-01-02", "product_id": 11,
     "category": "laptops", "price": 499.99, "quantity": 1, "line_total": 499.99},
]


class TestGoldRevenueByCategory:
    """Test revenue_by_category aggregation."""

    def _aggregate_by_category(self) -> dict:
        result = defaultdict(lambda: {"total_revenue": 0, "total_items": 0, "orders": set()})
        for order in SAMPLE_ORDERS:
            cat = order["category"]
            result[cat]["total_revenue"] += order["line_total"]
            result[cat]["total_items"] += order["quantity"]
            result[cat]["orders"].add(order["cart_id"])
        return result

    def test_category_count(self) -> None:
        result = self._aggregate_by_category()
        assert len(result) == 3  # beauty, smartphones, laptops

    def test_beauty_revenue(self) -> None:
        result = self._aggregate_by_category()
        assert round(result["beauty"]["total_revenue"], 2) == 100.00

    def test_smartphones_revenue(self) -> None:
        result = self._aggregate_by_category()
        assert round(result["smartphones"]["total_revenue"], 2) == 799.99

    def test_beauty_items_sold(self) -> None:
        result = self._aggregate_by_category()
        assert result["beauty"]["total_items"] == 7  # 4 + 3

    def test_beauty_order_count(self) -> None:
        result = self._aggregate_by_category()
        assert len(result["beauty"]["orders"]) == 2  # cart 1 and 2

    def test_revenue_is_non_negative(self) -> None:
        result = self._aggregate_by_category()
        for cat, metrics in result.items():
            assert metrics["total_revenue"] >= 0, f"Negative revenue for {cat}"


class TestGoldOrderSummary:
    """Test order_summary aggregation."""

    def _aggregate_by_date(self) -> dict:
        result = defaultdict(lambda: {"orders": set(), "customers": set(),
                                       "revenue": 0, "items": 0})
        for order in SAMPLE_ORDERS:
            date = order["order_date"]
            result[date]["orders"].add(order["cart_id"])
            result[date]["customers"].add(order["user_id"])
            result[date]["revenue"] += order["line_total"]
            result[date]["items"] += order["quantity"]
        return result

    def test_date_count(self) -> None:
        result = self._aggregate_by_date()
        assert len(result) == 2  # 2024-01-01, 2024-01-02

    def test_day1_orders(self) -> None:
        result = self._aggregate_by_date()
        assert len(result["2024-01-01"]["orders"]) == 1  # cart 1

    def test_day2_unique_customers(self) -> None:
        result = self._aggregate_by_date()
        assert len(result["2024-01-02"]["customers"]) == 2  # user 1 and 2

    def test_total_revenue_across_days(self) -> None:
        result = self._aggregate_by_date()
        total = sum(d["revenue"] for d in result.values())
        assert round(total, 2) == 1399.91

    def test_orders_greater_than_zero(self) -> None:
        result = self._aggregate_by_date()
        for date, metrics in result.items():
            assert len(metrics["orders"]) > 0, f"No orders on {date}"
