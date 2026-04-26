"""
Unit tests for Gold Layer: Sales Orders — Business Aggregations
Tests aggregation logic patterns locally without a Databricks cluster.
"""



# ── Sample Silver Orders Data ────────────────────────────────────
# Mirrors s_salesorders.orders after silver cleansing

SAMPLE_ORDERS = [
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-15", "product_id": 1,
     "product_title": "Essence Mascara", "category": "beauty",
     "price": 9.99, "quantity": 2, "line_total": 19.98},
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-15", "product_id": 3,
     "product_title": "Powder Canister", "category": "beauty",
     "price": 14.99, "quantity": 1, "line_total": 14.99},
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-15", "product_id": 6,
     "product_title": "Calvin Klein CK One", "category": "fragrances",
     "price": 49.99, "quantity": 2, "line_total": 99.98},
    {"cart_id": 2, "user_id": 2, "order_date": "2024-01-15", "product_id": 2,
     "product_title": "Eyeshadow Palette", "category": "beauty",
     "price": 19.99, "quantity": 1, "line_total": 19.99},
    {"cart_id": 2, "user_id": 2, "order_date": "2024-01-15", "product_id": 5,
     "product_title": "Red Nail Polish", "category": "beauty",
     "price": 8.99, "quantity": 2, "line_total": 17.98},
    {"cart_id": 3, "user_id": 3, "order_date": "2024-01-15", "product_id": 7,
     "product_title": "Samsung Galaxy S21", "category": "smartphones",
     "price": 799.99, "quantity": 1, "line_total": 799.99},
    {"cart_id": 3, "user_id": 3, "order_date": "2024-01-15", "product_id": 8,
     "product_title": "iPhone 13", "category": "smartphones",
     "price": 999.99, "quantity": 1, "line_total": 999.99},
]


def _revenue_by_category(orders: list) -> dict:
    """Simulate g_salesorders.revenue_by_category aggregation."""
    from collections import defaultdict
    agg = defaultdict(lambda: {"total_revenue": 0.0, "total_items_sold": 0,
                                "cart_ids": set(), "prices": [], "product_ids": set()})
    for o in orders:
        cat = o["category"]
        agg[cat]["total_revenue"] += o["line_total"]
        agg[cat]["total_items_sold"] += o["quantity"]
        agg[cat]["cart_ids"].add(o["cart_id"])
        agg[cat]["prices"].append(o["price"])
        agg[cat]["product_ids"].add(o["product_id"])

    result = {}
    for cat, data in agg.items():
        result[cat] = {
            "total_revenue": round(data["total_revenue"], 2),
            "total_items_sold": data["total_items_sold"],
            "total_orders": len(data["cart_ids"]),
            "avg_price": round(sum(data["prices"]) / len(data["prices"]), 2),
            "unique_products": len(data["product_ids"]),
        }
    return result


def _order_summary(orders: list) -> dict:
    """Simulate g_salesorders.order_summary aggregation."""
    from collections import defaultdict
    agg = defaultdict(lambda: {"cart_ids": set(), "user_ids": set(),
                                "total_revenue": 0.0, "total_items": 0,
                                "line_totals": []})
    for o in orders:
        dt = o["order_date"]
        agg[dt]["cart_ids"].add(o["cart_id"])
        agg[dt]["user_ids"].add(o["user_id"])
        agg[dt]["total_revenue"] += o["line_total"]
        agg[dt]["total_items"] += o["quantity"]
        agg[dt]["line_totals"].append(o["line_total"])

    result = {}
    for dt, data in agg.items():
        result[dt] = {
            "total_orders": len(data["cart_ids"]),
            "unique_customers": len(data["user_ids"]),
            "total_revenue": round(data["total_revenue"], 2),
            "total_items": data["total_items"],
            "avg_order_line_value": round(
                sum(data["line_totals"]) / len(data["line_totals"]), 2
            ),
        }
    return result


# ── Tests ────────────────────────────────────────────────────────

class TestCategoryCount:
    def test_correct_number_of_categories(self):
        """Test that revenue_by_category produces the right number of categories."""
        result = _revenue_by_category(SAMPLE_ORDERS)
        assert len(result) == 3  # beauty, fragrances, smartphones


class TestBeautyRevenue:
    def test_beauty_total_revenue(self):
        """Test that beauty category total_revenue matches expected value."""
        orders_adjusted = [
            {"cart_id": 1, "user_id": 1, "order_date": "2024-01-15", "product_id": 1,
             "category": "beauty", "price": 25.0, "quantity": 2, "line_total": 50.0},
            {"cart_id": 2, "user_id": 2, "order_date": "2024-01-15", "product_id": 2,
             "category": "beauty", "price": 25.0, "quantity": 2, "line_total": 50.0},
        ]
        result = _revenue_by_category(orders_adjusted)
        assert result["beauty"]["total_revenue"] == 100.00


class TestTotalRevenue:
    def test_total_revenue_across_all_categories(self):
        """Test that sum of all category revenues equals total order line_totals."""
        result = _revenue_by_category(SAMPLE_ORDERS)
        total = sum(cat["total_revenue"] for cat in result.values())
        expected = round(sum(o["line_total"] for o in SAMPLE_ORDERS), 2)
        assert total == expected


class TestTotalOrders:
    def test_daily_total_orders(self):
        """Test that order_summary counts distinct cart_ids correctly."""
        result = _order_summary(SAMPLE_ORDERS)
        day = "2024-01-15"
        assert result[day]["total_orders"] == 3  # cart_ids: 1, 2, 3


class TestUniqueCustomers:
    def test_daily_unique_customers(self):
        """Test that order_summary counts distinct user_ids correctly."""
        result = _order_summary(SAMPLE_ORDERS)
        day = "2024-01-15"
        assert result[day]["unique_customers"] == 3  # user_ids: 1, 2, 3
