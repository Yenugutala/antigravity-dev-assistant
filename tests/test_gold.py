"""Gold layer unit tests — pure Python with sample silver data."""


# ---- Sample silver data (mimics s_salesorders.orders) ----

SAMPLE_ORDERS = [
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-15", "product_id": 1,
     "product_title": "Mascara", "category": "beauty", "price": 10.0, "quantity": 2, "line_total": 40.00},
    {"cart_id": 1, "user_id": 1, "order_date": "2024-01-15", "product_id": 2,
     "product_title": "Palette", "category": "beauty", "price": 20.0, "quantity": 3, "line_total": 60.00},
    {"cart_id": 2, "user_id": 2, "order_date": "2024-01-15", "product_id": 6,
     "product_title": "CK One", "category": "fragrances", "price": 49.99, "quantity": 1, "line_total": 49.99},
    {"cart_id": 2, "user_id": 2, "order_date": "2024-01-15", "product_id": 7,
     "product_title": "Coco Noir", "category": "fragrances", "price": 129.99, "quantity": 1, "line_total": 129.99},
    {"cart_id": 3, "user_id": 3, "order_date": "2024-01-16", "product_id": 8,
     "product_title": "Galaxy S24", "category": "smartphones", "price": 799.99, "quantity": 1, "line_total": 799.99},
]


# ---- Helper aggregation functions mirroring gold SQL ----

def revenue_by_category(orders: list) -> dict:
    """Mirror g_salesorders.revenue_by_category aggregation."""
    categories: dict = {}
    for o in orders:
        cat = o["category"]
        if cat not in categories:
            categories[cat] = {
                "total_revenue": 0.0, "total_items_sold": 0,
                "cart_ids": set(), "prices": [], "product_ids": set(),
            }
        categories[cat]["total_revenue"] += o["line_total"]
        categories[cat]["total_items_sold"] += o["quantity"]
        categories[cat]["cart_ids"].add(o["cart_id"])
        categories[cat]["prices"].append(o["price"])
        categories[cat]["product_ids"].add(o["product_id"])

    result = {}
    for cat, d in categories.items():
        result[cat] = {
            "total_revenue": round(d["total_revenue"], 2),
            "total_items_sold": d["total_items_sold"],
            "total_orders": len(d["cart_ids"]),
            "avg_price": round(sum(d["prices"]) / len(d["prices"]), 2),
            "unique_products": len(d["product_ids"]),
        }
    return result


def order_summary(orders: list) -> dict:
    """Mirror g_salesorders.order_summary aggregation."""
    dates: dict = {}
    for o in orders:
        d = o["order_date"]
        if d not in dates:
            dates[d] = {
                "cart_ids": set(), "user_ids": set(),
                "total_revenue": 0.0, "total_items": 0, "line_totals": [],
            }
        dates[d]["cart_ids"].add(o["cart_id"])
        dates[d]["user_ids"].add(o["user_id"])
        dates[d]["total_revenue"] += o["line_total"]
        dates[d]["total_items"] += o["quantity"]
        dates[d]["line_totals"].append(o["line_total"])

    result = {}
    for d, data in dates.items():
        result[d] = {
            "total_orders": len(data["cart_ids"]),
            "unique_customers": len(data["user_ids"]),
            "total_revenue": round(data["total_revenue"], 2),
            "total_items": data["total_items"],
            "avg_order_line_value": round(
                sum(data["line_totals"]) / len(data["line_totals"]), 2
            ),
        }
    return result


# ---- Tests ----

class TestCategoryCount:
    def test_three_categories(self):
        result = revenue_by_category(SAMPLE_ORDERS)
        assert len(result) == 3


class TestBeautyRevenue:
    def test_beauty_revenue(self):
        result = revenue_by_category(SAMPLE_ORDERS)
        assert result["beauty"]["total_revenue"] == 100.00


class TestTotalRevenue:
    def test_total_revenue_across_categories(self):
        result = revenue_by_category(SAMPLE_ORDERS)
        total = sum(r["total_revenue"] for r in result.values())
        assert total == 1079.97


class TestTotalOrders:
    def test_total_distinct_orders(self):
        result = order_summary(SAMPLE_ORDERS)
        total = sum(r["total_orders"] for r in result.values())
        assert total == 3


class TestUniqueCustomers:
    def test_unique_customers_per_day(self):
        result = order_summary(SAMPLE_ORDERS)
        assert result["2024-01-15"]["unique_customers"] == 2
        assert result["2024-01-16"]["unique_customers"] == 1
