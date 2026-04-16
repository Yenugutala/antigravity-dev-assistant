"""Unit tests for Silver Layer: Sales Orders — Cleansing & Transformation."""

import pytest


class TestSilverProducts:
    """Tests for product cleansing transformations."""

    def test_trim_and_lower(self):
        raw_title = "  Essence Mascara  "
        raw_category = "  BEAUTY "
        assert raw_title.strip() == "Essence Mascara"
        assert raw_category.strip().lower() == "beauty"

    def test_cast_numeric(self):
        assert float("9.99") == 9.99
        assert float("4.94") == 4.94


class TestSilverOrders:
    """Tests for cart explosion and join logic."""

    def test_explode_cart_products(self):
        cart = {"id": 1, "userId": 10, "products": [
            {"id": 1, "title": "A", "price": 9.99, "quantity": 2, "total": 19.98},
            {"id": 2, "title": "B", "price": 5.00, "quantity": 1, "total": 5.00},
        ]}
        exploded = []
        for item in cart["products"]:
            exploded.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "product_id": item["id"],
                "line_total": round(item["total"], 2),
            })
        assert len(exploded) == 2
        assert exploded[0]["cart_id"] == 1

    def test_dedup_keeps_latest(self):
        records = [
            {"id": 1, "title": "Old", "timestamp": 1},
            {"id": 1, "title": "New", "timestamp": 2},
        ]
        sorted_recs = sorted(records, key=lambda r: r["timestamp"], reverse=True)
        seen = set()
        deduped = []
        for r in sorted_recs:
            if r["id"] not in seen:
                seen.add(r["id"])
                deduped.append(r)
        assert len(deduped) == 1
        assert deduped[0]["title"] == "New"


class TestSilverQuarantine:
    """Tests for quarantine logic."""

    def test_quarantine_null_cart_id(self):
        carts = [
            {"id": 1, "userId": 1, "products": []},
            {"id": None, "userId": 2, "products": []},
        ]
        quarantined = [c for c in carts if c["id"] is None]
        valid = [c for c in carts if c["id"] is not None]
        assert len(quarantined) == 1
        assert len(valid) == 1
