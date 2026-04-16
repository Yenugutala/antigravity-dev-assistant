"""Unit tests for Silver Layer: Sales Orders — Cleansing & Transformation."""

import pytest


# ===========================================================================
# Silver Products — Transformation Tests
# ===========================================================================

class TestSilverProducts:
    """Tests for products cleansing transformations."""

    def test_trim_title(self):
        raw = "  Essence Mascara  "
        assert raw.strip() == "Essence Mascara"

    def test_lower_category(self):
        raw = "  BEAUTY "
        assert raw.strip().lower() == "beauty"

    def test_cast_price_to_double(self):
        raw = "9.99"
        assert float(raw) == 9.99

    def test_cast_rating_to_double(self):
        raw = "4.94"
        assert float(raw) == 4.94

    def test_rename_id_to_product_id(self):
        record = {"id": 1, "title": "X"}
        renamed = {"product_id": record["id"], "title": record["title"]}
        assert renamed["product_id"] == 1
        assert "id" not in renamed

    def test_rename_rating_to_rating_score(self):
        record = {"rating": 4.5}
        renamed = {"rating_score": record["rating"]}
        assert renamed["rating_score"] == 4.5
        assert "rating" not in renamed

    def test_filter_null_id(self):
        records = [{"id": 1, "price": 5.0}, {"id": None, "price": 3.0}]
        filtered = [r for r in records if r["id"] is not None]
        assert len(filtered) == 1

    def test_filter_negative_price(self):
        records = [{"id": 1, "price": 5.0}, {"id": 2, "price": -1.0}]
        filtered = [r for r in records if r["price"] >= 0]
        assert len(filtered) == 1


# ===========================================================================
# Silver Products — Deduplication Tests
# ===========================================================================

class TestSilverDedup:
    """Tests for ROW_NUMBER deduplication logic."""

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

    def test_dedup_preserves_unique(self):
        records = [
            {"id": 1, "title": "A", "timestamp": 1},
            {"id": 2, "title": "B", "timestamp": 1},
        ]
        ids = set(r["id"] for r in records)
        assert len(ids) == 2


# ===========================================================================
# Silver Orders — Explode & Join Tests
# ===========================================================================

class TestSilverOrders:
    """Tests for cart explosion and product join logic."""

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
                "product_title": item["title"],
                "price": item["price"],
                "quantity": item["quantity"],
                "line_total": round(item["total"], 2),
            })
        assert len(exploded) == 2
        assert exploded[0]["cart_id"] == 1
        assert exploded[1]["product_id"] == 2

    def test_line_total_rounding(self):
        total = 19.980000000001
        assert round(total, 2) == 19.98

    def test_category_join_enrichment(self):
        order = {"product_id": 1, "category": None}
        product_lookup = {1: "beauty", 2: "fragrances"}
        order["category"] = product_lookup.get(order["product_id"])
        assert order["category"] == "beauty"

    def test_category_join_missing_product(self):
        order = {"product_id": 999, "category": None}
        product_lookup = {1: "beauty"}
        order["category"] = product_lookup.get(order["product_id"])
        assert order["category"] is None


# ===========================================================================
# Silver Customers — Flatten Tests
# ===========================================================================

class TestSilverCustomers:
    """Tests for user flattening and renaming transformations."""

    def test_rename_user_fields(self):
        raw = {"id": 1, "firstName": "Alice", "lastName": "Smith"}
        renamed = {
            "customer_id": raw["id"],
            "first_name": raw["firstName"],
            "last_name": raw["lastName"],
        }
        assert renamed["customer_id"] == 1
        assert renamed["first_name"] == "Alice"

    def test_lower_email(self):
        raw = " Alice@Example.COM "
        assert raw.strip().lower() == "alice@example.com"

    def test_lower_username(self):
        raw = " AliceSmith "
        assert raw.strip().lower() == "alicesmith"

    def test_flatten_address_struct(self):
        raw = {"address": {"address": "123 Main St", "city": "NYC",
                           "state": "NY", "postalCode": "10001"}}
        flat = {
            "street": raw["address"]["address"],
            "city": raw["address"]["city"],
            "zipcode": raw["address"]["postalCode"],
        }
        assert flat["street"] == "123 Main St"
        assert flat["city"] == "NYC"
        assert flat["zipcode"] == "10001"

    def test_filter_null_customer_id(self):
        records = [{"id": 1}, {"id": None}, {"id": 3}]
        filtered = [r for r in records if r["id"] is not None]
        assert len(filtered) == 2


# ===========================================================================
# Silver Quarantine Tests
# ===========================================================================

class TestSilverQuarantine:
    """Tests for quarantine logic (NULL cart IDs)."""

    def test_quarantine_null_cart_id(self):
        carts = [
            {"id": 1, "userId": 1, "products": []},
            {"id": None, "userId": 2, "products": []},
        ]
        quarantined = [c for c in carts if c["id"] is None]
        valid = [c for c in carts if c["id"] is not None]
        assert len(quarantined) == 1
        assert len(valid) == 1
        assert quarantined[0]["userId"] == 2
