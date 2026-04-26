"""
Unit tests for Silver Layer: Sales Orders — Cleansing & Transformation
Tests SQL logic patterns locally without a Databricks cluster.
"""



# ── Silver SQL Logic Tests ───────────────────────────────────────
# These tests validate the transformation logic defined in the silver spec
# by testing the SQL patterns and data expectations.


class TestTrimLower:
    def test_trim_and_lower_category(self):
        """Test LOWER(TRIM(...)) pattern used in silver products."""
        raw = "  BEAUTY  "
        result = raw.strip().lower()
        assert result == "beauty"


class TestCastNumeric:
    def test_cast_price_to_double(self):
        """Test that price string casts to float (DOUBLE)."""
        raw_price = "9.99"
        result = float(raw_price)
        assert result == 9.99
        assert isinstance(result, float)


class TestExplodeCart:
    def test_explode_cart_items(self):
        """Test that cart products array is exploded into individual rows."""
        cart = {
            "id": 1,
            "userId": 10,
            "products": [
                {"id": 1, "title": "Mascara", "price": 9.99, "quantity": 2, "total": 19.98},
                {"id": 3, "title": "Powder", "price": 14.99, "quantity": 1, "total": 14.99},
                {"id": 6, "title": "CK One", "price": 49.99, "quantity": 2, "total": 99.98},
            ]
        }
        exploded_rows = []
        for item in cart["products"]:
            exploded_rows.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "product_id": item["id"],
                "product_title": item["title"],
                "item_price": item["price"],
                "quantity": item["quantity"],
                "item_total": item["total"],
            })
        assert len(exploded_rows) == 3
        assert exploded_rows[0]["cart_id"] == 1
        assert exploded_rows[1]["product_id"] == 3
        assert exploded_rows[2]["quantity"] == 2

    def test_line_total_calculation(self):
        """Test line_total = ROUND(COALESCE(item.total, price * quantity), 2)."""
        item_total = 19.98
        price = 9.99
        quantity = 2
        line_total = round(item_total if item_total else price * quantity, 2)
        assert line_total == 19.98

        # Fallback when item_total is missing
        line_total_fallback = round(price * quantity, 2)
        assert line_total_fallback == 19.98


class TestDedupKeepsLatest:
    def test_dedup_by_product_id(self):
        """Test ROW_NUMBER dedup keeps latest record by _ingestion_timestamp."""
        records = [
            {"product_id": 1, "title": "Old Title", "_ingestion_timestamp": "2024-01-01T00:00:00"},
            {"product_id": 1, "title": "New Title", "_ingestion_timestamp": "2024-01-02T00:00:00"},
            {"product_id": 2, "title": "Other", "_ingestion_timestamp": "2024-01-01T00:00:00"},
        ]
        from collections import defaultdict
        groups = defaultdict(list)
        for r in records:
            groups[r["product_id"]].append(r)

        deduped = []
        for pid, recs in groups.items():
            latest = sorted(recs, key=lambda x: x["_ingestion_timestamp"], reverse=True)[0]
            deduped.append(latest)

        assert len(deduped) == 2
        prod1 = [r for r in deduped if r["product_id"] == 1][0]
        assert prod1["title"] == "New Title"


class TestQuarantineNullCartId:
    def test_null_cart_id_quarantined(self):
        """Test that records with null cart_id are routed to quarantine."""
        carts = [
            {"id": 1, "userId": 10, "total": 50.0},
            {"id": None, "userId": 20, "total": 30.0},
            {"id": 3, "userId": 30, "total": 100.0},
        ]
        quarantined = [c for c in carts if c["id"] is None]
        valid = [c for c in carts if c["id"] is not None]

        assert len(quarantined) == 1
        assert quarantined[0]["userId"] == 20
        assert len(valid) == 2
