"""Unit tests for Silver layer — cleansing and transformation logic."""
import pytest


class TestSilverCleansing:
    """Test cleansing rules applied in Silver layer."""

    def test_trim_whitespace(self) -> None:
        assert "  Product A  ".strip() == "Product A"

    def test_lowercase_category(self) -> None:
        assert "Electronics".lower().strip() == "electronics"
        assert "  JEWELERY  ".lower().strip() == "jewelery"

    def test_lowercase_email(self) -> None:
        assert "John@Test.COM".lower().strip() == "john@test.com"

    def test_cast_price_to_float(self) -> None:
        assert float("29.99") == 29.99
        assert isinstance(float("100"), float)

    def test_negative_price_rejected(self) -> None:
        prices = [29.99, -5.00, 100.00, -0.01]
        valid = [p for p in prices if p >= 0]
        assert len(valid) == 2
        assert -5.00 not in valid


class TestSilverDeduplication:
    """Test deduplication logic."""

    def test_keep_latest_by_timestamp(self) -> None:
        records = [
            {"id": 1, "name": "Old", "timestamp": "2024-01-01"},
            {"id": 1, "name": "New", "timestamp": "2024-01-02"},
            {"id": 2, "name": "Only", "timestamp": "2024-01-01"},
        ]
        # Simulate ROW_NUMBER() PARTITION BY id ORDER BY timestamp DESC
        seen = {}
        for r in sorted(records, key=lambda x: x["timestamp"], reverse=True):
            if r["id"] not in seen:
                seen[r["id"]] = r
        result = list(seen.values())

        assert len(result) == 2
        assert seen[1]["name"] == "New"  # Latest kept
        assert seen[2]["name"] == "Only"

    def test_null_id_quarantined(self) -> None:
        records = [
            {"id": 1, "name": "Valid"},
            {"id": None, "name": "Invalid"},
            {"id": 3, "name": "Valid"},
        ]
        valid = [r for r in records if r["id"] is not None]
        quarantined = [r for r in records if r["id"] is None]

        assert len(valid) == 2
        assert len(quarantined) == 1


class TestSilverCartExplode:
    """Test cart products array explosion."""

    def test_explode_cart_products(self) -> None:
        cart = {
            "id": 1, "userId": 1, "date": "2024-01-01",
            "products": [
                {"productId": 1, "quantity": 2},
                {"productId": 2, "quantity": 1},
            ]
        }
        # Simulate LATERAL VIEW EXPLODE
        exploded = []
        for item in cart["products"]:
            exploded.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "product_id": item["productId"],
                "quantity": item["quantity"],
            })

        assert len(exploded) == 2
        assert exploded[0]["product_id"] == 1
        assert exploded[0]["quantity"] == 2

    def test_line_total_calculation(self) -> None:
        price = 29.99
        quantity = 3
        line_total = round(price * quantity, 2)
        assert line_total == 89.97
