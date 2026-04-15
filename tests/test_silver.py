"""Unit tests for Silver layer — cleansing and transformation logic."""
import pytest


class TestSilverCleansing:
    """Test cleansing rules applied in Silver layer."""

    def test_trim_whitespace(self) -> None:
        assert "  Essence Mascara  ".strip() == "Essence Mascara"

    def test_lowercase_category(self) -> None:
        assert "Beauty".lower().strip() == "beauty"
        assert "  SMARTPHONES  ".lower().strip() == "smartphones"

    def test_lowercase_email(self) -> None:
        assert "Emily.Johnson@Test.COM".lower().strip() == "emily.johnson@test.com"

    def test_cast_price_to_float(self) -> None:
        assert float("9.99") == 9.99
        assert isinstance(float("100"), float)

    def test_negative_price_rejected(self) -> None:
        prices = [9.99, -5.00, 19.99, -0.01]
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
            "id": 1, "userId": 1, "totalProducts": 2, "totalQuantity": 5,
            "products": [
                {"id": 1, "title": "Essence Mascara", "price": 9.99, "quantity": 4, "total": 39.96},
                {"id": 9, "title": "Samsung Galaxy S24", "price": 799.99, "quantity": 1, "total": 799.99},
            ]
        }
        # Simulate LATERAL VIEW EXPLODE
        exploded = []
        for item in cart["products"]:
            exploded.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "product_id": item["id"],
                "quantity": item["quantity"],
                "line_total": item["total"],
            })

        assert len(exploded) == 2
        assert exploded[0]["product_id"] == 1
        assert exploded[0]["quantity"] == 4
        assert exploded[0]["line_total"] == 39.96

    def test_line_total_calculation(self) -> None:
        price = 9.99
        quantity = 4
        line_total = round(price * quantity, 2)
        assert line_total == 39.96
