"""Silver layer unit tests — pure Python, no pipeline imports needed."""


# ---- Helper functions mirroring silver transformations ----

def trim_lower(value: str) -> str:
    """Mirror LOWER(TRIM(col)) transformation."""
    return value.strip().lower() if value else value


def cast_to_double(value) -> float:
    """Mirror CAST(col AS DOUBLE) transformation."""
    return float(value) if value is not None else None


def explode_cart_products(cart: dict) -> list:
    """Mirror LATERAL VIEW EXPLODE(products) — one row per line item."""
    rows = []
    for item in cart.get("products", []):
        rows.append({
            "cart_id": cart["id"],
            "user_id": cart["userId"],
            "product_id": item["id"],
            "product_title": item.get("title", ""),
            "price": float(item["price"]),
            "quantity": int(item["quantity"]),
            "line_total": round(float(item["total"]), 2),
        })
    return rows


def dedup_by_key(records: list, key: str, order_field: str) -> list:
    """Mirror ROW_NUMBER() OVER (PARTITION BY key ORDER BY field DESC) WHERE row_num = 1."""
    seen = {}
    for r in sorted(records, key=lambda x: x.get(order_field, ""), reverse=True):
        k = r[key]
        if k not in seen:
            seen[k] = r
    return list(seen.values())


# ---- Tests ----

class TestTrimLower:
    def test_trims_and_lowercases(self):
        assert trim_lower("  Beauty  ") == "beauty"
        assert trim_lower("FRAGRANCES") == "fragrances"
        assert trim_lower("  Smartphones ") == "smartphones"


class TestCastNumeric:
    def test_casts_to_double(self):
        assert cast_to_double(29) == 29.0
        assert cast_to_double("19.99") == 19.99
        assert cast_to_double(None) is None


class TestExplodeCart:
    def test_explodes_cart_items_to_rows(self):
        cart = {
            "id": 1, "userId": 42,
            "products": [
                {"id": 10, "title": "Mascara", "price": 9.99, "quantity": 2, "total": 19.98},
                {"id": 20, "title": "Palette", "price": 19.99, "quantity": 1, "total": 19.99},
            ],
        }
        rows = explode_cart_products(cart)
        assert len(rows) == 2
        assert rows[0]["cart_id"] == 1
        assert rows[0]["product_id"] == 10
        assert rows[0]["line_total"] == 19.98
        assert rows[1]["line_total"] == 19.99


class TestDedupKeepsLatest:
    def test_keeps_latest_by_timestamp(self):
        records = [
            {"product_id": 1, "price": 10.0, "_ts": "2024-01-01"},
            {"product_id": 1, "price": 12.0, "_ts": "2024-06-01"},
            {"product_id": 2, "price": 20.0, "_ts": "2024-03-01"},
        ]
        result = dedup_by_key(records, "product_id", "_ts")
        assert len(result) == 2
        p1 = next(r for r in result if r["product_id"] == 1)
        assert p1["price"] == 12.0  # latest record wins


class TestQuarantineNullCartId:
    def test_null_cart_id_quarantined(self):
        carts = [
            {"id": 1, "userId": 1, "total": 10.0},
            {"id": None, "userId": 2, "total": 20.0},
            {"id": 3, "userId": 3, "total": 30.0},
        ]
        quarantined = [c for c in carts if c["id"] is None]
        valid = [c for c in carts if c["id"] is not None]
        assert len(quarantined) == 1
        assert quarantined[0]["userId"] == 2
        assert len(valid) == 2
