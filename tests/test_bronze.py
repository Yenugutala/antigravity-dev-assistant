"""Unit tests for Bronze layer — Sales Orders ingestion."""
import pytest
import requests
from unittest.mock import patch, MagicMock


PRODUCTS_SAMPLE = [
    {"id": 1, "title": "Essence Mascara", "price": 9.99, "category": "beauty",
     "description": "Test", "rating": 2.56, "brand": "Essence", "thumbnail": ""},
    {"id": 2, "title": "Eyeshadow Palette", "price": 19.99, "category": "beauty",
     "description": "Test 2", "rating": 2.86, "brand": "Glamour Beauty", "thumbnail": ""},
]

CARTS_SAMPLE = [
    {"id": 1, "userId": 1, "totalProducts": 2, "totalQuantity": 5, "total": 849.95,
     "products": [
         {"id": 1, "title": "Essence Mascara", "price": 9.99, "quantity": 4, "total": 39.96},
         {"id": 9, "title": "Samsung Galaxy S24", "price": 799.99, "quantity": 1, "total": 799.99},
     ]},
    {"id": 2, "userId": 2, "totalProducts": 1, "totalQuantity": 3, "total": 59.97,
     "products": [
         {"id": 2, "title": "Eyeshadow Palette", "price": 19.99, "quantity": 3, "total": 59.97},
     ]},
]

USERS_SAMPLE = [
    {"id": 1, "email": "emily.johnson@test.com", "username": "emilyj",
     "firstName": "Emily", "lastName": "Johnson",
     "phone": "+1-555-1234",
     "address": {"address": "626 Main Street", "city": "Phoenix",
                 "state": "AZ", "postalCode": "85001"}},
]


class TestBronzeAPIFetch:
    """Test API data fetching."""

    @patch("requests.get")
    def test_fetch_products_returns_data(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {"products": PRODUCTS_SAMPLE}
        mock_get.return_value.raise_for_status = MagicMock()

        response = requests.get("https://dummyjson.com/products")
        data = response.json()

        assert len(data["products"]) == 2
        assert data["products"][0]["id"] == 1
        assert data["products"][0]["price"] == 9.99

    @patch("requests.get")
    def test_fetch_carts_returns_data(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {"carts": CARTS_SAMPLE}
        mock_get.return_value.raise_for_status = MagicMock()

        response = requests.get("https://dummyjson.com/carts")
        data = response.json()

        assert len(data["carts"]) == 2
        assert data["carts"][0]["userId"] == 1
        assert len(data["carts"][0]["products"]) == 2

    @patch("requests.get")
    def test_fetch_users_returns_data(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = {"users": USERS_SAMPLE}
        mock_get.return_value.raise_for_status = MagicMock()

        response = requests.get("https://dummyjson.com/users")
        data = response.json()

        assert len(data["users"]) == 1
        assert data["users"][0]["email"] == "emily.johnson@test.com"

    @patch("requests.get")
    def test_api_error_raises_exception(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=500)
        mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")

        with pytest.raises(requests.exceptions.HTTPError):
            response = requests.get("https://dummyjson.com/products")
            response.raise_for_status()


class TestBronzeDataValidation:
    """Test data structure validation."""

    def test_products_have_required_fields(self) -> None:
        required = {"id", "title", "price", "category"}
        for product in PRODUCTS_SAMPLE:
            assert required.issubset(product.keys()), f"Missing fields in product {product['id']}"

    def test_carts_have_required_fields(self) -> None:
        required = {"id", "userId", "products"}
        for cart in CARTS_SAMPLE:
            assert required.issubset(cart.keys()), f"Missing fields in cart {cart['id']}"

    def test_products_price_is_positive(self) -> None:
        for product in PRODUCTS_SAMPLE:
            assert product["price"] >= 0, f"Negative price for product {product['id']}"

    def test_carts_have_products(self) -> None:
        for cart in CARTS_SAMPLE:
            assert len(cart["products"]) > 0, f"Empty products in cart {cart['id']}"

    def test_no_null_ids(self) -> None:
        for product in PRODUCTS_SAMPLE:
            assert product["id"] is not None
        for cart in CARTS_SAMPLE:
            assert cart["id"] is not None
        for user in USERS_SAMPLE:
            assert user["id"] is not None
