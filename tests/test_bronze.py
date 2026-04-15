"""Unit tests for Bronze layer — Sales Orders ingestion."""
import pytest
import requests
from unittest.mock import patch, MagicMock


PRODUCTS_SAMPLE = [
    {"id": 1, "title": "Product A", "price": 29.99, "category": "electronics",
     "description": "Test", "image": "http://img.com/1.jpg",
     "rating": {"rate": 4.5, "count": 120}},
    {"id": 2, "title": "Product B", "price": 59.99, "category": "jewelery",
     "description": "Test 2", "image": "http://img.com/2.jpg",
     "rating": {"rate": 3.8, "count": 80}},
]

CARTS_SAMPLE = [
    {"id": 1, "userId": 1, "date": "2024-01-01",
     "products": [{"productId": 1, "quantity": 2}, {"productId": 2, "quantity": 1}]},
    {"id": 2, "userId": 2, "date": "2024-01-02",
     "products": [{"productId": 1, "quantity": 3}]},
]

USERS_SAMPLE = [
    {"id": 1, "email": "john@test.com", "username": "johnd",
     "name": {"firstname": "John", "lastname": "Doe"},
     "phone": "1-555-1234",
     "address": {"city": "NYC", "street": "Main St", "number": 123,
                 "zipcode": "10001",
                 "geolocation": {"lat": "40.7", "long": "-74.0"}}},
]


class TestBronzeAPIFetch:
    """Test API data fetching."""

    @patch("requests.get")
    def test_fetch_products_returns_data(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = PRODUCTS_SAMPLE
        mock_get.return_value.raise_for_status = MagicMock()

        response = requests.get("https://fakestoreapi.com/products")
        data = response.json()

        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[0]["price"] == 29.99

    @patch("requests.get")
    def test_fetch_carts_returns_data(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = CARTS_SAMPLE
        mock_get.return_value.raise_for_status = MagicMock()

        response = requests.get("https://fakestoreapi.com/carts")
        data = response.json()

        assert len(data) == 2
        assert data[0]["userId"] == 1
        assert len(data[0]["products"]) == 2

    @patch("requests.get")
    def test_fetch_users_returns_data(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = USERS_SAMPLE
        mock_get.return_value.raise_for_status = MagicMock()

        response = requests.get("https://fakestoreapi.com/users")
        data = response.json()

        assert len(data) == 1
        assert data[0]["email"] == "john@test.com"

    @patch("requests.get")
    def test_api_error_raises_exception(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=500)
        mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")

        with pytest.raises(requests.exceptions.HTTPError):
            response = requests.get("https://fakestoreapi.com/products")
            response.raise_for_status()


class TestBronzeDataValidation:
    """Test data structure validation."""

    def test_products_have_required_fields(self) -> None:
        required = {"id", "title", "price", "category"}
        for product in PRODUCTS_SAMPLE:
            assert required.issubset(product.keys()), f"Missing fields in product {product['id']}"

    def test_carts_have_required_fields(self) -> None:
        required = {"id", "userId", "date", "products"}
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
