# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS b_salesorders

# COMMAND ----------

import requests
import uuid
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# ---- Configuration ----

BASE_URL = "https://dummyjson.com"
BATCH_ID = str(uuid.uuid4())
SOURCE_ID = "dummyjson_api"

# COMMAND ----------

# ---- Explicit Schemas (never rely on inference) ----

products_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
    StructField("rating", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True),
])

cart_item_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("total", DoubleType(), False),
])

carts_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(cart_item_schema), False),
])

address_schema = StructType([
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postalCode", StringType(), True),
])

users_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("username", StringType(), False),
    StructField("address", address_schema, True),
])

# COMMAND ----------

# ---- Clean Helper Functions ----
# Strip unnecessary fields, cast numerics to consistent types

def clean_product(raw: dict) -> dict:
    """Clean raw product: keep only spec fields, cast numerics to float."""
    return {
        "id": int(raw["id"]),
        "title": str(raw.get("title", "")),
        "price": float(raw.get("price", 0)),
        "category": str(raw.get("category", "")),
        "rating": float(raw["rating"]) if raw.get("rating") is not None else None,
        "brand": raw.get("brand"),
        "description": raw.get("description"),
    }


def clean_cart(raw: dict) -> dict:
    """Clean raw cart: strip extra fields, cast numerics, clean nested products."""
    return {
        "id": int(raw["id"]),
        "userId": int(raw.get("userId", 0)),
        "totalProducts": int(raw["totalProducts"]) if raw.get("totalProducts") is not None else None,
        "totalQuantity": int(raw["totalQuantity"]) if raw.get("totalQuantity") is not None else None,
        "total": float(raw["total"]) if raw.get("total") is not None else None,
        "products": [
            {
                "id": int(p["id"]),
                "title": str(p.get("title", "")),
                "price": float(p.get("price", 0)),
                "quantity": int(p.get("quantity", 0)),
                "total": float(p.get("total", 0)),
            }
            for p in raw.get("products", [])
        ],
    }


def clean_user(raw: dict) -> dict:
    """Clean raw user: flatten address, strip extra fields."""
    addr = raw.get("address", {})
    return {
        "id": int(raw["id"]),
        "firstName": raw.get("firstName"),
        "lastName": raw.get("lastName"),
        "email": str(raw.get("email", "")),
        "phone": raw.get("phone"),
        "username": str(raw.get("username", "")),
        "address": {
            "address": addr.get("address"),
            "city": addr.get("city"),
            "state": addr.get("state"),
            "postalCode": addr.get("postalCode"),
        } if addr else None,
    }

# COMMAND ----------

# ---- Fallback Sample Data ----
# Embedded so pipeline works even if API is unreachable

FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "Popular mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.28, "brand": "Glamour Beauty", "description": "Versatile eyeshadow palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet Touch", "description": "Fine setting powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.01, "brand": "Glamour Beauty", "description": "Classic red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Nail Couture", "description": "Vibrant red polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.17, "brand": "Calvin Klein", "description": "Classic unisex fragrance"},
    {"id": 7, "title": "Chanel Coco Noir Eau De", "price": 129.99, "category": "fragrances", "rating": 4.61, "brand": "Chanel", "description": "Elegant evening fragrance"},
    {"id": 8, "title": "Samsung Galaxy S24", "price": 799.99, "category": "smartphones", "rating": 4.52, "brand": "Samsung", "description": "Flagship smartphone"},
    {"id": 9, "title": "iPhone 15 Pro", "price": 999.99, "category": "smartphones", "rating": 4.81, "brand": "Apple", "description": "Premium smartphone"},
    {"id": 10, "title": "HP Pavilion 15", "price": 499.99, "category": "laptops", "rating": 4.11, "brand": "HP", "description": "Budget-friendly laptop"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 5, "total": 69.95, "products": [
        {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
        {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99},
        {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 2, "total": 29.98},
    ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 2, "total": 179.98, "products": [
        {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 1, "total": 49.99},
        {"id": 7, "title": "Chanel Coco Noir Eau De", "price": 129.99, "quantity": 1, "total": 129.99},
    ]},
    {"id": 3, "userId": 3, "totalProducts": 2, "totalQuantity": 2, "total": 1799.98, "products": [
        {"id": 8, "title": "Samsung Galaxy S24", "price": 799.99, "quantity": 1, "total": 799.99},
        {"id": 9, "title": "iPhone 15 Pro", "price": 999.99, "quantity": 1, "total": 999.99},
    ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@x.dummyjson.com", "phone": "+1-555-0101", "username": "emjohnson", "address": {"address": "626 Main Street", "city": "Phoenix", "state": "Mississippi", "postalCode": "29112"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@x.dummyjson.com", "phone": "+1-555-0102", "username": "mwilliams", "address": {"address": "385 Fifth Street", "city": "Houston", "state": "Alabama", "postalCode": "38807"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@x.dummyjson.com", "phone": "+1-555-0103", "username": "sbrown", "address": {"address": "1642 Ninth Street", "city": "Washington", "state": "Louisiana", "postalCode": "32822"}},
]

# COMMAND ----------

# ---- API Fetch with Fallback ----

def fetch_data(endpoint: str, wrapper_key: str) -> list:
    """Fetch data from DummyJSON API. Falls back to sample data on failure."""
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", timeout=30)
        response.raise_for_status()
        data = response.json().get(wrapper_key, [])
        if data:
            return data
    except Exception as e:
        print(f"API call failed for {endpoint}: {e}. Using fallback data.")

    fallbacks = {
        "products": FALLBACK_PRODUCTS,
        "carts": FALLBACK_CARTS,
        "users": FALLBACK_USERS,
    }
    return fallbacks.get(wrapper_key, [])

# COMMAND ----------

# ---- Ingest Products ----

raw_products = fetch_data("/products", "products")
cleaned_products = [clean_product(p) for p in raw_products]
source = SOURCE_ID if len(raw_products) > len(FALLBACK_PRODUCTS) else "fallback_sample"

df_products = spark.createDataFrame(cleaned_products, schema=products_schema)
df_products = (
    df_products
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(source))
    .withColumn("_batch_id", lit(BATCH_ID))
)
df_products.write.mode("overwrite").saveAsTable("b_salesorders.products")
print(f"Ingested {df_products.count()} products")
display(df_products.limit(5))

# COMMAND ----------

# ---- Ingest Carts ----

raw_carts = fetch_data("/carts", "carts")
cleaned_carts = [clean_cart(c) for c in raw_carts]
source = SOURCE_ID if len(raw_carts) > len(FALLBACK_CARTS) else "fallback_sample"

df_carts = spark.createDataFrame(cleaned_carts, schema=carts_schema)
df_carts = (
    df_carts
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(source))
    .withColumn("_batch_id", lit(BATCH_ID))
)
df_carts.write.mode("overwrite").saveAsTable("b_salesorders.carts")
print(f"Ingested {df_carts.count()} carts")
display(df_carts.limit(5))

# COMMAND ----------

# ---- Ingest Users ----

raw_users = fetch_data("/users", "users")
cleaned_users = [clean_user(u) for u in raw_users]
source = SOURCE_ID if len(raw_users) > len(FALLBACK_USERS) else "fallback_sample"

df_users = spark.createDataFrame(cleaned_users, schema=users_schema)
df_users = (
    df_users
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(source))
    .withColumn("_batch_id", lit(BATCH_ID))
)
df_users.write.mode("overwrite").saveAsTable("b_salesorders.users")
print(f"Ingested {df_users.count()} users")
display(df_users.limit(5))

# COMMAND ----------

print(f"Bronze ingestion complete. Batch ID: {BATCH_ID}")
print("Tables created: b_salesorders.products, b_salesorders.carts, b_salesorders.users")
