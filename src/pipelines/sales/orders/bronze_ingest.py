# Databricks notebook source

# COMMAND ----------

# Bronze Layer: Sales Orders — Raw Data Ingestion
# Fetches products, carts, and users from DummyJSON API
# Writes raw data to b_salesorders schema as Delta tables

# COMMAND ----------

import requests
import uuid
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

# COMMAND ----------

# Configuration
API_BASE_URL = "https://dummyjson.com"
BRONZE_SCHEMA = "b_salesorders"
BATCH_ID = str(uuid.uuid4())
INGESTION_TS = datetime.utcnow().isoformat()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# COMMAND ----------

# ── Explicit Schemas ──────────────────────────────────────────────

product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
    StructField("rating", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False),
])

cart_item_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("total", DoubleType(), False),
])

cart_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(cart_item_schema), False),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False),
])

address_schema = StructType([
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postalCode", StringType(), True),
])

user_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("username", StringType(), False),
    StructField("address", address_schema, True),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False),
])

# COMMAND ----------

# ── Fallback Sample Data ─────────────────────────────────────────

FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "Popular mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.28, "brand": "Glamour", "description": "Eyeshadow palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet", "description": "Fine powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.11, "brand": "Glamour", "description": "Classic red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Glamour", "description": "Long-lasting nail polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.71, "brand": "Calvin Klein", "description": "Classic unisex fragrance"},
    {"id": 7, "title": "Samsung Galaxy S21", "price": 799.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Flagship smartphone"},
    {"id": 8, "title": "iPhone 13", "price": 999.99, "category": "smartphones", "rating": 4.69, "brand": "Apple", "description": "Latest iPhone model"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 5, "total": 100.0,
     "products": [
         {"id": 1, "title": "Essence Mascara", "price": 9.99, "quantity": 2, "total": 19.98},
         {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 1, "total": 14.99},
         {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 2, "total": 99.98},
     ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 3, "total": 50.0,
     "products": [
         {"id": 2, "title": "Eyeshadow Palette", "price": 19.99, "quantity": 1, "total": 19.99},
         {"id": 5, "title": "Red Nail Polish", "price": 8.99, "quantity": 2, "total": 17.98},
     ]},
    {"id": 3, "userId": 3, "totalProducts": 2, "totalQuantity": 2, "total": 1799.98,
     "products": [
         {"id": 7, "title": "Samsung Galaxy S21", "price": 799.99, "quantity": 1, "total": 799.99},
         {"id": 8, "title": "iPhone 13", "price": 999.99, "quantity": 1, "total": 999.99},
     ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@x.dummyjson.com", "phone": "+1-555-0101", "username": "emilys",
     "address": {"address": "626 Main Street", "city": "Phoenix", "state": "Mississippi", "postalCode": "29112"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@x.dummyjson.com", "phone": "+1-555-0102", "username": "michaelw",
     "address": {"address": "385 Fifth Street", "city": "Houston", "state": "Alabama", "postalCode": "38807"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@x.dummyjson.com", "phone": "+1-555-0103", "username": "sophiab",
     "address": {"address": "100 Broadway", "city": "New York", "state": "New York", "postalCode": "10001"}},
]

# COMMAND ----------

def fetch_api_data(endpoint: str, wrapper_key: str) -> list:
    """Fetch data from DummyJSON API with fallback to sample data."""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        records = data.get(wrapper_key, [])
        if records:
            print(f"Fetched {len(records)} {endpoint} from API")
            return records
    except Exception as e:
        print(f"API call failed for {endpoint}: {e}")

    fallback_map = {
        "products": FALLBACK_PRODUCTS,
        "carts": FALLBACK_CARTS,
        "users": FALLBACK_USERS,
    }
    fallback = fallback_map.get(endpoint, [])
    print(f"Using fallback sample data ({len(fallback)} {endpoint})")
    return fallback

# COMMAND ----------

def clean_product(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a product record."""
    return {
        "id": int(raw["id"]),
        "title": str(raw.get("title", "")),
        "price": float(raw.get("price", 0)),
        "category": str(raw.get("category", "")),
        "rating": float(raw.get("rating", 0)) if raw.get("rating") is not None else None,
        "brand": str(raw.get("brand", "")) if raw.get("brand") is not None else None,
        "description": str(raw.get("description", "")) if raw.get("description") is not None else None,
        "_ingestion_timestamp": INGESTION_TS,
        "_source": "dummyjson_api",
        "_batch_id": BATCH_ID,
    }

# COMMAND ----------

def clean_cart(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a cart record."""
    items = []
    for p in raw.get("products", []):
        items.append({
            "id": int(p["id"]),
            "title": str(p.get("title", "")),
            "price": float(p.get("price", 0)),
            "quantity": int(p.get("quantity", 0)),
            "total": float(p.get("total", 0)),
        })
    return {
        "id": int(raw["id"]),
        "userId": int(raw["userId"]),
        "totalProducts": int(raw.get("totalProducts", 0)),
        "totalQuantity": int(raw.get("totalQuantity", 0)),
        "total": float(raw.get("total", 0)),
        "products": items,
        "_ingestion_timestamp": INGESTION_TS,
        "_source": "dummyjson_api",
        "_batch_id": BATCH_ID,
    }

# COMMAND ----------

def clean_user(raw: dict) -> dict:
    """Strip unnecessary fields and normalize a user record."""
    addr = raw.get("address", {})
    return {
        "id": int(raw["id"]),
        "firstName": str(raw.get("firstName", "")),
        "lastName": str(raw.get("lastName", "")),
        "email": str(raw.get("email", "")),
        "phone": str(raw.get("phone", "")) if raw.get("phone") is not None else None,
        "username": str(raw.get("username", "")),
        "address": {
            "address": str(addr.get("address", "")),
            "city": str(addr.get("city", "")),
            "state": str(addr.get("state", "")),
            "postalCode": str(addr.get("postalCode", "")),
        },
        "_ingestion_timestamp": INGESTION_TS,
        "_source": "dummyjson_api",
        "_batch_id": BATCH_ID,
    }

# COMMAND ----------

# ── Ingest Products ──────────────────────────────────────────────

raw_products = fetch_api_data("products", "products")
cleaned_products = [clean_product(p) for p in raw_products]

df_products = spark.createDataFrame(cleaned_products, schema=product_schema)
df_products.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.products")

print(f"Wrote {df_products.count()} rows to {BRONZE_SCHEMA}.products")
display(df_products.limit(5))

# COMMAND ----------

# ── Ingest Carts ─────────────────────────────────────────────────

raw_carts = fetch_api_data("carts", "carts")
cleaned_carts = [clean_cart(c) for c in raw_carts]

df_carts = spark.createDataFrame(cleaned_carts, schema=cart_schema)
df_carts.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.carts")

print(f"Wrote {df_carts.count()} rows to {BRONZE_SCHEMA}.carts")
display(df_carts.limit(5))

# COMMAND ----------

# ── Ingest Users ─────────────────────────────────────────────────

raw_users = fetch_api_data("users", "users")
cleaned_users = [clean_user(u) for u in raw_users]

df_users = spark.createDataFrame(cleaned_users, schema=user_schema)
df_users.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.users")

print(f"Wrote {df_users.count()} rows to {BRONZE_SCHEMA}.users")
display(df_users.limit(5))

# COMMAND ----------

print("=" * 60)
print("Bronze ingestion complete!")
print(f"  Schema: {BRONZE_SCHEMA}")
print(f"  Batch ID: {BATCH_ID}")
print("  Tables: products, carts, users")
print("=" * 60)
