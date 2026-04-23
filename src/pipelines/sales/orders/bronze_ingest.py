# Databricks notebook source

# COMMAND ----------
# Bronze Ingestion: Sales Orders
# Fetches products, carts, and users from DummyJSON API
# Writes raw data to b_salesorders schema as Delta tables

# COMMAND ----------
import requests
import uuid
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, ArrayType
)

# COMMAND ----------
# Configuration
API_BASE_URL = "https://dummyjson.com"
SCHEMA_NAME = "b_salesorders"
BATCH_ID = str(uuid.uuid4())
INGESTION_TS = datetime.utcnow().isoformat()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

# COMMAND ----------
# ── Fallback Sample Data ──

FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "Popular mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.28, "brand": "Glamour Beauty", "description": "Versatile palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet Touch", "description": "Fine powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.55, "brand": "Glamour Beauty", "description": "Classic red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Nail Couture", "description": "Chip-resistant polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.70, "brand": "Calvin Klein", "description": "Classic unisex fragrance"},
    {"id": 7, "title": "Samsung Galaxy S24", "price": 799.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Latest smartphone"},
    {"id": 8, "title": "iPhone 15 Pro", "price": 999.99, "category": "smartphones", "rating": 4.80, "brand": "Apple", "description": "Premium smartphone"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 2, "totalQuantity": 3, "total": 39.97, "products": [
        {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
        {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99},
    ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 2, "total": 63.98, "products": [
        {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 1, "total": 14.99},
        {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 1, "total": 49.99},
    ]},
    {"id": 3, "userId": 3, "totalProducts": 1, "totalQuantity": 1, "total": 799.99, "products": [
        {"id": 7, "title": "Samsung Galaxy S24", "price": 799.99, "quantity": 1, "total": 799.99},
    ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@example.com", "phone": "+1-555-0101", "username": "emilyjohnson", "address": {"address": "123 Main St", "city": "New York", "state": "NY", "postalCode": "10001"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@example.com", "phone": "+1-555-0102", "username": "michaelw", "address": {"address": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "postalCode": "90001"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@example.com", "phone": "+1-555-0103", "username": "sophiab", "address": {"address": "789 Pine Rd", "city": "Chicago", "state": "IL", "postalCode": "60601"}},
]

# COMMAND ----------
# ── Helper Functions ──

def fetch_api_data(endpoint: str, wrapper_key: str) -> list:
    """Fetch data from DummyJSON API with fallback."""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        records = data.get(wrapper_key, [])
        if records:
            print(f"✓ Fetched {len(records)} {wrapper_key} from API")
            return records
    except Exception as e:
        print(f"⚠ API call failed for {endpoint}: {e}")
    return None


def clean_product(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for products."""
    return {
        "id": int(raw["id"]),
        "title": str(raw.get("title", "")),
        "price": float(raw.get("price", 0)),
        "category": str(raw.get("category", "")),
        "rating": float(raw.get("rating", 0)) if raw.get("rating") is not None else None,
        "brand": str(raw.get("brand", "")) if raw.get("brand") else None,
        "description": str(raw.get("description", "")) if raw.get("description") else None,
    }


def clean_cart(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for carts."""
    products = []
    for p in raw.get("products", []):
        products.append({
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
        "products": products,
    }


def clean_user(raw: dict) -> dict:
    """Strip unnecessary fields for users."""
    addr = raw.get("address", {})
    return {
        "id": int(raw["id"]),
        "firstName": str(raw.get("firstName", "")),
        "lastName": str(raw.get("lastName", "")),
        "email": str(raw.get("email", "")),
        "phone": str(raw.get("phone", "")) if raw.get("phone") else None,
        "username": str(raw.get("username", "")),
        "address": {
            "address": str(addr.get("address", "")),
            "city": str(addr.get("city", "")),
            "state": str(addr.get("state", "")),
            "postalCode": str(addr.get("postalCode", "")),
        },
    }

# COMMAND ----------
# ── Schemas ──

products_schema = StructType([
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
    StructField("title", StringType(), False),
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

users_schema = StructType([
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
# ── Ingest Products ──

raw_products = fetch_api_data("products", "products")
if raw_products is None:
    raw_products = FALLBACK_PRODUCTS
    source_label = "fallback_sample"
    print("Using fallback sample data for products")
else:
    source_label = "dummyjson_api"

cleaned_products = [clean_product(p) for p in raw_products]
for p in cleaned_products:
    p["_ingestion_timestamp"] = INGESTION_TS
    p["_source"] = source_label
    p["_batch_id"] = BATCH_ID

df_products = spark.createDataFrame(cleaned_products, schema=products_schema)
df_products.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.products")
print(f"✓ Wrote {df_products.count()} rows to {SCHEMA_NAME}.products")

# COMMAND ----------
# ── Ingest Carts ──

raw_carts = fetch_api_data("carts", "carts")
if raw_carts is None:
    raw_carts = FALLBACK_CARTS
    source_label = "fallback_sample"
    print("Using fallback sample data for carts")
else:
    source_label = "dummyjson_api"

cleaned_carts = [clean_cart(c) for c in raw_carts]
for c in cleaned_carts:
    c["_ingestion_timestamp"] = INGESTION_TS
    c["_source"] = source_label
    c["_batch_id"] = BATCH_ID

df_carts = spark.createDataFrame(cleaned_carts, schema=carts_schema)
df_carts.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.carts")
print(f"✓ Wrote {df_carts.count()} rows to {SCHEMA_NAME}.carts")

# COMMAND ----------
# ── Ingest Users ──

raw_users = fetch_api_data("users", "users")
if raw_users is None:
    raw_users = FALLBACK_USERS
    source_label = "fallback_sample"
    print("Using fallback sample data for users")
else:
    source_label = "dummyjson_api"

cleaned_users = [clean_user(u) for u in raw_users]
for u in cleaned_users:
    u["_ingestion_timestamp"] = INGESTION_TS
    u["_source"] = source_label
    u["_batch_id"] = BATCH_ID

df_users = spark.createDataFrame(cleaned_users, schema=users_schema)
df_users.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.users")
print(f"✓ Wrote {df_users.count()} rows to {SCHEMA_NAME}.users")

# COMMAND ----------
# ── Display Sample Data ──

print("\n── Products Sample ──")
spark.sql(f"SELECT id, title, price, category FROM {SCHEMA_NAME}.products LIMIT 5").show(truncate=False)

print("── Carts Sample ──")
spark.sql(f"SELECT id, userId, totalProducts, total FROM {SCHEMA_NAME}.carts LIMIT 5").show(truncate=False)

print("── Users Sample ──")
spark.sql(f"SELECT id, firstName, lastName, email FROM {SCHEMA_NAME}.users LIMIT 5").show(truncate=False)

print("✓ Bronze ingestion complete")
