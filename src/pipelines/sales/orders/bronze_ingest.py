# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Sales Orders — Raw Data Ingestion
# MAGIC Fetches products, carts, and users from the DummyJSON REST API.
# MAGIC Writes raw Delta tables to the `b_salesorders` schema.

# COMMAND ----------

import requests
import uuid
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
)
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

BRONZE_SCHEMA = "b_salesorders"
API_BASE_URL = "https://dummyjson.com"
BATCH_ID = str(uuid.uuid4())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
print(f"Schema: {BRONZE_SCHEMA} | Batch: {BATCH_ID}")

# COMMAND ----------

# --- Explicit Schemas ---

ADDRESS_SCHEMA = StructType([
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postalCode", StringType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
    StructField("rating", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True),
])

CART_PRODUCT_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
])

CART_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(CART_PRODUCT_SCHEMA), False),
])

USER_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("username", StringType(), False),
    StructField("address", ADDRESS_SCHEMA, True),
])

# COMMAND ----------

# --- Fallback Sample Data ---

FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "Popular mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.28, "brand": "Glamour Beauty", "description": "Eyeshadow palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet Touch", "description": "Powder canister"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.01, "brand": "Glamour Beauty", "description": "Red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Nail Couture", "description": "Nail polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.85, "brand": "Calvin Klein", "description": "CK One fragrance"},
    {"id": 7, "title": "Samsung Galaxy S24", "price": 899.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Galaxy S24"},
    {"id": 8, "title": "iPhone 15 Pro", "price": 999.99, "category": "smartphones", "rating": 4.70, "brand": "Apple", "description": "iPhone 15 Pro"},
    {"id": 9, "title": "HP Pavilion 15", "price": 499.99, "category": "laptops", "rating": 4.20, "brand": "HP", "description": "HP Pavilion laptop"},
    {"id": 10, "title": "Dell XPS 13", "price": 1199.99, "category": "laptops", "rating": 4.60, "brand": "Dell", "description": "Dell XPS laptop"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 5, "total": 69.95, "products": [
        {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
        {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99},
        {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 2, "total": 29.98},
    ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 3, "total": 67.97, "products": [
        {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 1, "total": 49.99},
        {"id": 5, "title": "Red Nail Polish", "price": 8.99, "quantity": 2, "total": 17.98},
    ]},
    {"id": 3, "userId": 3, "totalProducts": 2, "totalQuantity": 2, "total": 1299.98, "products": [
        {"id": 8, "title": "iPhone 15 Pro", "price": 999.99, "quantity": 1, "total": 999.99},
        {"id": 9, "title": "HP Pavilion 15", "price": 499.99, "quantity": 1, "total": 499.99},
    ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@x.dummyjson.com", "phone": "+81 965-431-3024", "username": "emilys", "address": {"address": "626 Main Street", "city": "Phoenix", "state": "Mississippi", "postalCode": "29112"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@x.dummyjson.com", "phone": "+49 258-627-6644", "username": "michaelw", "address": {"address": "385 Fifth Street", "city": "Houston", "state": "Alabama", "postalCode": "38807"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@x.dummyjson.com", "phone": "+81 210-652-2785", "username": "sophiab", "address": {"address": "461 Cedar Street", "city": "Seattle", "state": "Pennsylvania", "postalCode": "80091"}},
]

# COMMAND ----------

# --- Clean Helper Functions ---

def clean_product(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a product record."""
    return {
        "id": int(raw.get("id", 0)),
        "title": str(raw.get("title", "")),
        "price": float(raw.get("price", 0.0)),
        "category": str(raw.get("category", "")),
        "rating": float(raw["rating"]) if raw.get("rating") is not None else None,
        "brand": str(raw["brand"]) if raw.get("brand") is not None else None,
        "description": str(raw["description"]) if raw.get("description") is not None else None,
    }


def clean_cart(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a cart record."""
    products = []
    for p in raw.get("products", []):
        products.append({
            "id": int(p.get("id", 0)),
            "title": str(p.get("title", "")),
            "price": float(p.get("price", 0.0)),
            "quantity": int(p.get("quantity", 0)),
            "total": float(p.get("total", 0.0)),
        })
    return {
        "id": int(raw.get("id", 0)),
        "userId": int(raw.get("userId", 0)),
        "totalProducts": int(raw.get("totalProducts", 0)),
        "totalQuantity": int(raw.get("totalQuantity", 0)),
        "total": float(raw.get("total", 0.0)),
        "products": products,
    }


def clean_user(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a user record."""
    addr = raw.get("address", {}) or {}
    return {
        "id": int(raw.get("id", 0)),
        "firstName": str(raw.get("firstName", "")),
        "lastName": str(raw.get("lastName", "")),
        "email": str(raw.get("email", "")),
        "phone": str(raw.get("phone", "")),
        "username": str(raw.get("username", "")),
        "address": {
            "address": str(addr.get("address", "")),
            "city": str(addr.get("city", "")),
            "state": str(addr.get("state", "")),
            "postalCode": str(addr.get("postalCode", "")),
        },
    }

# COMMAND ----------

# --- Fetch from API with Fallback ---

def fetch_from_api(endpoint: str, wrapper_key: str, fallback: list, clean_fn) -> list:
    """Fetch data from DummyJSON API; fall back to sample data on error."""
    try:
        url = f"{API_BASE_URL}{endpoint}"
        print(f"Fetching {url} ...")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        raw_items = resp.json().get(wrapper_key, [])
        print(f"  API returned {len(raw_items)} records")
        return [clean_fn(item) for item in raw_items]
    except Exception as e:
        print(f"  API error: {e} — using fallback data")
        return [clean_fn(item) for item in fallback]

# COMMAND ----------

# --- Ingest Products ---

products_data = fetch_from_api("/products", "products", FALLBACK_PRODUCTS, clean_product)
df_products = spark.createDataFrame(products_data, schema=PRODUCT_SCHEMA)
df_products = (df_products
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("dummyjson"))
    .withColumn("_batch_id", lit(BATCH_ID)))

df_products.write.mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.products")
print(f"Wrote {df_products.count()} products to {BRONZE_SCHEMA}.products")

# COMMAND ----------

display(df_products.limit(5))

# COMMAND ----------

# --- Ingest Carts ---

carts_data = fetch_from_api("/carts", "carts", FALLBACK_CARTS, clean_cart)
df_carts = spark.createDataFrame(carts_data, schema=CART_SCHEMA)
df_carts = (df_carts
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("dummyjson"))
    .withColumn("_batch_id", lit(BATCH_ID)))

df_carts.write.mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.carts")
print(f"Wrote {df_carts.count()} carts to {BRONZE_SCHEMA}.carts")

# COMMAND ----------

display(df_carts.limit(5))

# COMMAND ----------

# --- Ingest Users ---

users_data = fetch_from_api("/users", "users", FALLBACK_USERS, clean_user)
df_users = spark.createDataFrame(users_data, schema=USER_SCHEMA)
df_users = (df_users
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("dummyjson"))
    .withColumn("_batch_id", lit(BATCH_ID)))

df_users.write.mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.users")
print(f"Wrote {df_users.count()} users to {BRONZE_SCHEMA}.users")

# COMMAND ----------

display(df_users.limit(5))

# COMMAND ----------

# --- Summary ---

print("=" * 60)
print("BRONZE INGESTION COMPLETE")
print("=" * 60)
print(f"Schema: {BRONZE_SCHEMA}")
print(f"Batch ID: {BATCH_ID}")
for table in ["products", "carts", "users"]:
    count = spark.table(f"{BRONZE_SCHEMA}.{table}").count()
    print(f"  {BRONZE_SCHEMA}.{table}: {count} rows")
print("=" * 60)
