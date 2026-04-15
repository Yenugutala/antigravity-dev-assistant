# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Sales Orders — Raw Data Ingestion
# MAGIC Ingests products, carts, and users from DummyJSON REST API into `b_salesorders` schema.

# COMMAND ----------

import requests
import uuid
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType,
    TimestampType
)
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# Configuration
API_BASE_URL = "https://dummyjson.com"
BRONZE_SCHEMA = "b_salesorders"
BATCH_ID = str(uuid.uuid4())
SOURCE_SYSTEM = "dummyjson_api"

# COMMAND ----------

# Create Bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# COMMAND ----------

# --- Explicit Spark Schemas ---

PRODUCT_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
    StructField("rating", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True),
])

CART_ITEM_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("total", DoubleType(), False),
])

CART_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(CART_ITEM_SCHEMA), False),
])

ADDRESS_SCHEMA = StructType([
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postalCode", StringType(), True),
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
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet Touch", "description": "Fine powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.01, "brand": "Glamour Beauty", "description": "Classic red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Nail Couture", "description": "Vibrant red polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.85, "brand": "Calvin Klein", "description": "Classic unisex fragrance"},
    {"id": 7, "title": "Chanel Coco Noir", "price": 129.99, "category": "fragrances", "rating": 4.65, "brand": "Chanel", "description": "Elegant fragrance"},
    {"id": 8, "title": "Samsung Galaxy S24", "price": 799.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Flagship phone"},
    {"id": 9, "title": "iPhone 15 Pro", "price": 999.99, "category": "smartphones", "rating": 4.70, "brand": "Apple", "description": "Premium smartphone"},
    {"id": 10, "title": "HP Pavilion 15", "price": 499.99, "category": "laptops", "rating": 4.20, "brand": "HP", "description": "Budget laptop"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 5, "total": 59.97, "products": [
        {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
        {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99},
        {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 2, "total": 29.98},
    ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 3, "total": 79.97, "products": [
        {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 1, "total": 49.99},
        {"id": 5, "title": "Red Nail Polish", "price": 8.99, "quantity": 2, "total": 17.98},
    ]},
    {"id": 3, "userId": 3, "totalProducts": 2, "totalQuantity": 2, "total": 1299.98, "products": [
        {"id": 8, "title": "Samsung Galaxy S24", "price": 799.99, "quantity": 1, "total": 799.99},
        {"id": 10, "title": "HP Pavilion 15", "price": 499.99, "quantity": 1, "total": 499.99},
    ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@example.com", "phone": "+1-555-0101", "username": "emilyjohnson", "address": {"address": "123 Main St", "city": "New York", "state": "NY", "postalCode": "10001"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@example.com", "phone": "+1-555-0102", "username": "michaelwilliams", "address": {"address": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "postalCode": "90001"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@example.com", "phone": "+1-555-0103", "username": "sophiabrown", "address": {"address": "789 Pine Rd", "city": "Chicago", "state": "IL", "postalCode": "60601"}},
]

# COMMAND ----------

# --- Data Cleaning Functions ---

def clean_product(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a product record."""
    return {
        "id": int(raw["id"]),
        "title": str(raw.get("title", "")),
        "price": float(raw.get("price", 0.0)),
        "category": str(raw.get("category", "")),
        "rating": float(raw["rating"]) if raw.get("rating") is not None else None,
        "brand": str(raw["brand"]) if raw.get("brand") is not None else None,
        "description": str(raw["description"]) if raw.get("description") is not None else None,
    }


def clean_cart(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a cart record."""
    cleaned_products = []
    for item in raw.get("products", []):
        cleaned_products.append({
            "id": int(item["id"]),
            "title": str(item.get("title", "")),
            "price": float(item.get("price", 0.0)),
            "quantity": int(item.get("quantity", 0)),
            "total": float(item.get("total", 0.0)),
        })
    return {
        "id": int(raw["id"]),
        "userId": int(raw["userId"]),
        "totalProducts": int(raw.get("totalProducts", 0)),
        "totalQuantity": int(raw.get("totalQuantity", 0)),
        "total": float(raw.get("total", 0.0)),
        "products": cleaned_products,
    }


def clean_user(raw: dict) -> dict:
    """Strip unnecessary fields and cast numerics for a user record."""
    addr = raw.get("address", {})
    return {
        "id": int(raw["id"]),
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

# --- API Fetch Helper ---

def fetch_from_api(endpoint: str, wrapper_key: str, fallback_data: list, clean_fn) -> list:
    """Fetch data from DummyJSON API with fallback on failure."""
    url = f"{API_BASE_URL}{endpoint}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        raw_data = response.json().get(wrapper_key, [])
        print(f"Fetched {len(raw_data)} records from {url}")
        return [clean_fn(record) for record in raw_data]
    except Exception as e:
        print(f"API call failed for {url}: {e}")
        print(f"Using fallback data ({len(fallback_data)} records)")
        return [clean_fn(record) for record in fallback_data]

# COMMAND ----------

# --- Ingest Products ---

print("Ingesting products...")
products_data = fetch_from_api("/products", "products", FALLBACK_PRODUCTS, clean_product)

products_df = spark.createDataFrame(products_data, schema=PRODUCT_SCHEMA)
products_df = (
    products_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE_SYSTEM))
    .withColumn("_batch_id", lit(BATCH_ID))
)

products_df.write.mode("overwrite").format("delta").saveAsTable(f"{BRONZE_SCHEMA}.products")
print(f"Wrote {products_df.count()} products to {BRONZE_SCHEMA}.products")
display(products_df.limit(5))

# COMMAND ----------

# --- Ingest Carts ---

print("Ingesting carts...")
carts_data = fetch_from_api("/carts", "carts", FALLBACK_CARTS, clean_cart)

carts_df = spark.createDataFrame(carts_data, schema=CART_SCHEMA)
carts_df = (
    carts_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE_SYSTEM))
    .withColumn("_batch_id", lit(BATCH_ID))
)

carts_df.write.mode("overwrite").format("delta").saveAsTable(f"{BRONZE_SCHEMA}.carts")
print(f"Wrote {carts_df.count()} carts to {BRONZE_SCHEMA}.carts")
display(carts_df.limit(5))

# COMMAND ----------

# --- Ingest Users ---

print("Ingesting users...")
users_data = fetch_from_api("/users", "users", FALLBACK_USERS, clean_user)

users_df = spark.createDataFrame(users_data, schema=USER_SCHEMA)
users_df = (
    users_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE_SYSTEM))
    .withColumn("_batch_id", lit(BATCH_ID))
)

users_df.write.mode("overwrite").format("delta").saveAsTable(f"{BRONZE_SCHEMA}.users")
print(f"Wrote {users_df.count()} users to {BRONZE_SCHEMA}.users")
display(users_df.limit(5))

# COMMAND ----------

# --- Summary ---

print("=" * 60)
print("BRONZE INGESTION COMPLETE")
print("=" * 60)
print(f"Schema: {BRONZE_SCHEMA}")
print(f"Batch ID: {BATCH_ID}")
print(f"Tables created:")
for table in ["products", "carts", "users"]:
    count = spark.table(f"{BRONZE_SCHEMA}.{table}").count()
    print(f"  {BRONZE_SCHEMA}.{table}: {count} rows")
print("=" * 60)
