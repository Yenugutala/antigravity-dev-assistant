# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer — Sales Orders Ingestion
# MAGIC **Source**: DummyJSON API (https://dummyjson.com)
# MAGIC **Output**: products_bronze, carts_bronze, users_bronze (Delta tables)
# MAGIC **Pattern**: PySpark — API call → DataFrame → Delta table

# COMMAND ----------

import requests
import uuid
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType
)

# COMMAND ----------

# Configuration
BATCH_ID = str(uuid.uuid4())
SOURCE = "dummyjson"
API_BASE = "https://dummyjson.com"

print(f"Bronze Ingestion Started")
print(f"Batch ID: {BATCH_ID}")
print(f"Timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper — Fetch with Fallback

# COMMAND ----------

# Embedded fallback data — guarantees demo works even if API is down
FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 2.56, "brand": "Essence", "description": "Popular volumizing mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 2.86, "brand": "Glamour Beauty", "description": "Versatile palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 4.64, "brand": "Velvet Touch", "description": "Fine setting powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.36, "brand": "Chic Cosmetics", "description": "Classic red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 4.32, "brand": "Nail Couture", "description": "Vibrant red polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.85, "brand": "Calvin Klein", "description": "Classic unisex fragrance"},
    {"id": 7, "title": "Chanel Coco Noir", "price": 129.99, "category": "fragrances", "rating": 4.21, "brand": "Chanel", "description": "Elegant evening scent"},
    {"id": 8, "title": "Dior J'adore", "price": 89.99, "category": "fragrances", "rating": 4.62, "brand": "Dior", "description": "Iconic floral fragrance"},
    {"id": 9, "title": "Samsung Galaxy S24", "price": 799.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Flagship smartphone"},
    {"id": 10, "title": "iPhone 15 Pro", "price": 1099.99, "category": "smartphones", "rating": 4.75, "brand": "Apple", "description": "Premium smartphone"},
    {"id": 11, "title": "HP Pavilion 15", "price": 499.99, "category": "laptops", "rating": 4.10, "brand": "HP", "description": "Everyday laptop"},
    {"id": 12, "title": "Dell XPS 13", "price": 999.99, "category": "laptops", "rating": 4.60, "brand": "Dell", "description": "Ultra-thin laptop"},
    {"id": 13, "title": "Nike Air Max 270", "price": 129.99, "category": "mens-shoes", "rating": 4.45, "brand": "Nike", "description": "Comfortable running shoes"},
    {"id": 14, "title": "Adidas Ultraboost", "price": 149.99, "category": "mens-shoes", "rating": 4.55, "brand": "Adidas", "description": "Premium running shoes"},
    {"id": 15, "title": "Gucci Bloom Dress", "price": 79.99, "category": "womens-dresses", "rating": 3.90, "brand": "Gucci", "description": "Floral summer dress"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 8, "total": 1220.92,
     "products": [
         {"id": 1, "title": "Essence Mascara", "price": 9.99, "quantity": 4, "total": 39.96},
         {"id": 9, "title": "Samsung Galaxy S24", "price": 799.99, "quantity": 1, "total": 799.99},
         {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 3, "total": 149.97},
     ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 3, "total": 1149.98,
     "products": [
         {"id": 10, "title": "iPhone 15 Pro", "price": 1099.99, "quantity": 1, "total": 1099.99},
         {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 2, "total": 29.98},
     ]},
    {"id": 3, "userId": 1, "totalProducts": 2, "totalQuantity": 2, "total": 629.98,
     "products": [
         {"id": 11, "title": "HP Pavilion 15", "price": 499.99, "quantity": 1, "total": 499.99},
         {"id": 13, "title": "Nike Air Max 270", "price": 129.99, "quantity": 1, "total": 129.99},
     ]},
    {"id": 4, "userId": 3, "totalProducts": 3, "totalQuantity": 5, "total": 489.95,
     "products": [
         {"id": 7, "title": "Chanel Coco Noir", "price": 129.99, "quantity": 2, "total": 259.98},
         {"id": 4, "title": "Red Lipstick", "price": 12.99, "quantity": 1, "total": 12.99},
         {"id": 8, "title": "Dior J'adore", "price": 89.99, "quantity": 2, "total": 179.98},
     ]},
    {"id": 5, "userId": 4, "totalProducts": 2, "totalQuantity": 3, "total": 1279.98,
     "products": [
         {"id": 14, "title": "Adidas Ultraboost", "price": 149.99, "quantity": 2, "total": 299.98},
         {"id": 12, "title": "Dell XPS 13", "price": 999.99, "quantity": 1, "total": 999.99},
     ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@test.com", "phone": "+1-555-1234", "username": "emilyj",
     "address": {"address": "626 Main Street", "city": "Phoenix", "state": "AZ", "postalCode": "85001"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@test.com", "phone": "+1-555-5678", "username": "mikew",
     "address": {"address": "385 Fifth Street", "city": "Houston", "state": "TX", "postalCode": "77001"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@test.com", "phone": "+1-555-9012", "username": "sophiab",
     "address": {"address": "100 Oak Avenue", "city": "Chicago", "state": "IL", "postalCode": "60601"}},
    {"id": 4, "firstName": "James", "lastName": "Davis", "email": "james.davis@test.com", "phone": "+1-555-3456", "username": "jamesd",
     "address": {"address": "250 Pine Road", "city": "Seattle", "state": "WA", "postalCode": "98101"}},
]

# COMMAND ----------

def fetch_api_data(endpoint: str, wrapper_key: str = None) -> list:
    """Fetch data from DummyJSON API with fallback to embedded data."""
    url = f"{API_BASE}/{endpoint}"
    print(f"Fetching: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        if wrapper_key and wrapper_key in data:
            data = data[wrapper_key]
        print(f"  -> {len(data)} records fetched from API")
        return data
    except Exception as e:
        print(f"  ! API unavailable ({e}), using fallback data")
        fallback = {
            "products": FALLBACK_PRODUCTS,
            "carts": FALLBACK_CARTS,
            "users": FALLBACK_USERS,
        }
        data = fallback.get(endpoint, [])
        print(f"  -> {len(data)} records loaded from fallback")
        return data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas — explicit types to avoid Spark inference errors

# COMMAND ----------

# Explicit schemas prevent CANNOT_MERGE_TYPE errors (DoubleType vs LongType)
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

def clean_products(raw_data: list) -> list:
    """Keep only needed product fields with correct types."""
    cleaned = []
    for p in raw_data:
        cleaned.append({
            "id": int(p["id"]),
            "title": str(p.get("title", "")),
            "price": float(p.get("price", 0)),
            "category": str(p.get("category", "")),
            "rating": float(p.get("rating", 0)),
            "brand": str(p.get("brand", "")),
            "description": str(p.get("description", "")),
        })
    return cleaned

def clean_carts(raw_data: list) -> list:
    """Keep only needed cart fields with correct types."""
    cleaned = []
    for c in raw_data:
        items = []
        for item in c.get("products", []):
            items.append({
                "id": int(item["id"]),
                "title": str(item.get("title", "")),
                "price": float(item.get("price", 0)),
                "quantity": int(item.get("quantity", 0)),
                "total": float(item.get("total", 0)),
            })
        cleaned.append({
            "id": int(c["id"]),
            "userId": int(c["userId"]),
            "totalProducts": int(c.get("totalProducts", 0)),
            "totalQuantity": int(c.get("totalQuantity", 0)),
            "total": float(c.get("total", 0)),
            "products": items,
        })
    return cleaned

def clean_users(raw_data: list) -> list:
    """Keep only needed user fields with correct types."""
    cleaned = []
    for u in raw_data:
        addr = u.get("address", {})
        cleaned.append({
            "id": int(u["id"]),
            "firstName": str(u.get("firstName", "")),
            "lastName": str(u.get("lastName", "")),
            "email": str(u.get("email", "")),
            "phone": str(u.get("phone", "")),
            "username": str(u.get("username", "")),
            "address": {
                "address": str(addr.get("address", "")),
                "city": str(addr.get("city", "")),
                "state": str(addr.get("state", "")),
                "postalCode": str(addr.get("postalCode", "")),
            },
        })
    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Products

# COMMAND ----------

# Fetch, clean, and write Products
products_data = fetch_api_data("products", wrapper_key="products")
products_clean = clean_products(products_data)
df_products = spark.createDataFrame(products_clean, schema=products_schema)
df_products = (df_products
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE))
    .withColumn("_batch_id", lit(BATCH_ID))
)

df_products.write.format("delta").mode("overwrite").saveAsTable("default.products_bronze")
print("✓ default.products_bronze written")

# COMMAND ----------

display(spark.sql("SELECT id, title, price, category FROM default.products_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Carts (Orders)

# COMMAND ----------

# Fetch, clean, and write Carts
carts_data = fetch_api_data("carts", wrapper_key="carts")
carts_clean = clean_carts(carts_data)
df_carts = spark.createDataFrame(carts_clean, schema=carts_schema)
df_carts = (df_carts
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE))
    .withColumn("_batch_id", lit(BATCH_ID))
)

df_carts.write.format("delta").mode("overwrite").saveAsTable("default.carts_bronze")
print("✓ default.carts_bronze written")

# COMMAND ----------

display(spark.sql("SELECT id, userId, totalProducts, totalQuantity FROM default.carts_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Users (Customers)

# COMMAND ----------

# Fetch, clean, and write Users
users_data = fetch_api_data("users", wrapper_key="users")
users_clean = clean_users(users_data)
df_users = spark.createDataFrame(users_clean, schema=users_schema)
df_users = (df_users
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit(SOURCE))
    .withColumn("_batch_id", lit(BATCH_ID))
)

df_users.write.format("delta").mode("overwrite").saveAsTable("default.users_bronze")
print("✓ default.users_bronze written")

# COMMAND ----------

display(spark.sql("SELECT id, email, username FROM default.users_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Summary

# COMMAND ----------

print("=" * 50)
print("BRONZE INGESTION COMPLETE")
print("=" * 50)
for table in ["products_bronze", "carts_bronze", "users_bronze"]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM default.{table}").collect()[0]["cnt"]
    print(f"  default.{table}: {count} rows")
print(f"  Batch ID: {BATCH_ID}")
