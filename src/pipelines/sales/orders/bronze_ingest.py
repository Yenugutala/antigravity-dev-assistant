# Databricks notebook source

# COMMAND ----------

# Cell 1: Create Bronze Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS b_salesorders")

# COMMAND ----------

# Cell 2: Imports and Configuration
import requests
import uuid
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

BASE_URL = "https://dummyjson.com"
BATCH_ID = str(uuid.uuid4())
INGESTION_TS = datetime.utcnow().isoformat()

# COMMAND ----------

# Cell 3: Fallback Sample Data
FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "Popular mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.28, "brand": "Glamour Beauty", "description": "Eyeshadow palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet Touch", "description": "Fine setting powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.55, "brand": "Glamour Beauty", "description": "Classic red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Nail Couture", "description": "Vibrant red polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.85, "brand": "Calvin Klein", "description": "Iconic unisex fragrance"},
    {"id": 7, "title": "Samsung Galaxy S24", "price": 899.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Latest Samsung flagship"},
    {"id": 8, "title": "iPhone 15 Pro", "price": 999.99, "category": "smartphones", "rating": 4.70, "brand": "Apple", "description": "Apple flagship phone"},
    {"id": 9, "title": "HP Pavilion 15", "price": 499.99, "category": "laptops", "rating": 4.20, "brand": "HP", "description": "Budget laptop"},
    {"id": 10, "title": "Classic Leather Jacket", "price": 139.99, "category": "mens-clothing", "rating": 4.00, "brand": "Urban Threads", "description": "Genuine leather jacket"},
]

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 5, "total": 69.95,
     "products": [
         {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
         {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99},
         {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 2, "total": 29.98},
     ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 2, "total": 179.98,
     "products": [
         {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 1, "total": 49.99},
         {"id": 10, "title": "Classic Leather Jacket", "price": 139.99, "quantity": 1, "total": 139.99},
     ]},
    {"id": 3, "userId": 3, "totalProducts": 2, "totalQuantity": 2, "total": 1899.98,
     "products": [
         {"id": 7, "title": "Samsung Galaxy S24", "price": 899.99, "quantity": 1, "total": 899.99},
         {"id": 8, "title": "iPhone 15 Pro", "price": 999.99, "quantity": 1, "total": 999.99},
     ]},
]

FALLBACK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@x.dummyjson.com", "phone": "+1-555-0101", "username": "emjohnson",
     "address": {"address": "123 Main St", "city": "New York", "state": "NY", "postalCode": "10001"}},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@x.dummyjson.com", "phone": "+1-555-0102", "username": "mwilliams",
     "address": {"address": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "postalCode": "90001"}},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@x.dummyjson.com", "phone": "+1-555-0103", "username": "sbrown",
     "address": {"address": "789 Pine Rd", "city": "Chicago", "state": "IL", "postalCode": "60601"}},
]

# COMMAND ----------

# Cell 4: API Fetch Helper
def fetch_api_data(endpoint, wrapper_key, fallback_data):
    """Fetch data from DummyJSON API with fallback to sample data."""
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", timeout=10)
        response.raise_for_status()
        raw = response.json().get(wrapper_key, [])
        if raw:
            print(f"API fetch successful: {endpoint} ({len(raw)} records)")
            return raw, "dummyjson_api"
    except Exception as e:
        print(f"API fetch failed for {endpoint}: {e}")
    print(f"Using fallback data for {endpoint} ({len(fallback_data)} records)")
    return fallback_data, "fallback_sample"

# COMMAND ----------

# Cell 5: Products Schema and Ingestion
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

raw_products, products_source = fetch_api_data("/products", "products", FALLBACK_PRODUCTS)

clean_products = []
for p in raw_products:
    clean_products.append({
        "id": int(p["id"]),
        "title": str(p.get("title", "")),
        "price": float(p.get("price", 0.0)),
        "category": str(p.get("category", "")),
        "rating": float(p["rating"]) if p.get("rating") is not None else None,
        "brand": str(p["brand"]) if p.get("brand") is not None else None,
        "description": str(p["description"]) if p.get("description") is not None else None,
        "_ingestion_timestamp": INGESTION_TS,
        "_source": products_source,
        "_batch_id": BATCH_ID,
    })

df_products = spark.createDataFrame(clean_products, schema=products_schema)
df_products.write.format("delta").mode("overwrite").saveAsTable("b_salesorders.products")
print(f"b_salesorders.products: {df_products.count()} rows written")

# COMMAND ----------

# Cell 6: Carts Schema and Ingestion
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
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False),
])

raw_carts, carts_source = fetch_api_data("/carts", "carts", FALLBACK_CARTS)

clean_carts = []
for c in raw_carts:
    cart_products = []
    for item in c.get("products", []):
        cart_products.append({
            "id": int(item["id"]),
            "title": str(item.get("title", "")),
            "price": float(item.get("price", 0.0)),
            "quantity": int(item.get("quantity", 0)),
            "total": float(item.get("total", 0.0)),
        })
    clean_carts.append({
        "id": int(c["id"]),
        "userId": int(c["userId"]),
        "totalProducts": int(c["totalProducts"]) if c.get("totalProducts") is not None else None,
        "totalQuantity": int(c["totalQuantity"]) if c.get("totalQuantity") is not None else None,
        "total": float(c["total"]) if c.get("total") is not None else None,
        "products": cart_products,
        "_ingestion_timestamp": INGESTION_TS,
        "_source": carts_source,
        "_batch_id": BATCH_ID,
    })

df_carts = spark.createDataFrame(clean_carts, schema=carts_schema)
df_carts.write.format("delta").mode("overwrite").saveAsTable("b_salesorders.carts")
print(f"b_salesorders.carts: {df_carts.count()} rows written")

# COMMAND ----------

# Cell 7: Users Schema and Ingestion
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

raw_users, users_source = fetch_api_data("/users", "users", FALLBACK_USERS)

clean_users = []
for u in raw_users:
    addr = u.get("address", {})
    clean_users.append({
        "id": int(u["id"]),
        "firstName": str(u["firstName"]) if u.get("firstName") is not None else None,
        "lastName": str(u["lastName"]) if u.get("lastName") is not None else None,
        "email": str(u.get("email", "")),
        "phone": str(u["phone"]) if u.get("phone") is not None else None,
        "username": str(u.get("username", "")),
        "address": {
            "address": str(addr.get("address", "")),
            "city": str(addr.get("city", "")),
            "state": str(addr.get("state", "")),
            "postalCode": str(addr.get("postalCode", "")),
        } if addr else None,
        "_ingestion_timestamp": INGESTION_TS,
        "_source": users_source,
        "_batch_id": BATCH_ID,
    })

df_users = spark.createDataFrame(clean_users, schema=users_schema)
df_users.write.format("delta").mode("overwrite").saveAsTable("b_salesorders.users")
print(f"b_salesorders.users: {df_users.count()} rows written")

# COMMAND ----------

# Cell 8: Display Sample Data
print("=" * 60)
print("BRONZE INGESTION COMPLETE")
print("=" * 60)
print(f"Batch ID: {BATCH_ID}")
print(f"Timestamp: {INGESTION_TS}")
print()
print("Tables created:")
print(f"  b_salesorders.products  — {df_products.count()} rows")
print(f"  b_salesorders.carts     — {df_carts.count()} rows")
print(f"  b_salesorders.users     — {df_users.count()} rows")
print()
print("Sample products:")
spark.sql("SELECT id, title, price, category FROM b_salesorders.products LIMIT 5").show(truncate=False)
