# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Sales Ingestion (Python)
# MAGIC Ingests raw data from DummyJSON API into `b_antigravity_sales` Delta tables with explicit schemas and API fallback.

# COMMAND ----------

# Cell 1: Schema Creation
spark.sql("CREATE SCHEMA IF NOT EXISTS b_antigravity_sales")

# COMMAND ----------

# Cell 2: Imports
import requests
import uuid
import yaml
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, ArrayType
)

# COMMAND ----------

# Cell 3: Configuration & Initialization
import os
try:
    notebook_path = dbutils.entrypoint.getDbutils().notebook().getContext().notebookPath().get()
    if "/src/" in notebook_path:
        repo_root = notebook_path.split("/src/")[0]
    elif "/notebooks/" in notebook_path:
        repo_root = notebook_path.split("/notebooks/")[0]
    else:
        repo_root = notebook_path
    config_path = f"/Workspace{repo_root}/config.yml"
except Exception:
    config_path = "config.yml"
    for _ in range(5):
        if os.path.exists(config_path):
            break
        config_path = os.path.join("..", config_path)

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

API_BASE_URL = config.get("source_api", "https://dummyjson.com")
BATCH_ID = str(uuid.uuid4())
INGESTION_TS = datetime.utcnow().isoformat()

# COMMAND ----------

# Cell 4: Products Schema & Fallback
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

FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "Popular mascara"},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.28, "brand": "Glamour Beauty", "description": "Eyeshadow palette"},
    {"id": 3, "title": "Powder Canister", "price": 14.99, "category": "beauty", "rating": 3.82, "brand": "Velvet Touch", "description": "Fine powder"},
    {"id": 4, "title": "Red Lipstick", "price": 12.99, "category": "beauty", "rating": 4.55, "brand": "Glamour Beauty", "description": "Red lipstick"},
    {"id": 5, "title": "Red Nail Polish", "price": 8.99, "category": "beauty", "rating": 3.91, "brand": "Nail Couture", "description": "Nail polish"},
    {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "category": "fragrances", "rating": 4.85, "brand": "Calvin Klein", "description": "Classic unisex fragrance"},
    {"id": 7, "title": "Samsung Galaxy S24", "price": 899.99, "category": "smartphones", "rating": 4.50, "brand": "Samsung", "description": "Latest smartphone"},
    {"id": 8, "title": "iPhone 15 Pro", "price": 999.99, "category": "smartphones", "rating": 4.75, "brand": "Apple", "description": "Premium smartphone"},
    {"id": 9, "title": "HP Pavilion 15", "price": 499.99, "category": "laptops", "rating": 4.30, "brand": "HP", "description": "Budget laptop"},
    {"id": 10, "title": "Blue Denim Jacket", "price": 39.99, "category": "mens-clothing", "rating": 4.02, "brand": "Levi's", "description": "Classic denim jacket"},
]

# COMMAND ----------

# Cell 5: Products Ingestion Execution
source_label = "dummyjson_api"
try:
    resp = requests.get(f"{API_BASE_URL}/products?limit=50", timeout=10)
    resp.raise_for_status()
    raw_products = resp.json()["products"]
except Exception as e:
    print(f"API products fetch failed ({e}), switching to fallback sample data")
    raw_products = FALLBACK_PRODUCTS
    source_label = "fallback_sample"

cleaned_products = []
for p in raw_products:
    cleaned_products.append((
        int(p["id"]),
        str(p.get("title", "")),
        float(p.get("price", 0.0)),
        str(p.get("category", "")),
        float(p["rating"]) if p.get("rating") is not None else None,
        str(p.get("brand", "")) if p.get("brand") is not None else None,
        str(p.get("description", "")) if p.get("description") is not None else None,
        INGESTION_TS,
        source_label,
        BATCH_ID,
    ))

df_products = spark.createDataFrame(cleaned_products, schema=products_schema)
df_products.write.format("delta").mode("overwrite").saveAsTable("b_antigravity_sales.products")
print(f"Ingested {df_products.count()} products into b_antigravity_sales.products")

# COMMAND ----------

# Cell 6: Carts Schema & Fallback
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

FALLBACK_CARTS = [
    {"id": 1, "userId": 1, "totalProducts": 3, "totalQuantity": 5, "total": 59.97,
     "products": [
         {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
         {"id": 6, "title": "Calvin Klein CK One", "price": 49.99, "quantity": 1, "total": 49.99},
         {"id": 3, "title": "Powder Canister", "price": 14.99, "quantity": 2, "total": 29.98},
     ]},
    {"id": 2, "userId": 2, "totalProducts": 2, "totalQuantity": 3, "total": 129.97,
     "products": [
         {"id": 7, "title": "Samsung Galaxy S24", "price": 899.99, "quantity": 1, "total": 899.99},
         {"id": 10, "title": "Blue Denim Jacket", "price": 39.99, "quantity": 2, "total": 79.98},
     ]},
    {"id": 3, "userId": 3, "totalProducts": 2, "totalQuantity": 2, "total": 28.98,
     "products": [
         {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99},
         {"id": 5, "title": "Red Nail Polish", "price": 8.99, "quantity": 1, "total": 8.99},
     ]},
]

# COMMAND ----------

# Cell 7: Carts Ingestion Execution
source_label = "dummyjson_api"
try:
    resp = requests.get(f"{API_BASE_URL}/carts?limit=20", timeout=10)
    resp.raise_for_status()
    raw_carts = resp.json()["carts"]
except Exception as e:
    print(f"API carts fetch failed ({e}), switching to fallback sample data")
    raw_carts = FALLBACK_CARTS
    source_label = "fallback_sample"

cleaned_carts = []
for c in raw_carts:
    items = []
    for item in c.get("products", []):
        items.append((
            int(item["id"]),
            str(item.get("title", "")),
            float(item.get("price", 0.0)),
            int(item.get("quantity", 0)),
            float(item.get("total", 0.0)),
        ))
    cleaned_carts.append((
        int(c["id"]) if c.get("id") is not None else None,
        int(c["userId"]) if c.get("userId") is not None else None,
        int(c.get("totalProducts", 0)) if c.get("totalProducts") is not None else None,
        int(c.get("totalQuantity", 0)) if c.get("totalQuantity") is not None else None,
        float(c.get("total", 0.0)) if c.get("total") is not None else None,
        items,
        INGESTION_TS,
        source_label,
        BATCH_ID,
    ))

df_carts = spark.createDataFrame(cleaned_carts, schema=carts_schema)
df_carts.write.format("delta").mode("overwrite").saveAsTable("b_antigravity_sales.carts")
print(f"Ingested {df_carts.count()} carts into b_antigravity_sales.carts")

# COMMAND ----------

# Cell 8: Users Schema & Fallback
user_address_schema = StructType([
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
    StructField("address", user_address_schema, True),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False),
])

FALLBACK_USERS = [
    {"id": 1, "firstName": "John", "lastName": "Doe", "email": "john.doe@x.com", "phone": "123-456-7890", "username": "johndoe",
     "address": {"address": "123 Main St", "city": "Springfield", "state": "IL", "postalCode": "62701"}},
    {"id": 2, "firstName": "Jane", "lastName": "Smith", "email": "jane.smith@x.com", "phone": "987-654-3210", "username": "janesmith",
     "address": {"address": "456 Oak Ave", "city": "Bloomington", "state": "IN", "postalCode": "47401"}},
    {"id": 3, "firstName": "Bob", "lastName": "Johnson", "email": "bob.j@x.com", "phone": "555-555-5555", "username": "bobjohnson",
     "address": {"address": "789 Pine Rd", "city": "Madison", "state": "WI", "postalCode": "53703"}},
]

# COMMAND ----------

# Cell 9: Users Ingestion Execution
source_label = "dummyjson_api"
try:
    resp = requests.get(f"{API_BASE_URL}/users?limit=30", timeout=10)
    resp.raise_for_status()
    raw_users = resp.json()["users"]
except Exception as e:
    print(f"API users fetch failed ({e}), switching to fallback sample data")
    raw_users = FALLBACK_USERS
    source_label = "fallback_sample"

cleaned_users = []
for u in raw_users:
    addr = u.get("address", {})
    addr_tuple = (
        str(addr.get("address", "")),
        str(addr.get("city", "")),
        str(addr.get("state", "")),
        str(addr.get("postalCode", "")),
    )
    cleaned_users.append((
        int(u["id"]),
        str(u.get("firstName", "")),
        str(u.get("lastName", "")),
        str(u.get("email", "")),
        str(u.get("phone", "")),
        str(u.get("username", "")),
        addr_tuple,
        INGESTION_TS,
        source_label,
        BATCH_ID,
    ))

df_users = spark.createDataFrame(cleaned_users, schema=users_schema)
df_users.write.format("delta").mode("overwrite").saveAsTable("b_antigravity_sales.users")
print(f"Ingested {df_users.count()} users into b_antigravity_sales.users")

# COMMAND ----------

# Cell 10: Final Display / Verification
display(spark.sql("SELECT * FROM b_antigravity_sales.products LIMIT 5"))
