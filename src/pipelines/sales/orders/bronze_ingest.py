# Databricks notebook source
# CELL 1: Schema Creation
spark.sql("CREATE SCHEMA IF NOT EXISTS b_salesorders")

# COMMAND ----------
# CELL 2: Imports
import os
import sys
import uuid
import requests
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, ArrayType
)

# COMMAND ----------
# CELL 3: Configuration & Path Resolution
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

config = {}
current_path = []
with open(config_path, "r") as f:
    for line in f:
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(stripped)
        level = indent // 2
        current_path = current_path[:level]
        
        if ":" in stripped:
            parts = stripped.split(":", 1)
            key = parts[0].strip()
            val = parts[1].split("#")[0].strip()
            
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            elif val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
                
            target = config
            for p in current_path:
                target = target[p]
                
            if not val:
                target[key] = {}
                current_path.append(key)
            else:
                target[key] = val

BATCH_ID = str(uuid.uuid4())
INGESTION_TIMESTAMP = datetime.utcnow().isoformat()
BASE_URL = config.get("source_api", "https://dummyjson.com")

# COMMAND ----------
# CELL 4: Products Schema & Fallback Definition
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
    StructField("_batch_id", StringType(), False)
])

FALLBACK_PRODUCTS = [
    {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "category": "beauty", "rating": 4.94, "brand": "Essence", "description": "The Essence Mascara Lash Princess is a popular mascara."},
    {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "category": "beauty", "rating": 3.5, "brand": "Glamour", "description": "A beautiful eyeshadow palette."}
]

# COMMAND ----------
# CELL 5: Ingest Products Execution
def fetch_products():
    url = f"{BASE_URL.rstrip('/')}/products"
    source = "dummyjson_api"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json().get("products", [])
        else:
            data = FALLBACK_PRODUCTS
            source = "fallback_sample"
    except Exception:
        data = FALLBACK_PRODUCTS
        source = "fallback_sample"
        
    cleaned_data = []
    for item in data:
        cleaned_data.append({
            "id": int(item["id"]) if item.get("id") is not None else None,
            "title": str(item.get("title", "")),
            "price": float(item["price"]) if item.get("price") is not None else 0.0,
            "category": str(item.get("category", "")),
            "rating": float(item["rating"]) if item.get("rating") is not None else None,
            "brand": str(item["brand"]) if item.get("brand") is not None else None,
            "description": str(item["description"]) if item.get("description") is not None else None,
            "_ingestion_timestamp": INGESTION_TIMESTAMP,
            "_source": source,
            "_batch_id": BATCH_ID
        })
    return spark.createDataFrame(cleaned_data, schema=product_schema)

products_df = fetch_products()
products_df.write.format("delta").mode("overwrite").saveAsTable(config["tables"]["bronze"]["products"])

# COMMAND ----------
# CELL 6: Carts Schema & Fallback Definition
cart_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("total", DoubleType(), False)
    ])), False),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False)
])

FALLBACK_CARTS = [
    {
        "id": 1,
        "userId": 1,
        "totalProducts": 2,
        "totalQuantity": 3,
        "total": 39.97,
        "products": [
            {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99, "quantity": 2, "total": 19.98},
            {"id": 2, "title": "Eyeshadow Palette with Mirror", "price": 19.99, "quantity": 1, "total": 19.99}
        ]
    }
]

# COMMAND ----------
# CELL 7: Ingest Carts Execution
def fetch_carts():
    url = f"{BASE_URL.rstrip('/')}/carts"
    source = "dummyjson_api"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json().get("carts", [])
        else:
            data = FALLBACK_CARTS
            source = "fallback_sample"
    except Exception:
        data = FALLBACK_CARTS
        source = "fallback_sample"
        
    cleaned_data = []
    for item in data:
        cart_products = []
        for p in item.get("products", []):
            cart_products.append({
                "id": int(p["id"]) if p.get("id") is not None else None,
                "title": str(p.get("title", "")),
                "price": float(p["price"]) if p.get("price") is not None else 0.0,
                "quantity": int(p["quantity"]) if p.get("quantity") is not None else 0,
                "total": float(p["total"]) if p.get("total") is not None else 0.0
            })
            
        cleaned_data.append({
            "id": int(item["id"]) if item.get("id") is not None else None,
            "userId": int(item["userId"]) if item.get("userId") is not None else None,
            "totalProducts": int(item["totalProducts"]) if item.get("totalProducts") is not None else None,
            "totalQuantity": int(item["totalQuantity"]) if item.get("totalQuantity") is not None else None,
            "total": float(item["total"]) if item.get("total") is not None else None,
            "products": cart_products,
            "_ingestion_timestamp": INGESTION_TIMESTAMP,
            "_source": source,
            "_batch_id": BATCH_ID
        })
    return spark.createDataFrame(cleaned_data, schema=cart_schema)

carts_df = fetch_carts()
carts_df.write.format("delta").mode("overwrite").saveAsTable(config["tables"]["bronze"]["carts"])

# COMMAND ----------
# CELL 8: Users Schema & Fallback Definition
user_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("username", StringType(), False),
    StructField("address", StructType([
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postalCode", StringType(), True)
    ]), True),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False)
])

FALLBACK_USERS = [
    {
        "id": 1,
        "firstName": "John",
        "lastName": "Doe",
        "email": "john.doe@example.com",
        "phone": "123-456-7890",
        "username": "johnd",
        "address": {"address": "123 Main St", "city": "Springfield", "state": "IL", "postalCode": "62701"}
    }
]

# COMMAND ----------
# CELL 9: Ingest Users Execution
def fetch_users():
    url = f"{BASE_URL.rstrip('/')}/users"
    source = "dummyjson_api"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json().get("users", [])
        else:
            data = FALLBACK_USERS
            source = "fallback_sample"
    except Exception:
        data = FALLBACK_USERS
        source = "fallback_sample"
        
    cleaned_data = []
    for item in data:
        addr = item.get("address", {})
        cleaned_data.append({
            "id": int(item["id"]) if item.get("id") is not None else None,
            "firstName": str(item["firstName"]) if item.get("firstName") is not None else None,
            "lastName": str(item["lastName"]) if item.get("lastName") is not None else None,
            "email": str(item.get("email", "")),
            "phone": str(item["phone"]) if item.get("phone") is not None else None,
            "username": str(item.get("username", "")),
            "address": {
                "address": str(addr.get("address", "")) if addr.get("address") is not None else None,
                "city": str(addr.get("city", "")) if addr.get("city") is not None else None,
                "state": str(addr.get("state", "")) if addr.get("state") is not None else None,
                "postalCode": str(addr.get("postalCode", "")) if addr.get("postalCode") is not None else None
            } if addr else None,
            "_ingestion_timestamp": INGESTION_TIMESTAMP,
            "_source": source,
            "_batch_id": BATCH_ID
        })
    return spark.createDataFrame(cleaned_data, schema=user_schema)

users_df = fetch_users()
users_df.write.format("delta").mode("overwrite").saveAsTable(config["tables"]["bronze"]["users"])

# COMMAND ----------
# CELL 10: Ingestion Success Message
print(f"Bronze layer ingestion completed successfully with batch ID: {BATCH_ID}")
display(spark.sql(f"SELECT * FROM {config['tables']['bronze']['products']} LIMIT 5"))
