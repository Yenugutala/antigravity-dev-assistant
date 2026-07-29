# Databricks notebook source
# Cell 1: Schema creation
spark.sql("CREATE SCHEMA IF NOT EXISTS b_salesorders")

# COMMAND ----------
# Cell 2: Imports
import os
import uuid
import datetime
import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType

# COMMAND ----------
# Cell 3: Configuration
# Resolve config.yml path
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

# Zero-dependency configuration parsing
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
INGESTION_TIMESTAMP = datetime.datetime.utcnow().isoformat()
SOURCE_API = config.get("source_api", "https://dummyjson.com")
BRONZE_SCHEMA = config.get("schemas", {}).get("bronze", "b_salesorders")

# COMMAND ----------
# Cell 4: Products Ingestion
# Fallback data
FALLBACK_PRODUCTS = [
    {
        "id": 1,
        "title": "Essence Mascara Lash Princess",
        "price": 9.99,
        "category": "beauty",
        "rating": 4.94,
        "brand": "Essence",
        "description": "The Essence Mascara Lash Princess is a popular mascara."
    }
]

def preprocess_products(raw_list):
    clean = []
    for item in raw_list:
        clean.append({
            "id": int(item["id"]),
            "title": str(item["title"]),
            "price": float(item["price"]),
            "category": str(item["category"]),
            "rating": float(item["rating"]) if item.get("rating") is not None else None,
            "brand": str(item["brand"]) if item.get("brand") is not None else None,
            "description": str(item["description"]) if item.get("description") is not None else None
        })
    return clean

# Fetch from API
try:
    response = requests.get(f"{SOURCE_API}/products", timeout=10)
    if response.status_code == 200:
        raw_data = response.json().get("products", [])
        source_name = "dummyjson_api"
    else:
        raw_data = FALLBACK_PRODUCTS
        source_name = "fallback_sample"
except Exception:
    raw_data = FALLBACK_PRODUCTS
    source_name = "fallback_sample"

# Preprocess and enforce schema
clean_data = preprocess_products(raw_data)

# Define explicit PySpark schema
product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
    StructField("rating", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("description", StringType(), True)
])

df_products = spark.createDataFrame(clean_data, schema=product_schema)

# Add metadata columns
from pyspark.sql.functions import lit
df_products = df_products.withColumn("_ingestion_timestamp", lit(INGESTION_TIMESTAMP)) \
                         .withColumn("_source", lit(source_name)) \
                         .withColumn("_batch_id", lit(BATCH_ID))

# Write Delta Table
target_table_products = config.get("tables", {}).get("bronze", {}).get("products", f"{BRONZE_SCHEMA}.products")
df_products.write.format("delta").mode("overwrite").saveAsTable(target_table_products)

# COMMAND ----------
# Cell 5: Carts Ingestion
# Fallback data
FALLBACK_CARTS = [
    {
        "id": 1,
        "userId": 1,
        "totalProducts": 1,
        "totalQuantity": 2,
        "total": 19.98,
        "products": [
            {
                "id": 1,
                "title": "Essence Mascara Lash Princess",
                "price": 9.99,
                "quantity": 2,
                "total": 19.98
            }
        ]
    }
]

def preprocess_carts(raw_list):
    clean = []
    for item in raw_list:
        clean_products = []
        for p in item.get("products") or []:
            clean_products.append({
                "id": int(p["id"]),
                "title": str(p["title"]),
                "price": float(p["price"]),
                "quantity": int(p["quantity"]),
                "total": float(p["total"])
            })
        clean.append({
            "id": int(item["id"]) if item.get("id") is not None else None,
            "userId": int(item["userId"]) if item.get("userId") is not None else None,
            "totalProducts": int(item["totalProducts"]) if item.get("totalProducts") is not None else None,
            "totalQuantity": int(item["totalQuantity"]) if item.get("totalQuantity") is not None else None,
            "total": float(item["total"]) if item.get("total") is not None else None,
            "products": clean_products
        })
    return clean

# Fetch from API
try:
    response = requests.get(f"{SOURCE_API}/carts", timeout=10)
    if response.status_code == 200:
        raw_data = response.json().get("carts", [])
        source_name = "dummyjson_api"
    else:
        raw_data = FALLBACK_CARTS
        source_name = "fallback_sample"
except Exception:
    raw_data = FALLBACK_CARTS
    source_name = "fallback_sample"

# Preprocess and enforce schema
clean_data = preprocess_carts(raw_data)

# Define explicit PySpark schema
cart_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(
        StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("total", DoubleType(), False)
        ])
    ), False)
])

df_carts = spark.createDataFrame(clean_data, schema=cart_schema)

# Add metadata columns
df_carts = df_carts.withColumn("_ingestion_timestamp", lit(INGESTION_TIMESTAMP)) \
                   .withColumn("_source", lit(source_name)) \
                   .withColumn("_batch_id", lit(BATCH_ID))

# Write Delta Table
target_table_carts = config.get("tables", {}).get("bronze", {}).get("carts", f"{BRONZE_SCHEMA}.carts")
df_carts.write.format("delta").mode("overwrite").saveAsTable(target_table_carts)

# COMMAND ----------
# Cell 6: Users Ingestion
# Fallback data
FALLBACK_USERS = [
    {
        "id": 1,
        "firstName": "John",
        "lastName": "Doe",
        "email": "john.doe@x.dummyjson.com",
        "phone": "+123456789",
        "username": "johndoe",
        "address": {
            "address": "123 Main St",
            "city": "San Jose",
            "state": "CA",
            "postalCode": "95112"
        }
    }
]

def preprocess_users(raw_list):
    clean = []
    for item in raw_list:
        raw_address = item.get("address")
        address = None
        if raw_address:
            address = {
                "address": str(raw_address.get("address") or ""),
                "city": str(raw_address.get("city") or ""),
                "state": str(raw_address.get("state") or ""),
                "postalCode": str(raw_address.get("postalCode") or "")
            }
        clean.append({
            "id": int(item["id"]),
            "firstName": str(item["firstName"]) if item.get("firstName") is not None else None,
            "lastName": str(item["lastName"]) if item.get("lastName") is not None else None,
            "email": str(item["email"]),
            "phone": str(item["phone"]) if item.get("phone") is not None else None,
            "username": str(item["username"]),
            "address": address
        })
    return clean

# Fetch from API
try:
    response = requests.get(f"{SOURCE_API}/users", timeout=10)
    if response.status_code == 200:
        raw_data = response.json().get("users", [])
        source_name = "dummyjson_api"
    else:
        raw_data = FALLBACK_USERS
        source_name = "fallback_sample"
except Exception:
    raw_data = FALLBACK_USERS
    source_name = "fallback_sample"

# Preprocess and enforce schema
clean_data = preprocess_users(raw_data)

# Define explicit PySpark schema
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
    ]), True)
])

df_users = spark.createDataFrame(clean_data, schema=user_schema)

# Add metadata columns
df_users = df_users.withColumn("_ingestion_timestamp", lit(INGESTION_TIMESTAMP)) \
                   .withColumn("_source", lit(source_name)) \
                   .withColumn("_batch_id", lit(BATCH_ID))

# Write Delta Table
target_table_users = config.get("tables", {}).get("bronze", {}).get("users", f"{BRONZE_SCHEMA}.users")
df_users.write.format("delta").mode("overwrite").saveAsTable(target_table_users)

# COMMAND ----------
# Cell 7: Verification
print(f"Bronze Layer Ingested successfully for batch {BATCH_ID}")
display(spark.sql(f"SELECT count(*) FROM {target_table_products}"))
display(spark.sql(f"SELECT count(*) FROM {target_table_carts}"))
display(spark.sql(f"SELECT count(*) FROM {target_table_users}"))
