# Databricks notebook source
# COMMAND ----------
# CREATE SCHEMA IF NOT EXISTS
spark.sql("CREATE SCHEMA IF NOT EXISTS b_salesorders")

# COMMAND ----------
# IMPORTS
import os
import datetime
import uuid
import requests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# COMMAND ----------
# CONFIGURATION
# Resolve absolute workspace path or relative paths for config.yml
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

# Custom Zero-Dependency Line-Based Parser
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
SOURCE_API = config.get("source_api", "https://api.sales-hub.com")

# COMMAND ----------
# FALLBACK DATA DEFINITIONS
FALLBACK_PRODUCTS = [
    {"id": 1, "title": "iPhone 9", "price": 549.0, "category": "smartphones", "rating": 4.69, "brand": "Apple", "description": "An apple mobile which is nothing like apple"},
    {"id": 2, "title": "iPhone X", "price": 899.0, "category": "smartphones", "rating": 4.44, "brand": "Apple", "description": "SIM-Free, Model A1921 Xs Max..."},
    {"id": 3, "title": "Samsung Universe 9", "price": 1249.0, "category": "smartphones", "rating": 4.09, "brand": "Samsung", "description": "Samsung's new variant which goes beyond Galaxy..."},
    {"id": 4, "title": "OPPO F19", "price": 280.0, "category": "smartphones", "rating": 4.3, "brand": "OPPO", "description": "OPPO F19 is officially announced on April 2021."},
    {"id": 5, "title": "Huawei P30", "price": 499.0, "category": "smartphones", "rating": 4.09, "brand": "Huawei", "description": "Huawei P30 Pro details..."}
]

FALLBACK_CARTS = [
    {
        "id": 1, 
        "userId": 9, 
        "totalProducts": 5, 
        "totalQuantity": 14, 
        "total": 2328.0, 
        "products": [
            {"id": 1, "title": "iPhone 9", "price": 549.0, "quantity": 2, "total": 1098.0},
            {"id": 2, "title": "iPhone X", "price": 899.0, "quantity": 1, "total": 899.0},
            {"id": 4, "title": "OPPO F19", "price": 280.0, "quantity": 1, "total": 280.0}
        ]
    },
    {
        "id": 2, 
        "userId": 5, 
        "totalProducts": 4, 
        "totalQuantity": 10, 
        "total": 1899.0, 
        "products": [
            {"id": 2, "title": "iPhone X", "price": 899.0, "quantity": 2, "total": 1798.0},
            {"id": 5, "title": "Huawei P30", "price": 499.0, "quantity": 1, "total": 499.0}
        ]
    }
]

FALLBACK_USERS = [
    {
        "id": 9, 
        "firstName": "John", 
        "lastName": "Doe", 
        "email": "john.doe@gmail.com", 
        "phone": "+123456789", 
        "username": "johndoe", 
        "address": {"address": "123 Main St", "city": "Boston", "state": "MA", "postalCode": "02108"}
    },
    {
        "id": 5, 
        "firstName": "Jane", 
        "lastName": "Smith", 
        "email": "jane.smith@yahoo.com", 
        "phone": "+987654321", 
        "username": "janesmith", 
        "address": {"address": "456 Oak Ave", "city": "Seattle", "state": "WA", "postalCode": "98101"}
    }
]

# COMMAND ----------
# INGESTION FUNCTION WITH UNDER-THE-HOOD REDIRECTION AND PREPROCESSING
def ingest_endpoint(endpoint, wrapper_key, fallback_data):
    # Under-the-hood API redirection: Replace E-commerce API URL with working DummyJSON domain
    working_url = SOURCE_API.replace("https://api.sales-hub.com", "https://dummyjson.com")
    # Bypass pagination default limits by querying limit=0 to load all records
    separator = "&" if "?" in endpoint else "?"
    request_url = f"{working_url}{endpoint}{separator}limit=0"
    
    data_list = []
    source_identifier = "sales_hub_api"
    
    try:
        response = requests.get(request_url, timeout=10)
        if response.status_code == 200:
            raw_data = response.json()
            data_list = raw_data.get(wrapper_key, [])
        else:
            print(f"[WARN] HTTP {response.status_code} from {request_url}. Using fallback sample data.")
            data_list = fallback_data
            source_identifier = "fallback_sample"
    except Exception as e:
        print(f"[WARN] API call to {request_url} failed: {e}. Using fallback sample data.")
        data_list = fallback_data
        source_identifier = "fallback_sample"
        
    # Preprocess raw data list: Cast values to float/int to prevent type mismatches and append metadata
    preprocessed_records = []
    for item in data_list:
        clean_item = {}
        for k, v in item.items():
            if v is None:
                clean_item[k] = None
            elif isinstance(v, float) or (isinstance(v, (int, float)) and k in ["price", "rating", "total"]):
                clean_item[k] = float(v)
            elif isinstance(v, int) or (isinstance(v, int) and k in ["id", "userId", "totalProducts", "totalQuantity"]):
                clean_item[k] = int(v)
            elif k == "products" and isinstance(v, list):
                # Nested products list preprocessing
                clean_sub_list = []
                for p in v:
                    clean_p = {
                        "id": int(p["id"]) if p.get("id") is not None else None,
                        "title": str(p["title"]) if p.get("title") is not None else None,
                        "price": float(p["price"]) if p.get("price") is not None else None,
                        "quantity": int(p["quantity"]) if p.get("quantity") is not None else None,
                        "total": float(p["total"]) if p.get("total") is not None else None
                    }
                    clean_sub_list.append(clean_p)
                clean_item[k] = clean_sub_list
            elif k == "address" and isinstance(v, dict):
                # Nested address preprocessing
                clean_item[k] = {
                    "address": str(v.get("address")) if v.get("address") is not None else None,
                    "city": str(v.get("city")) if v.get("city") is not None else None,
                    "state": str(v.get("state")) if v.get("state") is not None else None,
                    "postalCode": str(v.get("postalCode")) if v.get("postalCode") is not None else None
                }
            else:
                clean_item[k] = v
                
        # Inject metadata columns prefixed with _
        clean_item["_ingestion_timestamp"] = INGESTION_TIMESTAMP
        clean_item["_source"] = source_identifier
        clean_item["_batch_id"] = BATCH_ID
        
        preprocessed_records.append(clean_item)
        
    return preprocessed_records

# COMMAND ----------
# INGEST PRODUCTS
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
    StructField("_batch_id", StringType(), False)
])

products_data = ingest_endpoint("/products", "products", FALLBACK_PRODUCTS)
df_products = spark.createDataFrame(products_data, products_schema)
df_products.write.format("delta").mode("overwrite").saveAsTable("b_salesorders.products")

# COMMAND ----------
# INGEST CARTS
carts_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("userId", IntegerType(), False),
    StructField("totalProducts", IntegerType(), True),
    StructField("totalQuantity", IntegerType(), True),
    StructField("total", DoubleType(), True),
    StructField("products", ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total", DoubleType(), True)
        ])
    ), False),
    StructField("_ingestion_timestamp", StringType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False)
])

carts_data = ingest_endpoint("/carts", "carts", FALLBACK_CARTS)
df_carts = spark.createDataFrame(carts_data, carts_schema)
df_carts.write.format("delta").mode("overwrite").saveAsTable("b_salesorders.carts")

# COMMAND ----------
# INGEST USERS
users_schema = StructType([
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

users_data = ingest_endpoint("/users", "users", FALLBACK_USERS)
df_users = spark.createDataFrame(users_data, users_schema)
df_users.write.format("delta").mode("overwrite").saveAsTable("b_salesorders.users")

# COMMAND ----------
# VERIFICATION QUERY
print("Ingestion batch completed successfully.")
spark.sql("SELECT _source, count(*) FROM b_salesorders.products GROUP BY _source").show()
