# -------------------------------------------------------------
# silver_layer.py
# Purpose: Transform and write data from Bronze → Silver layer
# Author: <your-name>
# -------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------------------------------------
# 1️⃣ Spark Session Setup
# -------------------------------------------------------------
spark = SparkSession.builder.appName("SilverLayerTransformation").getOrCreate()

# -------------------------------------------------------------
# 2️⃣ Securely Load Credentials from Databricks Secrets
# -------------------------------------------------------------
# Make sure to create a secret scope in Databricks:
#    databricks secrets create-scope --scope <scope-name>
# Then store secrets like client-id, client-secret, and tenant-id inside that scope.

scope_name = "<your-secret-scope>"  # e.g., 'hospitalvaultscope'

storage_account_name = dbutils.secrets.get(scope=scope_name, key="storage-account-name")
tenant_id = dbutils.secrets.get(scope=scope_name, key="tenant-id")
client_id = dbutils.secrets.get(scope=scope_name, key="client-id")
client_secret = dbutils.secrets.get(scope=scope_name, key="client-secret")

# -------------------------------------------------------------
# 3️⃣ Azure ADLS Configuration
# -------------------------------------------------------------
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# -------------------------------------------------------------
# 4️⃣ Define Generic Paths
# -------------------------------------------------------------
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"

datasets = {
    "calendar": "AdventureWorks_Calendar",
    "customers": "AdventureWorks_Customers",
    "categories": "AdventureWorks_Product_Categories",
    "products": "AdventureWorks_Products",
    "returns": "AdventureWorks_Returns",
    "sales": "AdventureWorks_Sales*",
    "territories": "AdventureWorks_Territories",
    "subcategories": "Product_Subcategories"
}

# -------------------------------------------------------------
# 5️⃣ Load Data from Bronze Layer
# -------------------------------------------------------------
df = {}
for name, path in datasets.items():
    print(f"🔹 Loading {name} dataset...")
    df[name] = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{bronze_path}/{path}")
    )

# -------------------------------------------------------------
# 6️⃣ Transformations
# -------------------------------------------------------------

# 🗓️ Calendar
df["calendar"] = (
    df["calendar"]
    .withColumn("Year", year(col("Date")))
    .withColumn("Month", month(col("Date")))
    .withColumn("Day", dayofmonth(col("Date")))
)

# 👥 Customers
df["customers"] = df["customers"].withColumn(
    "FullName",
    concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName"))
)

# 🛍️ Products
df["products"] = (
    df["products"]
    .withColumn("ProductSKU", split(col("ProductSKU"), "-")[0])
    .withColumn("ProductName", split(col("ProductName"), " ")[0])
)

# 💵 Sales
df["sales"] = (
    df["sales"]
    .withColumn("StockDate", to_timestamp("StockDate"))
    .withColumn("OrderNumber", regexp_replace(col("OrderNumber"), "^S", "T"))
    .withColumn("TotalCost", col("OrderLineItem") * col("OrderQuantity"))
)

# Example aggregation (for validation)
df["sales"].groupBy("OrderDate").agg(count("OrderNumber").alias("TotalOrders")).show(5)

# -------------------------------------------------------------
# 7️⃣ Write Transformed Data to Silver Layer
# -------------------------------------------------------------
for name, dataframe in df.items():
    target_path = f"{silver_path}/{datasets[name].replace('*', '')}"
    print(f"💾 Writing {name} → {target_path}")
    (
        dataframe.write.format("parquet")
        .mode("overwrite")  # or "append" if running incrementally
        .save(target_path)
    )

print("🎉 Silver layer transformation completed successfully!")
