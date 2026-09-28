# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import current_timestamp

# Read raw JSON from Unity Catalog Volume
df_crypto_raw = spark.read.json("/Volumes/financial_market_dev/bronze/raw_landing/raw_crypto.json")

# Append ingestion timestamp metadata
df_crypto_bronze = df_crypto_raw.withColumn("_ingested_at", current_timestamp())

# Write directly to Bronze Delta Table in Unity Catalog
df_crypto_bronze.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("financial_market_dev.bronze.crypto_prices")

display(spark.table("financial_market_dev.bronze.crypto_prices"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Path to the raw JSON file in Unity Catalog Volume
VOLUME_FILE_PATH = "/Volumes/financial_market_dev/bronze/raw_landing/raw_news.json"

# 1. Read raw multiline JSON from Volume
df_news_raw = spark.read \
    .option("multiline", "true") \
    .json(VOLUME_FILE_PATH)

# 2. Append ingestion metadata timestamp
df_news_bronze = df_news_raw.withColumn("_ingested_at", current_timestamp())

# 3. Write into Bronze Delta Table in Unity Catalog
df_news_bronze.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("financial_market_dev.bronze.news_headlines")

# 4. Display results
display(spark.table("financial_market_dev.bronze.news_headlines"))