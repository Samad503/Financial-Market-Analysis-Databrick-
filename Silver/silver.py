# Databricks notebook source
from pyspark.sql.functions import col, when, lower, to_timestamp

# COMMAND ----------

# DBTITLE 1,Cell 2
# 1. Clean Market Data
df_silver_prices = spark.table("financial_market_dev.bronze.crypto_prices").select(
    col("asset_symbol"),
    col("price_usd").cast("double"),
    col("volume_24h").cast("double"),
    col("change_24h_pct").cast("double"),
    col("fetched_at").cast("timestamp").alias("recorded_timestamp"),
    col("_ingested_at")
).dropDuplicates(["asset_symbol", "recorded_timestamp"])

df_silver_prices.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("financial_market_dev.silver.crypto_prices")

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import col, lower, when, expr

# 1. Define asset keywords with explicit word boundaries (\b)
# Using raw strings r"..." and \\b prevents matching 'something', 'canada', 'solution', etc.
crypto_pattern = r"\b(bitcoin|btc|ethereum|eth|solana|sol|cardano|ada|crypto)\b"

# Define sentiment patterns with word boundaries
pos_pattern = r"\b(growth|surge|profit|gain|bull|high|up|rise)\b"
neg_pattern = r"\b(drop|loss|crash|bear|down|hack|risk|sec|fall)\b"

# 2. Pipeline Execution
headline_lower = lower(col("headline_text"))

df_silver_news = (
    spark.table("financial_market_dev.bronze.news_headlines")
    .filter(headline_lower.rlike(crypto_pattern))
    .select(
        col("headline_text"),
        col("source_url"),
        col("scraped_at").cast("timestamp").alias("published_timestamp"),
        when(headline_lower.rlike(pos_pattern), "POSITIVE")
        .when(headline_lower.rlike(neg_pattern), "NEGATIVE")
        .otherwise("NEUTRAL").alias("sentiment_label")
    )
    .dropDuplicates(["headline_text"])
)

# 3. Write to Silver Delta Table
df_silver_news.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("financial_market_dev.silver.news_headlines")
display(spark.table("financial_market_dev.silver.news_headlines"))