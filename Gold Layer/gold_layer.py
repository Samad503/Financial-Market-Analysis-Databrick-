# Databricks notebook source
from pyspark.sql.functions import count, avg, round as spark_round


# COMMAND ----------

# DBTITLE 1,Cell 2
# Gold Table 1: Asset Market Summary & Volatility Indicators
df_silver_read = spark.table("financial_market_dev.silver.crypto_prices")

df_gold_market_summary = df_silver_read.groupBy("asset_symbol").agg(
    spark_round(avg("price_usd"), 2).alias("avg_price_usd"),
    spark_round(avg("volume_24h"), 2).alias("avg_24h_volume"),
    spark_round(avg("change_24h_pct"), 2).alias("avg_momentum_pct"),
    count("recorded_timestamp").alias("data_points")
)
df_gold_market_summary.write.format("delta").mode("overwrite").saveAsTable("financial_market_dev.gold.market_summary")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Gold Table 2: Headline Sentiment Distribution
df_news_read = spark.table("financial_market_dev.silver.news_headlines")

df_gold_sentiment_summary = df_news_read.groupBy("sentiment_label").agg(
    count("headline_text").alias("total_headlines")
)

df_gold_sentiment_summary.write.format("delta").mode("overwrite").saveAsTable("financial_market_dev.gold.sentiment_summary")

display(spark.table("financial_market_dev.gold.sentiment_summary"))