# Databricks notebook source
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

# COMMAND ----------

# 1. Ingest Live Crypto Market Data via API
def fetch_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana,cardano",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params).json()
    
    records = []
    for coin_id, metrics in response.items():
        records.append({
            "asset_symbol": coin_id.upper(),
            "price_usd": float(metrics.get("usd", 0.0)),
            "volume_24h": float(metrics.get("usd_24h_vol", 0.0)),
            "change_24h_pct": float(metrics.get("usd_24h_change", 0.0)),
            "fetched_at": datetime.utcnow().isoformat()
        })
    return records

# COMMAND ----------

# 2. Scrape Live Tech/Financial Headlines
def scrape_live_news():
    url = "https://news.ycombinator.com/"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    headlines = []
    for tag in soup.select(".titleline > a"):
        text = tag.get_text()
        link = tag.get("href", "")
        # Filter relevant financial/tech keywords
        headlines.append({
            "headline_text": text,
            "source_url": link,
            "scraped_at": datetime.utcnow().isoformat()
        })
    return headlines

# COMMAND ----------

# Execute Ingestion
crypto_data = fetch_crypto_prices()
news_data = scrape_live_news()

# COMMAND ----------

# DBTITLE 1,Cell 5
# Save payload to Unity Catalog Volume
# Create catalog, schema, and volume if they don't exist
spark.sql("CREATE CATALOG IF NOT EXISTS financial_market_dev")
spark.sql("CREATE SCHEMA IF NOT EXISTS financial_market_dev.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS financial_market_dev.bronze.raw_landing")

volume_path = "/Volumes/financial_market_dev/bronze/raw_landing"

dbutils.fs.put(f"{volume_path}/raw_crypto.json", json.dumps(crypto_data), overwrite=True)
dbutils.fs.put(f"{volume_path}/raw_news.json", json.dumps(news_data), overwrite=True)

print(f"Successfully saved files to Unity Catalog Volume: {volume_path}")