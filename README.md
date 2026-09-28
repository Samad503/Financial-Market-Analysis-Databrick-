# Financial Market Analysis — Crypto Market Data Pipeline on Databricks

## Overview

This project implements an end-to-end **medallion architecture** (Bronze → Silver → Gold) data pipeline on Databricks for analyzing cryptocurrency market data and financial news sentiment. The pipeline ingests live crypto prices from the CoinGecko API and scrapes financial/tech headlines from Hacker News, then processes them through successive data quality and aggregation layers using Delta Lake and Unity Catalog.

## Architecture

```
┌─────────────────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌───────────────────┐
│  Live Ingestion Script  │────▶│ Bronze Layer │────▶│   Silver Layer   │────▶│    Gold Layer     │
│                         │     │  (Raw Data)  │     │ (Cleaned/Enriched)│     │ (Aggregated/Marts)│
└─────────────────────────┘     └──────────────┘     └──────────────────┘     └───────────────────┘
```

## Data Sources

| Source | Type | Description |
|--------|------|-------------|
| [CoinGecko API](https://www.coingecko.com/api/documentation) | REST API | Live crypto prices for Bitcoin, Ethereum, Solana, and Cardano — including price (USD), 24h volume, and 24h change percentage |
| [Hacker News](https://news.ycombinator.com/) | Web Scraping | Financial and tech news headlines scraped via BeautifulSoup |

## Project Structure

```
Financial-Market-Analysis-Databrick-/
├── Bronze Layer/
│   └── bronze_layer.py          # Reads raw JSON from UC Volume, writes to Bronze Delta tables
├── Silver/
│   └── silver.py                 # Cleans, casts, deduplicates, and adds sentiment labels
├── Gold Layer/
│   └── gold_layer.py            # Aggregates market metrics and sentiment distribution
├── Live Ingestion Script/
│   └── importing_data.py        # Fetches crypto prices via API and scrapes news headlines
└── README.md
```

## Pipeline Layers

### 1. Live Ingestion Script (`importing_data.py`)
- Fetches live cryptocurrency market data from the **CoinGecko API** (`/simple/price` endpoint) for Bitcoin, Ethereum, Solana, and Cardano
- Scrapes financial/tech headlines from **Hacker News** using BeautifulSoup
- Saves raw JSON payloads to a Unity Catalog Volume (`financial_market_dev.bronze.raw_landing`)

### 2. Bronze Layer (`bronze_layer.py`)
- Reads raw JSON files from the Unity Catalog Volume
- Appends ingestion timestamp metadata (`_ingested_at`)
- Writes to Bronze Delta tables in Unity Catalog:
  - `financial_market_dev.bronze.crypto_prices`
  - `financial_market_dev.bronze.news_headlines`

### 3. Silver Layer (`silver.py`)
- **Market Data**: Casts columns to proper types (double, timestamp), removes duplicates
- **News Headlines**: Filters for crypto-related keywords using regex (bitcoin, ethereum, solana, etc.), assigns sentiment labels (POSITIVE / NEGATIVE / NEUTRAL) based on keyword matching
- Writes to Silver Delta tables:
  - `financial_market_dev.silver.crypto_prices`
  - `financial_market_dev.silver.news_headlines`

### 4. Gold Layer (`gold_layer.py`)
- **Market Summary**: Aggregates average price, average 24h volume, average momentum percentage, and data point count per asset
- **Sentiment Summary**: Counts headlines grouped by sentiment label
- Writes to Gold Delta tables:
  - `financial_market_dev.gold.market_summary`
  - `financial_market_dev.gold.sentiment_summary`

## Unity Catalog Schema

```
financial_market_dev (Catalog)
├── bronze (Schema)
│   ├── raw_landing (Volume)       # Raw JSON landing zone
│   ├── crypto_prices (Table)     # Raw crypto price data
│   └── news_headlines (Table)    # Raw news headline data
├── silver (Schema)
│   ├── crypto_prices (Table)     # Cleaned crypto prices
│   └── news_headlines (Table)    # Filtered news with sentiment
└── gold (Schema)
    ├── market_summary (Table)    # Aggregated market metrics
    └── sentiment_summary (Table) # Sentiment distribution
```

## Technologies Used

- **Databricks** — Cloud data platform for running PySpark workloads
- **Apache Spark / PySpark** — Distributed data processing engine
- **Delta Lake** — ACID transactional storage format
- **Unity Catalog** — Unified governance for data assets, schemas, and volumes
- **Python** — requests, BeautifulSoup for data ingestion
- **CoinGecko API** — Cryptocurrency market data provider

## How to Run

1. **Clone the repo** into a Databricks Git folder
2. **Run the Live Ingestion notebook** first to fetch fresh data and populate the UC Volume
3. **Run the Bronze Layer notebook** to load raw data into Delta tables
4. **Run the Silver Layer notebook** to clean and enrich the data
5. **Run the Gold Layer notebook** to produce final aggregated analytics tables

> **Note**: Ensure the `financial_market_dev` catalog and its schemas/volumes exist before running. The ingestion notebook will create them if they don't.

## Author

**Samad Khan**  
Email: samadkhan2990@gmail.com  
GitHub: [@Samad503](https://github.com/Samad503)

## License

This project is for educational and portfolio purposes.

<!-- Updated: pipeline ready for deployment -->
