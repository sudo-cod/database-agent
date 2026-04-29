# Olist Big Data Analysis System

A full end-to-end data pipeline and LLM-powered SQL agent built on the Brazilian E-Commerce Public Dataset (Olist). Raw CSVs are processed by a Spark ETL pipeline, stored in DuckDB, and queried via natural language through a Streamlit web app.

The system runs entirely in Docker — no local Spark or Java installation required.

## Architecture

```
Raw CSVs → [Spark Pipeline] → Parquet files → [DuckDB] → [SQL Agent] → Streamlit UI
                                                              ↑
                                                   DeepSeek / Claude / GPT-4o
```

## Quick Start

```bash
# 1. Place Olist CSVs in data/raw/
#    Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

# 2. Create .env with your API key (at least one)
echo "DEEPSEEK_API_KEY=your_key" > .env

# 3. Build Docker images
docker compose build

# 4. Start services
docker compose up -d

# 5. Run Spark pipeline (processes all 9 CSVs → 8 parquet directories)
docker compose exec spark python spark/run_pipeline.py

# 6. Load into DuckDB
docker compose exec spark python warehouse/load_duckdb.py

# 7. Open the app
open http://localhost:8501
```

**Other useful URLs:**
- Jupyter Lab: http://localhost:8888
- Spark UI (while a job runs): http://localhost:4040

## Backend Options

| Backend  | Model            | Env Variable        | Notes              |
|----------|------------------|---------------------|--------------------|
| DeepSeek | deepseek-chat    | DEEPSEEK_API_KEY    | Default, cheapest  |
| Anthropic| claude-sonnet-4-6| ANTHROPIC_API_KEY   | Best reasoning     |
| OpenAI   | gpt-4o           | OPENAI_API_KEY      | Strong alternative |

Set the corresponding key in `.env` or enter it directly in the Streamlit sidebar.

## Project Structure

```
olist_project/
├── data/
│   └── raw/                  ← drop Olist CSVs here
├── spark/
│   ├── ingest.py             ← load 9 CSVs into Spark DataFrames
│   ├── clean.py              ← type casting, deduplication, filtering
│   ├── enrich.py             ← computed columns, joins, order_summary view
│   └── run_pipeline.py       ← orchestrator → writes Parquet
├── warehouse/
│   └── load_duckdb.py        ← Parquet → DuckDB tables
├── agent/
│   ├── schema_extractor.py   ← introspect DuckDB schema
│   ├── sql_generator.py      ← LLM → SQL via DeepSeek/Anthropic/OpenAI
│   ├── executor.py           ← run SQL with self-repair loop
│   ├── analyzer.py           ← LLM result interpretation + chart hints
│   └── agent.py              ← high-level SQLAgent orchestrator
├── app/
│   └── streamlit_app.py      ← web UI
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Dataset

Brazilian E-Commerce Public Dataset by Olist.  
Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce  
License: CC BY-NC-SA 4.0

Files needed in `data/raw/`:
- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`
