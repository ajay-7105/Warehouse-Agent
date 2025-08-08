# Autonomous Warehouse Operations Agent (Vertex AI + BigQuery Edition)

This repo is a **basic scaffold** for an end-to-end Agentic AI system that runs an autonomous warehouse operations agent using **Google BigQuery** for warehousing and **Vertex AI (Gemini)** for the LLM layer.

## What’s Inside

- **ETL → BigQuery** (`scripts/etl_bq.py`)
- **Demand Forecasting** (`scripts/forecasting_bq.py`) with Prophet (or optional BQML SQL)
- **Cross-sell analysis** via pure BigQuery SQL (`scripts/cross_sell_pairs.sql`) + helper (`scripts/cross_sell_bq.py`)
- **LangChain Agent** (`agents/warehouse_agent_bq.py`) using Vertex AI (Gemini) + tools (SQL, forecast, restock with human gate, cross-sell)
- **FastAPI API** (`app/`) for approvals / integration
- **Airflow/Composer DAG** (`dags/warehouse_dag.py`) for scheduling ETL/forecast jobs
- `.env.example`, `requirements.txt`, and detailed configs

> ⚠️ This is a scaffold, not production code. Harden, secure, and test for real deployments.

---

## 1. Setup

### 1.1 Prerequisites
- Python 3.10+
- GCP project with **BigQuery** & **Vertex AI** APIs enabled
- Service Account JSON key (or ADC if running on GCP)
- (Optional) Airflow / Cloud Composer
- (Optional) Docker, Terraform

### 1.2 Environment Variables

Create a `.env` (or export env vars):

```
GCP_PROJECT_ID=your-project-id
BQ_DATASET=warehouse

# Vertex AI
VERTEX_PROJECT_ID=your-project-id
VERTEX_LOCATION=us-central1
VERTEX_MODEL_NAME=gemini-1.5-pro

# LLM alt (keep if dual-running with OpenAI; else ignore)
OPENAI_API_KEY=sk-...

# GCP auth
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Agent thresholds
MAX_AUTO_RESTOCK=100
```

### 1.3 Install Deps

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Data & ETL

Place CSVs in `data/`:
- `products.csv`
- `stock_levels.csv`
- `receiving_logs.csv`
- `picking_logs.csv`

Run ETL:
```bash
python scripts/etl_bq.py
```

This writes tables into BigQuery: `products`, `stock_levels`, `receiving_logs`, `picking_logs`, `daily_demand`.

---

## 3. Forecasting

### Python Prophet
```bash
python scripts/forecasting_bq.py
```
Writes `demand_forecast` to BigQuery.

### BigQuery ML (ARIMA_PLUS)
Use `scripts/forecasting_bqml.sql` in the BQ console or through Airflow.

---

## 4. Cross-Sell Analysis

1. Run `scripts/cross_sell_pairs.sql` in BigQuery (creates/updates `cross_sell_pairs`).
2. Agent uses `scripts/cross_sell_bq.py:get_cross_sells` to retrieve suggestions.

---

## 5. Agent (Vertex AI)

```bash
python agents/warehouse_agent_bq.py
```
This initializes Vertex AI, then runs a LangChain agent with tools:

- **SQL** via SQLAlchemy + pybigquery
- **ForecastLookup**
- **RestockOrder** (with human approval gate)
- **CrossSellSuggest**

---

## 6. Human-in-the-loop

Large restock actions are inserted into `pending_actions` table (BigQuery). Approve them via FastAPI:

```bash
uvicorn app.main:app --reload
# POST /approve/{action_id}
```

---

## 7. Airflow / Composer

`dags/warehouse_dag.py` shows a simple DAG. Upload to Composer or run local Airflow.

---

## 8. Deploy

### Local
- Scripts run locally; BQ/Vertex are remote.
- FastAPI on localhost.

### Cloud
- Dockerize & deploy to Cloud Run/ECS/Azure
- Use Composer / Cloud Scheduler for jobs
- Secrets in Secret Manager

### Hybrid
- Heavy ETL local/on-prem → push to BQ
- Agent service in Cloud (Vertex AI + BQ)

---

## 9. Next Steps

- Add unit/integration tests
- Add retries/backoff & observability (logging, tracing, alerts)
- Switch Prophet → BQML or Vertex AutoML Tables
- Replace LangChain tools with Gemini function-calling (advanced)
- Terraform infra as code

Enjoy & extend! 🚀
