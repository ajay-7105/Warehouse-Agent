#!/usr/bin/env python3
"""
Forecast Planner (standalone)
- Reads env vars GCP_PROJECT_ID and BQ_DATASET (or CLI flags).
- Uses demand_forecast if present for the next HORIZON days.
- Otherwise builds a naive forecast (avg of last 30 days of fact_pick).
- Optional: --prefer-bqml trains ARIMA_PLUS (BQML) and materializes demand_forecast.
- Produces inventory_plan with recommended_order_qty and stockout ETA.

Usage:
  python forecast_planner.py --horizon 14 --safety-days 7 \
    --project $GCP_PROJECT_ID --dataset $BQ_DATASET

Auth:
  Use Application Default Credentials (ADC). For local dev either:
  - export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json, or
  - gcloud auth application-default login
"""
import argparse, math, os
from datetime import date, timedelta
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery
from google.oauth2 import service_account

# add near the top of main(), before creating the client:
SA_PATH = "/Users/ajay/project/alpine-alpha-467613-k9-708aeb6f2f6b.json"
CREDS = service_account.Credentials.from_service_account_file(SA_PATH)
def table_exists(client: bigquery.Client, project: str, dataset: str, table: str) -> bool:
    try:
        client.get_table(f"{project}.{dataset}.{table}")
        return True
    except Exception:
        return False

def query_df(client: bigquery.Client, sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe(create_bqstorage_client=True)

def load_df(client: bigquery.Client, df: pd.DataFrame, table_ref: str, write_disposition="WRITE_TRUNCATE"):
    job = client.load_table_from_dataframe(
        df, table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition=write_disposition)
    )
    job.result()

def ensure_daily_demand(client, project, dataset):
    # Build daily_demand from fact_pick (last 180 days for speed; adjust as needed)
    sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.daily_demand` AS
    SELECT DATE(event_ts) AS date, sku, SUM(qty) AS picks
    FROM `{project}.{dataset}.fact_pick`
    WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
    GROUP BY date, sku
    """
    client.query(sql).result()

def ensure_bqml_forecast(client, project, dataset, horizon):
    # Train multi-series ARIMA_PLUS and write demand_forecast (date, sku, predicted_demand)
    ensure_daily_demand(client, project, dataset)
    train_sql = f"""
    CREATE OR REPLACE MODEL `{project}.{dataset}.demand_arima_all`
    OPTIONS(
      MODEL_TYPE='ARIMA_PLUS',
      TIME_SERIES_TIMESTAMP_COL='date',
      TIME_SERIES_DATA_COL='picks',
      TIME_SERIES_ID_COL='sku',
      HOLIDAY_REGION='US',
      HORIZON={horizon}
    ) AS
    SELECT date, sku, picks
    FROM `{project}.{dataset}.daily_demand`
    WHERE date < CURRENT_DATE()
    ORDER BY date;
    """
    client.query(train_sql).result()
    fc_sql = f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.demand_forecast` AS
    SELECT
      CAST(forecast_timestamp AS DATE) AS date,
      sku,
      forecast_value AS predicted_demand
    FROM ML.FORECAST(MODEL `{project}.{dataset}.demand_arima_all`, STRUCT({horizon} AS horizon));
    """
    client.query(fc_sql).result()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID"), help="GCP project ID")
    parser.add_argument("--dataset", default=os.getenv("BQ_DATASET", "warehouse"), help="BigQuery dataset")
    parser.add_argument("--horizon", type=int, default=int(os.getenv("HORIZON_DAYS", 14)), help="Forecast horizon days")
    parser.add_argument("--safety-days", type=int, default=int(os.getenv("SAFETY_DAYS", 7)), dest="safety_days", help="Safety stock days")
    parser.add_argument("--prefer-bqml", action="store_true", help="If no demand_forecast data for horizon, train BQML ARIMA and use it")
    args = parser.parse_args()

    if not args.project:
        raise SystemExit("Project ID not set. Use --project or export GCP_PROJECT_ID.")

    client = bigquery.Client(project=args.project, credentials=CREDS)

    # Try to read an existing demand_forecast covering the horizon
    have_forecast = table_exists(client, args.project, args.dataset, "demand_forecast")
    fc = pd.DataFrame()
    if have_forecast:
        fc = query_df(client, f"""
            SELECT sku, date, predicted_demand
            FROM `{args.project}.{args.dataset}.demand_forecast`
            WHERE date >= CURRENT_DATE() AND date < DATE_ADD(CURRENT_DATE(), INTERVAL {args.horizon} DAY)
        """)

    if fc.empty:
        if args.prefer_bqml:
            print("No forecast found for horizon; training BQML ARIMA_PLUS...")
            ensure_bqml_forecast(client, args.project, args.dataset, args.horizon)
            fc = query_df(client, f"""
                SELECT sku, date, predicted_demand
                FROM `{args.project}.{args.dataset}.demand_forecast`
                WHERE date >= CURRENT_DATE() AND date < DATE_ADD(CURRENT_DATE(), INTERVAL {args.horizon} DAY)
            """)
        if fc.empty:
            print("No forecast available; using naive average of last 30 days from fact_pick.")
            # Fallback: build a flat forecast using last 30 days avg picks per SKU
            daily = query_df(client, f"""
                SELECT sku, DATE(event_ts) AS date, SUM(qty) AS qty
                FROM `{args.project}.{args.dataset}.fact_pick`
                WHERE DATE(event_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY sku, date
            """)
            if daily.empty:
                raise SystemExit("No picks in the last 30 days to build a naive forecast.")
            avg = daily.groupby('sku', as_index=False)['qty'].mean().rename(columns={'qty':'avg_daily'})
            future_dates = pd.date_range(pd.Timestamp.today().normalize(), periods=args.horizon, freq='D')
            fc = avg.assign(key=1).merge(
                pd.DataFrame({'date': future_dates, 'key': 1}), on='key').drop(columns=['key'])
            fc['predicted_demand'] = fc['avg_daily']
            fc = fc[['sku','date','predicted_demand']]

    # Aggregate over horizon per SKU
    horizon_df = (fc.groupby('sku', as_index=False)['predicted_demand']
                  .sum().rename(columns={'predicted_demand':'demand_horizon'}))
    horizon_df['daily'] = horizon_df['demand_horizon'] / float(args.horizon)

    # On-hand (sum across locations)
    stock = query_df(client, f"""
        SELECT sku, SUM(on_hand) AS on_hand
        FROM `{args.project}.{args.dataset}.fact_stock_snapshot`
        GROUP BY sku
    """)
    if stock.empty:
        # If no snapshot, treat as zeros
        stock = pd.DataFrame({'sku': horizon_df['sku'], 'on_hand': 0})

    # Merge + compute plan
    df = horizon_df.merge(stock, on='sku', how='left').fillna({'on_hand': 0})
    df['safety_qty'] = df['daily'] * float(args.safety_days)
    df['net_req'] = (df['demand_horizon'] + df['safety_qty'] - df['on_hand']).clip(lower=0)
    df['recommended_order_qty'] = df['net_req'].apply(lambda x: int(math.ceil(x)))

    def stockout_day(row):
        d = row['daily']
        if d <= 0: return None
        days_cover = row['on_hand'] / d
        return None if days_cover >= (args.horizon + args.safety_days) else int(math.floor(days_cover))

    df['est_days_until_stockout'] = df.apply(stockout_day, axis=1)
    out = df[['sku','on_hand','demand_horizon','safety_qty','recommended_order_qty','est_days_until_stockout']]

    dest = f"{args.project}.{args.dataset}.inventory_plan"
    load_df(client, out, dest, write_disposition="WRITE_TRUNCATE")
    print(f"Wrote {len(out)} rows to {dest}.")

if __name__ == "__main__":
    main()
