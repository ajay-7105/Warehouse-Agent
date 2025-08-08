import pandas as pd
from google.cloud import bigquery
from scripts.config import config

client = bigquery.Client(project=config.GCP_PROJECT_ID)
dataset_ref = bigquery.DatasetReference(config.GCP_PROJECT_ID, config.BQ_DATASET)

def load_df(df: pd.DataFrame, table_name: str, write_disposition="WRITE_TRUNCATE"):
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_name}")

def main():
    # Extract
    products_df  = pd.read_csv("data/products.csv")
    stock_df     = pd.read_csv("data/stock_levels.csv")
    receiving_df = pd.read_csv("data/receiving_logs.csv")
    picks_df     = pd.read_csv("data/picking_logs.csv")

    # Transform
    picks_df['timestamp'] = pd.to_datetime(picks_df['timestamp'])
    picks_df['date']      = picks_df['timestamp'].dt.date
    daily_demand = (picks_df.groupby(['date','sku']).size().reset_index(name='picks'))

    # Load
    for name, df in [("products", products_df),
                     ("stock_levels", stock_df),
                     ("receiving_logs", receiving_df),
                     ("picking_logs", picks_df),
                     ("daily_demand", daily_demand)]:
        load_df(df, name)

    print("ETL complete.")

if __name__ == "__main__":
    main()
