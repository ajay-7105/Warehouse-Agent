from google.cloud import bigquery
from scripts.config import config          # or: from .config import config

client = bigquery.Client(project=config.GCP_PROJECT_ID)

def get_cross_sells(sku: str, top_n: int = 3) -> str:
    query = f"""
    SELECT
      IF(sku_a = @sku, sku_b, sku_a) AS suggested_sku,
      pair_orders
    FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.cross_sell_pairs`
    WHERE sku_a = @sku OR sku_b = @sku
    ORDER BY pair_orders DESC
    LIMIT {top_n}
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("sku", "STRING", sku)]
        ),
    )
    suggestions = [row.suggested_sku for row in job]    # job.result() implicit
    return (
        f"Cross-sell for {sku}: {', '.join(suggestions)}"
        if suggestions
        else f"No cross-sell data for {sku}."
    )
