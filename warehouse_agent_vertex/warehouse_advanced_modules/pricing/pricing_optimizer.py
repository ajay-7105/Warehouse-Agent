
#!/usr/bin/env python3
"""Dynamic Pricing & Promotion Optimizer
Calculates days-of-cover and writes price_recommendations.
Usage:
  python pricing_optimizer.py --project <id> --dataset whadb --days-cover 30
"""
import argparse, os
from google.cloud import bigquery
import math

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--dataset', default='whadb')
    parser.add_argument('--days-cover', type=int, default=30)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    ds = f"{args.project}.{args.dataset}"

    sql = f"""
    CREATE OR REPLACE TABLE `{ds}.price_recommendations` AS
    WITH joined AS (
      SELECT p.sku,
             p.current_price,
             i.on_hand,
             COALESCE(f.daily, 0) * {args.days_cover} AS target_stock,
             COALESCE(f.daily, 0) AS daily
      FROM `{ds}.dim_product` p
      LEFT JOIN `{ds}.inventory_plan` i USING(sku)
      LEFT JOIN (
        SELECT sku, AVG(predicted_demand) AS daily
        FROM `{ds}.demand_forecast`
        WHERE date >= CURRENT_DATE() AND date < DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY sku
      ) f USING(sku)
    )
    SELECT sku,
           current_price,
           CASE
             WHEN on_hand > target_stock * 1.5 THEN ROUND(current_price * 0.9, 2)
             WHEN on_hand < target_stock * 0.5 THEN ROUND(current_price * 1.1, 2)
             ELSE current_price
           END AS recommended_price,
           on_hand,
           target_stock
    FROM joined;
    """
    client.query(sql).result()
    print("price_recommendations refreshed.")

if __name__ == "__main__":
    main()
