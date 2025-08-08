
#!/usr/bin/env python3
"""Nightly Slotting Optimizer
Computes velocity, travel cost, cross‑sell adjacency and writes slotting_move_list.
Run:
  python slotting_optimizer.py --project <id> --dataset whadb
"""
import argparse, os, datetime
from google.cloud import bigquery

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--dataset', default='whadb')
    parser.add_argument('--lookback', type=int, default=30, help='days for velocity')
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    ds = f"{args.project}.{args.dataset}"

    # 1. SKU velocity (picks per day)
    velocity_sql = f"""
    CREATE OR REPLACE TABLE `{ds}.sku_velocity` AS
    SELECT sku,
           SUM(qty) / {args.lookback} AS picks_per_day
    FROM `{ds}.fact_pick`
    WHERE DATE(event_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL {args.lookback} DAY)
    GROUP BY sku;
    """
    client.query(velocity_sql).result()

    # 2. Rank locations by travel_cost
    travel_sql = f"""
    CREATE OR REPLACE TABLE `{ds}.loc_rank` AS
    SELECT location_id,
           travel_cost,
           ROW_NUMBER() OVER (ORDER BY travel_cost) AS loc_rank
    FROM `{ds}.dim_location`;
    """
    client.query(travel_sql).result()

    # 3. Current pick-face per SKU (choose min travel location)
    current_slot_sql = f"""
    CREATE OR REPLACE TABLE `{ds}.sku_current_loc` AS
    SELECT v.sku,
           ANY_VALUE(s.location_id) AS cur_loc,
           MIN(l.travel_cost) AS cur_cost
    FROM `{ds}.sku_velocity` v
    JOIN `{ds}.fact_stock_snapshot` s USING(sku)
    JOIN `{ds}.loc_rank` l ON l.location_id = s.location_id
    GROUP BY v.sku;
    """
    client.query(current_slot_sql).result()

    # 4. Desired slot rank target = velocity percentile
    desired_sql = f"""
    CREATE OR REPLACE TABLE `{ds}.slotting_move_list` AS
    WITH pct AS (
      SELECT sku,
             picks_per_day,
             NTILE(100) OVER (ORDER BY picks_per_day DESC) AS velocity_pct
      FROM `{ds}.sku_velocity`
    )
    SELECT p.sku,
           c.cur_loc,
           d.location_id AS proposed_loc,
           c.cur_cost,
           l.travel_cost AS proposed_cost,
           c.cur_cost - l.travel_cost AS travel_saving
    FROM pct p
    JOIN `{ds}.sku_current_loc` c USING(sku)
    JOIN `{ds}.loc_rank` l ON l.loc_rank = p.velocity_pct  -- map top velocity to best loc
    JOIN `{ds}.loc_rank` d ON d.loc_rank = p.velocity_pct
    WHERE l.location_id <> c.cur_loc
    ORDER BY travel_saving DESC
    LIMIT 100;
    """
    client.query(desired_sql).result()
    print("slotting_move_list refreshed.")

if __name__ == "__main__":
    main()
