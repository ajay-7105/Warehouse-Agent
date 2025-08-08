CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.cross_sell_pairs` AS
WITH orders AS (
  SELECT order_id, ARRAY_AGG(DISTINCT sku) AS sku_list
  FROM `{{project}}.{{dataset}}.picking_logs`
  GROUP BY order_id
),
pairs AS (
  SELECT a AS sku_a, b AS sku_b
  FROM orders,
  UNNEST(sku_list) AS a,
  UNNEST(sku_list) AS b
  WHERE a < b
)
SELECT sku_a, sku_b, COUNT(*) AS pair_count
FROM pairs
GROUP BY sku_a, sku_b;
