-- Example BigQuery ML ARIMA_PLUS model for ONE SKU
CREATE OR REPLACE MODEL `{{project}}.{{dataset}}.demand_arima_sku_ABC123`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'date',
  time_series_data_col = 'picks',
  horizon = 7
) AS
SELECT date, picks
FROM `{{project}}.{{dataset}}.daily_demand`
WHERE sku = 'ABC123'
ORDER BY date;
