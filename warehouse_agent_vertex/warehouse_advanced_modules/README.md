
# Advanced Modules

This bundle adds three advanced capabilities to your warehouse agent:

1. **Slotting Optimizer** (`slotting/`)
   * Python optimizer (`slotting_optimizer.py`) plus SQL template.
   * Outputs `slotting_move_list` in BigQuery.

2. **Dynamic Pricing & Promotion Optimizer** (`pricing/`)
   * Computes `price_recommendations` table.
   * Simple heuristic: markdown if >150% target cover, surge if <50%.

3. **Streaming Ingest** (`streaming/`)
   * Shell script to create Pub/Sub topics, schemas, and BQ subscriptions.
   * Avro schemas included under `streaming/schemas/`.

See each sub‑folder for usage instructions.
