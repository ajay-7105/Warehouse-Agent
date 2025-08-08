
#!/usr/bin/env bash
# Pub/Sub → BigQuery setup script
# Usage: ./pubsub_to_bq_setup.sh <PROJECT_ID> <DATASET>
PROJECT=$1
DATASET=$2

# Topics
gcloud pubsub topics create picking-events --project=$PROJECT
gcloud pubsub topics create receiving-events --project=$PROJECT

# Schemas
gcloud pubsub schemas create picking_schema --project=$PROJECT --type=avro --definition-file=schemas/pick_event.avsc
gcloud pubsub schemas create receiving_schema --project=$PROJECT --type=avro --definition-file=schemas/receipt_event.avsc

gcloud pubsub topics update picking-events --project=$PROJECT --schema=picking_schema --message-encoding=json
gcloud pubsub topics update receiving-events --project=$PROJECT --schema=receiving_schema --message-encoding=json

# Tables must exist
bq mk --table $PROJECT:$DATASET.fact_pick event_ts:TIMESTAMP,order_id:STRING,sku:STRING,qty:INT64,location_id:STRING,staff:STRING
bq mk --table $PROJECT:$DATASET.fact_receipt event_ts:TIMESTAMP,sku:STRING,qty:INT64,location_id:STRING,supplier:STRING

# Subscriptions (Storage Write API)
gcloud pubsub subscriptions create picking-to-bq --project=$PROJECT \
  --topic=picking-events --bigquery-table=$PROJECT:$DATASET.fact_pick --use-topic-schema --drop-unknown-fields
gcloud pubsub subscriptions create receiving-to-bq --project=$PROJECT \
  --topic=receiving-events --bigquery-table=$PROJECT:$DATASET.fact_receipt --use-topic-schema --drop-unknown-fields
