#!/usr/bin/env python3
"""Build product text embeddings with Vertex AI and write to BigQuery.

Usage:
  python -m scripts.product_text_embeddings_vertex --project <id> --dataset whadb --model text-embedding-004

Requires:
  pip install google-cloud-aiplatform google-cloud-bigquery
  Env: VERTEX_LOCATION (e.g., us-central1), GOOGLE_APPLICATION_CREDENTIALS
"""
import argparse, os
import numpy as np
from google.cloud import bigquery
from scripts.config import config

try:
    from vertexai import init as vertex_init
    try:
        from vertexai.language_models import TextEmbeddingModel
    except Exception:
        from vertexai.preview.language_models import TextEmbeddingModel
except Exception as e:
    raise SystemExit("Vertex AI SDK not installed. pip install google-cloud-aiplatform") from e

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def build_text(row: dict) -> str:
    parts = [
        str(row.get("description") or ""),
        str(row.get("category") or ""),
        str(row.get("brand") or ""),
        str(row.get("size") or ""),
        str(row.get("product_function") or ""),
    ]
    txt = " | ".join([p for p in parts if p])
    if not txt:
        txt = str(row.get("sku") or "")
    return txt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None)
    ap.add_argument("--dataset", default="whadb")
    ap.add_argument("--model", default=os.getenv("VERTEX_EMBED_MODEL","text-embedding-004"))
    ap.add_argument("--batch", type=int, default=32)
    args = ap.parse_args()

    project = args.project or getattr(config, "GCP_PROJECT_ID", None)
    dataset = args.dataset or getattr(config, "BQ_DATASET", "whadb")
    if not project:
        raise SystemExit("Project not set. Use --project or env GCP_PROJECT_ID.")
    location = os.getenv("VERTEX_LOCATION") or getattr(config, "VERTEX_LOCATION", None)
    if not location:
        raise SystemExit("VERTEX_LOCATION not set. export VERTEX_LOCATION=us-central1")

    vertex_init(project=project, location=location)
    bq = bigquery.Client(project=project)

    sql = f"SELECT sku, description, category, brand, size, product_function FROM `{project}.{dataset}.dim_product`"
    rows = list(bq.query(sql).result())
    if not rows:
        raise SystemExit("dim_product is empty.")

    model = TextEmbeddingModel.from_pretrained(args.model)

    out_rows = []
    for batch in chunked(rows, args.batch):
        texts = [build_text(dict(r)) for r in batch]
        embeddings = model.get_embeddings(texts=texts)
        for r, e in zip(batch, embeddings):
            vec = getattr(e, "values", None)
            if vec is None:
                emb = getattr(e, "embedding", None)
                vec = getattr(emb, "values", None) if emb else None
            if vec is None:
                continue
            v = [float(x) for x in vec]
            norm = float(np.linalg.norm(v) + 1e-9)
            out_rows.append({"sku": r["sku"], "v": v, "norm": norm})

    bq.query(f"CREATE OR REPLACE TABLE `{project}.{dataset}.product_text_embeddings` (sku STRING, v ARRAY<FLOAT64>, norm FLOAT64)").result()

    table_id = f"{project}.{dataset}.product_text_embeddings"
    for i in range(0, len(out_rows), 500):
        chunk = out_rows[i:i+500]
        errors = bq.insert_rows_json(table_id, chunk)
        if errors:
            raise RuntimeError(f"Insert errors: {errors}")
    print(f"Wrote {len(out_rows)} embeddings to {table_id}")

if __name__ == "__main__":
    main()
