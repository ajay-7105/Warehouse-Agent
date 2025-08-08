
#!/usr/bin/env python3
"""Build Vertex AI text embeddings for products and store in BigQuery.

Usage:
  python -m scripts.vertex_build_embeddings --project <bq_project> --dataset whadb       --vertex-project <vertex_project> --vertex-location us-central1       --model text-embedding-004 --batch 96

Creates:
  <project>.<dataset>.product_embeddings (sku STRING, emb ARRAY<FLOAT64>, norm FLOAT64)
"""
import argparse, math, os, sys
from typing import List
from google.cloud import bigquery

# Try modern Vertex SDK first, then fallback
def _get_model(model_name: str):
    try:
        import vertexai
        try:
            # new style
            from vertexai.preview import text_embeddings as te
            vertexai.init()
            return ("preview", te.TextEmbeddingModel.from_pretrained(model_name))
        except Exception:
            # old style
            from vertexai.language_models import TextEmbeddingModel
            vertexai.init()
            return ("legacy", TextEmbeddingModel.from_pretrained(model_name))
    except Exception as e:
        raise SystemExit(f"Vertex AI SDK not available. Install google-cloud-aiplatform>=1.38 and retry. Error: {e}")

def _embed_batch(model, texts: List[str]):
    # Works for either preview or legacy class
    try:
        embs = model.get_embeddings(texts)
        # preview returns list of objects with 'values' or 'embedding.values'
        out = []
        for e in embs:
            vec = getattr(e, "values", None)
            if vec is None and hasattr(e, "embedding"):
                vec = getattr(e.embedding, "values", None)
            if vec is None:
                raise RuntimeError("Unexpected embedding response shape")
            out.append([float(x) for x in vec])
        return out
    except TypeError:
        # some SDKs expect kwargs
        embs = model.get_embeddings(input=texts)
        out = []
        for e in embs:
            vec = getattr(e, "values", None)
            if vec is None and hasattr(e, "embedding"):
                vec = getattr(e.embedding, "values", None)
            out.append([float(x) for x in vec])
        return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", help="BigQuery project id (tables live here)")
    ap.add_argument("--dataset", default="whadb")
    ap.add_argument("--vertex-project", default=None, help="Vertex AI project (defaults to --project)")
    ap.add_argument("--vertex-location", default=os.getenv("VERTEX_LOCATION","us-central1"))
    ap.add_argument("--model", default="text-embedding-004")
    ap.add_argument("--batch", type=int, default=96)
    args = ap.parse_args()

    bq_project = args.project or os.getenv("GCP_PROJECT_ID")
    if not bq_project:
        raise SystemExit("Missing project. Use --project or export GCP_PROJECT_ID.")
    vtx_project = args.vertex_project or bq_project

    # Init Vertex
    try:
        import vertexai
        vertexai.init(project=vtx_project, location=args.vertex_location)
    except Exception as e:
        raise SystemExit(f"Failed to init Vertex AI: {e}")

    which, model = _get_model(args.model)

    client = bigquery.Client(project=bq_project)
    ds = f"{bq_project}.{args.dataset}"

    # Pull source product text
    sql = f"""
    SELECT
      CAST(sku AS STRING) AS sku,
      CONCAT_WS(' ',
        IFNULL(description,''),
        IFNULL(category,''),
        IFNULL(brand,''),
        IFNULL(size,''),
        IFNULL(product_function,'')) AS text
    FROM `{ds}.dim_product`
    WHERE sku IS NOT NULL
    """
    df = client.query(sql).to_dataframe(create_bqstorage_client=True)
    if df.empty:
        raise SystemExit("No rows found in dim_product.")

    # Prepare target table
    client.query(f"""
      CREATE OR REPLACE TABLE `{ds}.product_embeddings` (
        sku STRING,
        emb ARRAY<FLOAT64>,
        norm FLOAT64
      )""").result()

    # Batch embed
    rows = []
    batch = int(args.batch)
    for i in range(0, len(df), batch):
        chunk = df.iloc[i:i+batch]
        texts = chunk["text"].fillna("").astype(str).tolist()
        vecs = _embed_batch(model, texts)
        for (sku, _txt), v in zip(chunk[["sku","text"]].itertuples(index=False), vecs):
            norm = float(math.sqrt(sum(x*x for x in v)) + 1e-9)
            rows.append({"sku": sku, "emb": [float(x) for x in v], "norm": norm})
        # Write in blocks of ~1000
        if len(rows) >= 1000:
            client.insert_rows_json(f"{ds}.product_embeddings", rows)
            rows = []
    if rows:
        client.insert_rows_json(f"{ds}.product_embeddings", rows)

    print(f"Wrote embeddings to {ds}.product_embeddings (rows={len(df)})")

if __name__ == "__main__":
    main()
