# agents/hybrid_vertex_tool.py
from langchain.tools import Tool
from google.cloud import bigquery
from scripts.config import config

_client = bigquery.Client(project=config.GCP_PROJECT_ID)

def _hybrid_vertex_query(sku: str, top_n: int, w_bpr: float, w_emb: float):
    query = f"""
    DECLARE p_sku STRING DEFAULT @sku;
    DECLARE w_bpr FLOAT64 DEFAULT @w_bpr;
    DECLARE w_emb FLOAT64 DEFAULT @w_emb;

    WITH A_bpr AS (
      SELECT v AS v_a, norm AS n_a
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.custom_item_vecs`
      WHERE sku = p_sku
    ),
    A_emb AS (
      SELECT v AS v_a, norm AS n_a
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.product_text_embeddings`
      WHERE sku = p_sku
    ),
    BPR AS (
      SELECT b.sku AS candidate,
        (SELECT SUM(av*bv)
         FROM A_bpr, UNNEST(A_bpr.v_a) av WITH OFFSET i
         JOIN UNNEST(b.v) bv WITH OFFSET j ON i=j) / (A_bpr.n_a * b.norm) AS cosine_bpr
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.custom_item_vecs` b, A_bpr
      WHERE b.sku != p_sku AND A_bpr.n_a IS NOT NULL AND b.norm IS NOT NULL
    ),
    EMB AS (
      SELECT b.sku AS candidate,
        (SELECT SUM(av*bv)
         FROM A_emb, UNNEST(A_emb.v_a) av WITH OFFSET i
         JOIN UNNEST(b.v) bv WITH OFFSET j ON i=j) / (A_emb.n_a * b.norm) AS cosine_emb
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.product_text_embeddings` b, A_emb
      WHERE b.sku != p_sku AND A_emb.n_a IS NOT NULL AND b.norm IS NOT NULL
    ),
    BLEND AS (
      SELECT
        COALESCE(BPR.candidate, EMB.candidate) AS candidate,
        COALESCE(cosine_bpr, 0.0) AS cosine_bpr,
        COALESCE(cosine_emb, 0.0) AS cosine_emb,
        (w_bpr * COALESCE(cosine_bpr,0.0)) + (w_emb * COALESCE(cosine_emb,0.0)) AS hybrid_score
      FROM BPR
      FULL OUTER JOIN EMB USING(candidate)
    )
    SELECT candidate
    FROM BLEND
    ORDER BY hybrid_score DESC
    LIMIT @k;
    """
    job = _client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sku","STRING", sku),
                bigquery.ScalarQueryParameter("k","INT64", top_n),
                bigquery.ScalarQueryParameter("w_bpr","FLOAT64", w_bpr),
                bigquery.ScalarQueryParameter("w_emb","FLOAT64", w_emb),
            ]
        )
    )
    return [r.candidate for r in job]

def hybrid_vertex_cross_sell(sku: str, top_n: int = 5) -> str:
    items = _hybrid_vertex_query(sku, top_n, w_bpr=0.55, w_emb=0.45)
    return "Hybrid (BPR + VertexEmb) cross-sell for {}: {}".format(sku, ", ".join(items) if items else "no candidates")

HybridVertexCrossSell = Tool(
    name="HybridVertexCrossSell",
    func=hybrid_vertex_cross_sell,
    description="Hybrid cross-sell using custom BPR vectors and Vertex text embeddings. Input: SKU"
)
