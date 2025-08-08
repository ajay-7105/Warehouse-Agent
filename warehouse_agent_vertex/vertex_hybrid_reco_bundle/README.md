
# Vertex Hybrid Recommender (BPR + Text Embeddings)

This extends the custom recommender by replacing token Jaccard with **Vertex AI text embeddings** for product content.

## Steps

1) **Generate product embeddings**
```bash
PYTHONPATH="$PWD" python -m scripts.vertex_build_embeddings   --project alpine-alpha-467613-k9 --dataset whadb   --vertex-project alpine-alpha-467613-k9 --vertex-location us-central1   --model text-embedding-004 --batch 96
```
Creates: `whadb.product_embeddings (sku, emb ARRAY<FLOAT64>, norm)`

2) **(If not done) Train BPR item vectors**
```bash
PYTHONPATH="$PWD" python -m scripts.custom_bpr_reco   --project alpine-alpha-467613-k9 --dataset whadb --factors 32 --epochs 5 --neg 5
```
Creates: `whadb.custom_item_vecs (sku, v, norm)`

3) **Wire the agent tool**
```python
from agents.vertex_hybrid_tool import VertexHybridCrossSell
tools += [VertexHybridCrossSell]
```
Ask the agent: *"Recommend cross-sell for SKU 100049"*

## Notes
- Requires `google-cloud-aiplatform` (Vertex SDK). Model name default: `text-embedding-004`.
- If you run in a region other than `us-central1`, set `--vertex-location` accordingly and ensure the model is available in that region.
- For large catalogs, consider materializing a **top-K neighbors table** from embeddings to speed up queries.
