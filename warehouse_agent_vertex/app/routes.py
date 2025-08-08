from fastapi import APIRouter
from google.cloud import bigquery
from scripts.config import config

router = APIRouter()

@router.post("/approve/{action_id}")
def approve_action(action_id: str):
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    sql = f"""
      UPDATE `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.pending_actions`
      SET status='APPROVED'
      WHERE id = @id
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id","STRING",action_id)]
    ))
    job.result()
    return {"msg": "Approved"}
