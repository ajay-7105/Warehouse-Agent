from google.cloud import aiplatform
from scripts.config import config

def init_vertex():
    aiplatform.init(
        project=config.VERTEX_PROJECT_ID,
        location=config.VERTEX_LOCATION,
    )
