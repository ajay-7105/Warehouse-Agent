import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    # Core GCP
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "my-project")
    BQ_DATASET     = os.getenv("BQ_DATASET", "warehouse")
    SQLALCHEMY_BQ_URI = f"bigquery://{GCP_PROJECT_ID}/{BQ_DATASET}"

    # Vertex AI
    VERTEX_PROJECT_ID = os.getenv("VERTEX_PROJECT_ID", GCP_PROJECT_ID)
    VERTEX_LOCATION   = os.getenv("VERTEX_LOCATION", "us-central1")
    VERTEX_MODEL_NAME = os.getenv("VERTEX_MODEL_NAME", "gemini-1.5-pro")  # or 1.0-pro, etc.

    # Legacy/OpenAI optional (set only if needed)
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")  

    # Agent params
    MAX_AUTO_RESTOCK = int(os.getenv("MAX_AUTO_RESTOCK", "100"))

config = Config()
