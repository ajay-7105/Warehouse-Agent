from fastapi import FastAPI
from app.routes import router

app = FastAPI(title="Warehouse Agent API (Vertex)")

app.include_router(router)

@app.get("/")
def root():
    return {"msg": "Warehouse Agent API is running"}
