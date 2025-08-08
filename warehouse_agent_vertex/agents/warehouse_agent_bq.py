from langchain.agents import Tool, AgentType, initialize_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.utilities import SQLDatabase

from langchain_google_vertexai import ChatVertexAI
from scripts.vertex_init import init_vertex
from scripts.config import config
from scripts.cross_sell_bq import get_cross_sells

from google.cloud import bigquery

# Init Vertex
init_vertex()

# DB via SQLAlchemy BigQuery dialect
db = SQLDatabase.from_uri(config.SQLALCHEMY_BQ_URI)

# LLM (Gemini)
llm = ChatVertexAI(
    model=config.VERTEX_MODEL_NAME,
    temperature=0,
    max_output_tokens=2048
)

# SQL tools
sql_toolkit = SQLDatabaseToolkit(db=db, llm=llm)
sql_tools = sql_toolkit.get_tools()

# Forecast lookup tool
def forecast_lookup(sku: str) -> str:
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query = f"""
      SELECT date, predicted_demand
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.demand_forecast`
      WHERE sku = @sku
      ORDER BY date
      LIMIT 7
    """
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("sku","STRING",sku)]
    ))
    rows = list(job.result())
    if not rows:
        return f"No forecast for {sku}"
    return " | ".join(f"{r.date}: {int(r.predicted_demand)}" for r in rows)

forecast_tool = Tool(
    name="ForecastLookup",
    func=forecast_lookup,
    description="Get next 7-day demand forecast for a SKU"
)

# Restock tool with human gate
def trigger_restock_with_gate(payload: str) -> str:
    parts = payload.split()
    if len(parts) != 2:
        return "Usage: '<SKU> <amount>'"
    sku, amt = parts[0], int(parts[1])
    if amt > config.MAX_AUTO_RESTOCK:
        client = bigquery.Client(project=config.GCP_PROJECT_ID)
        table = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.pending_actions"
        rows_to_insert = [{"action_type":"RESTOCK","sku":sku,"amount":amt}]
        errors = client.insert_rows_json(table, rows_to_insert)
        return (f"Error inserting pending action: {errors}"
                if errors else f"Restock {sku} ({amt}) pending human approval.")
    return f"Restock order placed for {sku} amount {amt}."

restock_tool = Tool(
    name="RestockOrder",
    func=trigger_restock_with_gate,
    description="Place restock: input '<SKU> <amount>' (auto if small, else approval)"
)

cross_sell_tool = Tool(
    name="CrossSellSuggest",
    func=get_cross_sells,
    description="Suggest cross-sell items: input SKU id"
)

tools = sql_tools + [forecast_tool, restock_tool, cross_sell_tool]

agent = initialize_agent(tools, llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)

if __name__ == "__main__":
    q = "List SKUs below safety stock and suggest restocks for next week"
    print(agent.run(q))
