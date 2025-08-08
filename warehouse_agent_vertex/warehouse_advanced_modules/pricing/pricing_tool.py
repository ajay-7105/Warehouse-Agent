
from langchain.tools import Tool
from scripts.pricing_optimizer import main as run_pricing

PriceAdvisor = Tool(
    name="PriceAdvisor",
    func=lambda _: (run_pricing(), "Price recommendations refreshed."),
    description="Recomputes price_recommendations and returns confirmation"
)
