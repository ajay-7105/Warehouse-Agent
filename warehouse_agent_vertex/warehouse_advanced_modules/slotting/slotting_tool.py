
from langchain.tools import Tool
from scripts.slotting_optimizer import main as run_slotting

SlottingAdvisor = Tool(
    name="SlottingAdvisor",
    func=lambda _: (run_slotting(), "Slotting move list refreshed."),
    description="Recomputes slotting_move_list and returns confirmation"
)
