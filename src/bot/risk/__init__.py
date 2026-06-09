"""Risk management package.

Public API:
    RiskManager — pure risk logic.
    RiskState — state dataclass.
    JsonRiskRepository — JSON-file persistence adapter.
"""
from bot.risk.manager import RiskManager
from bot.risk.repository import JsonRiskRepository
from bot.risk.state import RiskState

__all__ = ["RiskManager", "RiskState", "JsonRiskRepository"]
