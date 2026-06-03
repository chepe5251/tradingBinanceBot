"""Position monitoring package.

Public API:
    PositionMonitor — facade for an active monitored position.

Internal modules:
    facade, runtime, protection, decisions, logic, orphan, scaling, state.
"""
from bot.monitor.facade import PositionMonitor

__all__ = ["PositionMonitor"]
