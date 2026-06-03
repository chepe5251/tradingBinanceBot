from __future__ import annotations

import logging
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bot.config import Settings
from bot.services.runtime_controller import BotApplication


@pytest.mark.unit
class RuntimeControllerTests(unittest.TestCase):
    def test_shutdown_stops_stream_even_when_state_persistence_fails(self) -> None:
        settings = Settings()
        app = BotApplication(settings=settings)

        runtime = SimpleNamespace(
            risk=MagicMock(),
            operations=MagicMock(),
            stream=MagicMock(),
            logger=logging.getLogger("test.runtime.shutdown"),
        )
        runtime.risk.save.side_effect = OSError("disk issue")
        runtime.operations.save_state.side_effect = OSError("disk issue")
        app.runtime = runtime

        app._shutdown()  # noqa: SLF001

        runtime.stream.stop.assert_called_once()
        runtime.operations.force_report.assert_called_once()

