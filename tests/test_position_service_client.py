from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bot.services.position_service import configure_client


@pytest.mark.unit
def test_configure_client_retries_then_succeeds() -> None:
    mock_client = MagicMock()
    with patch("bot.services.position_service.Client", side_effect=[OSError("dns"), mock_client]) as client_ctor:
        with patch("bot.services.position_service.time.sleep", return_value=None):
            client = configure_client("key", "secret", testnet=False)
    assert client is mock_client
    assert client_ctor.call_count == 2


@pytest.mark.unit
def test_configure_client_sets_testnet_futures_url() -> None:
    mock_client = MagicMock()
    with patch("bot.services.position_service.Client", return_value=mock_client):
        client = configure_client("key", "secret", testnet=True)
    assert client is mock_client
    assert mock_client.FUTURES_URL == "https://testnet.binancefuture.com/fapi"

