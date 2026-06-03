"""Application entrypoint for the Binance Futures trading bot."""
from __future__ import annotations

from bot.services.runtime_controller import BotApplication


def main() -> None:
    """Run the bot application lifecycle."""
    BotApplication().run()


if __name__ == "__main__":
    main()
