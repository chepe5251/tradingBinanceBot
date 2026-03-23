"""Application entrypoint for the Binance Futures trading bot."""
from __future__ import annotations

import os
import signal
import threading
import time

from binance.exceptions import BinanceAPIException, BinanceRequestException

from config import from_env
from services.bootstrap_service import bootstrap_runtime
from services.entry_service import EntryService
from services.position_service import resume_orphaned_positions
from services.telegram_service import TelegramService


def main() -> None:
    """Bootstrap services and run the scheduler heartbeat loop."""
    settings = from_env()
    api_key = os.getenv("BINANCE_API_KEY", "").strip()
    api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
    if not settings.use_paper_trading and (not api_key or not api_secret):
        raise RuntimeError("Missing BINANCE_API_KEY or BINANCE_API_SECRET in .env")

    runtime = bootstrap_runtime(settings=settings, api_key=api_key, api_secret=api_secret)
    telegram = TelegramService(
        token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        logger=runtime.logger,
    )

    entry_service = EntryService(
        settings=settings,
        stream=runtime.stream,
        symbols=runtime.symbols,
        context_map=runtime.context_map,
        trade_client=runtime.trade_client,
        risk=runtime.risk,
        position_cache=runtime.position_cache,
        get_executor=runtime.get_executor,
        logger=runtime.logger,
        trades_logger=runtime.trades_logger,
        telegram=telegram,
    )

    shutdown = threading.Event()

    def _signal_handler(signum: int, frame: object) -> None:  # noqa: ARG001
        sig_name = signal.Signals(signum).name
        runtime.logger.info("Signal %s received, shutting down.", sig_name)
        shutdown.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    if not settings.use_paper_trading:
        try:
            resume_orphaned_positions(
                trade_client=runtime.trade_client,
                symbols=runtime.symbols,
                stream=runtime.stream,
                settings=settings,
                get_executor=runtime.get_executor,
                risk=runtime.risk,
                pos_cache_invalidate=runtime.position_cache.invalidate,
                risk_updater=runtime.risk.update_trade,
                logger=runtime.logger,
                trades_logger=runtime.trades_logger,
            )
        except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
            runtime.logger.warning("orphan_recovery_failed err=%s", exc)
        except Exception as exc:  # noqa: BLE001
            # Keep broad fallback: orphan recovery should never abort startup.
            runtime.logger.warning("orphan_recovery_failed_unexpected err=%s", exc)

    callbacks = {
        interval: entry_service.make_on_close(interval) for interval in runtime.evaluation_intervals
    }
    runtime.logger.info(
        "Starting scheduler symbols=%d intervals=%s",
        len(runtime.symbols),
        runtime.evaluation_intervals,
    )
    runtime.stream.start_scheduler(callbacks)

    last_heartbeat = time.time()
    while not shutdown.wait(timeout=1):
        if time.time() - last_heartbeat < settings.log_heartbeat_sec:
            continue
        status = runtime.stream.status()
        runtime.logger.info(
            "Heartbeat: bot alive | polls=%s last_close=%s next_close_in=%.0fs scheduler=%s",
            status.get("event_count"),
            status.get("last_closed_ts"),
            status.get("next_close_in_sec", 0),
            status.get("scheduler_alive"),
        )
        try:
            os.makedirs("logs", exist_ok=True)
            with open("logs/.alive", "w", encoding="utf-8") as alive_file:
                alive_file.write(str(time.time()))
        except OSError:
            pass
        last_heartbeat = time.time()

    runtime.logger.info("Shutdown complete.")
    runtime.stream.stop()


if __name__ == "__main__":
    main()
