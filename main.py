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
from services.position_service import count_active_positions, resume_orphaned_positions
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
        operations=runtime.operations,
    )
    try:
        runtime.operations.bind_telegram(telegram)
        runtime.operations.record_startup(
            symbols=len(runtime.symbols),
            intervals=runtime.evaluation_intervals,
        )
    except Exception as exc:  # noqa: BLE001
        runtime.logger.warning("ops_startup_hook_failed err=%s", exc)

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
                operations=runtime.operations,
            )
        except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
            runtime.logger.warning("orphan_recovery_failed err=%s", exc)
            try:
                runtime.operations.record_error(
                    stage="orphan_recovery",
                    err=exc,
                    recoverable=True,
                    api_related=True,
                )
            except Exception:  # noqa: BLE001
                pass
        except Exception as exc:  # noqa: BLE001
            # Keep broad fallback: orphan recovery should never abort startup.
            runtime.logger.warning("orphan_recovery_failed_unexpected err=%s", exc)
            try:
                runtime.operations.record_error(
                    stage="orphan_recovery",
                    err=exc,
                    recoverable=False,
                    api_related=True,
                )
            except Exception:  # noqa: BLE001
                pass

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
        open_positions = 0
        if not settings.use_paper_trading:
            try:
                positions_snapshot = runtime.position_cache.get()
                open_positions, _ = count_active_positions(positions_snapshot)
            except (BinanceAPIException, BinanceRequestException, OSError, ValueError, TypeError) as exc:
                runtime.logger.warning("heartbeat_position_count_failed err=%s", exc)
                try:
                    runtime.operations.record_error(
                        stage="heartbeat_positions",
                        err=exc,
                        recoverable=True,
                        api_related=True,
                    )
                except Exception:  # noqa: BLE001
                    pass
        try:
            runtime.operations.heartbeat(
                stream_status=status,
                risk_state=runtime.risk.snapshot(),
                open_positions=open_positions,
            )
        except Exception as exc:  # noqa: BLE001
            runtime.logger.warning("ops_heartbeat_failed err=%s", exc)
        try:
            os.makedirs("logs", exist_ok=True)
            with open("logs/.alive", "w", encoding="utf-8") as alive_file:
                alive_file.write(str(time.time()))
        except OSError:
            pass
        last_heartbeat = time.time()

    runtime.risk.save("logs/risk_state.json")
    try:
        runtime.operations.force_report()
        runtime.operations.save_state(settings.ops_state_json_path)
    except Exception as exc:  # noqa: BLE001
        runtime.logger.warning("ops_shutdown_hook_failed err=%s", exc)
    runtime.logger.info("Shutdown complete.")
    runtime.stream.stop()


if __name__ == "__main__":
    main()
