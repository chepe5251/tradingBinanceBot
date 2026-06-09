"""High-level application lifecycle controller for the trading bot."""
from __future__ import annotations

import os
import signal
import threading
import time

from binance.exceptions import BinanceAPIException, BinanceRequestException
from bot.config import Settings, from_env
from bot.services.bootstrap_service import RuntimeContext, bootstrap_runtime
from bot.services.entry_service import EntryService
from bot.services.position_service import count_active_positions, resume_orphaned_positions
from bot.services.telegram_service import TelegramService

EXCHANGE_RECOVERABLE_ERRORS = (
    BinanceAPIException,
    BinanceRequestException,
    OSError,
    ValueError,
    TypeError,
)


class BotApplication:
    """Coordinates startup, runtime loops, and graceful shutdown."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or from_env()
        self.shutdown_event = threading.Event()
        self.runtime: RuntimeContext | None = None
        self.telegram: TelegramService | None = None
        self.entry_service: EntryService | None = None

    def run(self) -> None:
        """Run bot lifecycle until shutdown signal."""
        self._bootstrap()
        self._install_signal_handlers()
        self._run_orphan_recovery_startup()
        self._start_scheduler()
        try:
            self._heartbeat_loop()
        finally:
            self._shutdown()

    def _bootstrap(self) -> None:
        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        if not self.settings.use_paper_trading and (not api_key or not api_secret):
            raise RuntimeError("Missing BINANCE_API_KEY or BINANCE_API_SECRET in .env")

        self.runtime = bootstrap_runtime(
            settings=self.settings,
            api_key=api_key,
            api_secret=api_secret,
        )
        self.telegram = TelegramService(
            token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            logger=self.runtime.logger,
        )
        self.entry_service = EntryService(
            settings=self.settings,
            stream=self.runtime.stream,
            symbols=self.runtime.symbols,
            context_map=self.runtime.context_map,
            trade_client=self.runtime.trade_client,
            risk=self.runtime.risk,
            position_cache=self.runtime.position_cache,
            get_executor=self.runtime.get_executor,
            logger=self.runtime.logger,
            trades_logger=self.runtime.trades_logger,
            telegram=self.telegram,
            operations=self.runtime.operations,
        )
        self.runtime.operations.bind_telegram(self.telegram)
        self.runtime.operations.record_startup(
            symbols=len(self.runtime.symbols),
            intervals=self.runtime.evaluation_intervals,
        )

    def _install_signal_handlers(self) -> None:
        def _signal_handler(signum: int, frame: object) -> None:  # noqa: ARG001
            if not self.runtime:
                self.shutdown_event.set()
                return
            sig_name = signal.Signals(signum).name
            self.runtime.logger.info("Signal %s received, shutting down.", sig_name)
            self.shutdown_event.set()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _run_orphan_recovery_startup(self) -> None:
        if not self.runtime or self.settings.use_paper_trading:
            return
        try:
            resume_orphaned_positions(
                trade_client=self.runtime.trade_client,
                symbols=self.runtime.symbols,
                stream=self.runtime.stream,
                settings=self.settings,
                get_executor=self.runtime.get_executor,
                risk=self.runtime.risk,
                pos_cache_invalidate=self.runtime.position_cache.invalidate,
                risk_updater=self.runtime.risk.update_trade,
                logger=self.runtime.logger,
                trades_logger=self.runtime.trades_logger,
                operations=self.runtime.operations,
            )
        except EXCHANGE_RECOVERABLE_ERRORS as exc:
            self.runtime.logger.warning("orphan_recovery_failed err=%s", exc)
            self.runtime.operations.record_error(
                stage="orphan_recovery",
                err=exc,
                recoverable=True,
                api_related=True,
            )
        except Exception as exc:  # noqa: BLE001
            # Startup must continue even if orphan recovery has an unexpected edge case.
            self.runtime.logger.warning("orphan_recovery_failed_unexpected err=%s", exc)
            self.runtime.operations.record_error(
                stage="orphan_recovery",
                err=exc,
                recoverable=False,
                api_related=True,
            )

    def _start_scheduler(self) -> None:
        if not self.runtime or not self.entry_service:
            raise RuntimeError("Runtime not initialized")
        callbacks = {
            interval: self.entry_service.make_on_close(interval)
            for interval in self.runtime.evaluation_intervals
        }
        self.runtime.logger.info(
            "Starting scheduler symbols=%d intervals=%s",
            len(self.runtime.symbols),
            self.runtime.evaluation_intervals,
        )
        self.runtime.stream.start_scheduler(callbacks)

    def _heartbeat_loop(self) -> None:
        if not self.runtime:
            return
        last_heartbeat = time.time()
        while not self.shutdown_event.wait(timeout=1):
            if time.time() - last_heartbeat < self.settings.log_heartbeat_sec:
                continue
            self._heartbeat_once()
            last_heartbeat = time.time()

    def _heartbeat_once(self) -> None:
        if not self.runtime:
            return
        status = self.runtime.stream.status()
        self.runtime.logger.info(
            "Heartbeat: bot alive | polls=%s last_close=%s next_close_in=%.0fs scheduler=%s",
            status.get("event_count"),
            status.get("last_closed_ts"),
            status.get("next_close_in_sec", 0),
            status.get("scheduler_alive"),
        )

        open_positions = 0
        if not self.settings.use_paper_trading:
            try:
                positions_snapshot = self.runtime.position_cache.get()
                open_positions, _ = count_active_positions(positions_snapshot)
            except EXCHANGE_RECOVERABLE_ERRORS as exc:
                self.runtime.logger.warning("heartbeat_position_count_failed err=%s", exc)
                self.runtime.operations.record_error(
                    stage="heartbeat_positions",
                    err=exc,
                    recoverable=True,
                    api_related=True,
                )

        self.runtime.operations.heartbeat(
            stream_status=status,
            risk_state=self.runtime.risk.snapshot(),
            open_positions=open_positions,
        )
        self._persist_alive_file()

    @staticmethod
    def _persist_alive_file() -> None:
        try:
            os.makedirs("logs", exist_ok=True)
            with open("logs/.alive", "w", encoding="utf-8") as alive_file:
                alive_file.write(str(time.time()))
        except OSError:
            pass

    def _shutdown(self) -> None:
        if not self.runtime:
            return
        try:
            self.runtime.risk.repository.save(self.runtime.risk.state)
        except OSError as exc:
            self.runtime.logger.warning("risk_state_save_failed err=%s", exc)
        try:
            self.runtime.operations.force_report()
            self.runtime.operations.save_state(self.settings.ops_state_json_path)
        except OSError as exc:
            self.runtime.logger.warning("ops_state_save_failed err=%s", exc)
        finally:
            self.runtime.stream.stop()
        self.runtime.logger.info("Shutdown complete.")
