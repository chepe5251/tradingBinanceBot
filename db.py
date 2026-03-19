"""SQLite persistence layer for trade history.

All writes are synchronous and use a per-call connection so the module
is safe to call from any thread without additional locking.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

DB_PATH = "logs/trades.db"


def init_db() -> None:
    """Create the trades table if it does not exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at     TEXT    NOT NULL,
            closed_at     TEXT,
            symbol        TEXT    NOT NULL,
            side          TEXT    NOT NULL,
            entry_price   REAL,
            exit_price    REAL,
            qty           REAL,
            tp            REAL,
            sl            REAL,
            result        TEXT,
            pnl           REAL
        )
    """)
    conn.commit()
    conn.close()


def record_entry(
    symbol: str,
    side: str,
    entry_price: float,
    qty: float,
    tp: float,
    sl: float,
) -> int:
    """Insert a new trade row and return its id."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        """
        INSERT INTO trades (opened_at, symbol, side, entry_price, qty, tp, sl)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            symbol,
            side,
            entry_price,
            qty,
            tp,
            sl,
        ),
    )
    trade_id = cur.lastrowid
    conn.commit()
    conn.close()
    return trade_id  # type: ignore[return-value]


def record_exit(trade_id: int, exit_price: float, result: str, pnl: float) -> None:
    """Stamp the exit fields on an existing trade row."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        UPDATE trades
        SET closed_at = ?, exit_price = ?, result = ?, pnl = ?
        WHERE id = ?
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            exit_price,
            result,
            pnl,
            trade_id,
        ),
    )
    conn.commit()
    conn.close()


def record_scale(trade_id: int, new_entry: float, new_qty: float, new_tp: float) -> None:
    """Update average entry, qty and TP after a scale-in event."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        UPDATE trades
        SET entry_price = ?, qty = ?, tp = ?
        WHERE id = ?
        """,
        (new_entry, new_qty, new_tp, trade_id),
    )
    conn.commit()
    conn.close()
