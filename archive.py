import sqlite3
import threading
from datetime import datetime
from pathlib import Path


def default_db_path() -> str:
    """Return a timestamped path under ./data/ for the current run."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return f"./data/raw_price_{timestamp}.db"


class TickArchive:
    """Thread-safe SQLite archive for streaming price ticks."""

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_path = default_db_path()

        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._create_table()

    def _create_table(self):
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ticks (
                    timestamp REAL,
                    symbol    TEXT,
                    bid       REAL,
                    offer     REAL,
                    state     TEXT
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ticks_symbol ON ticks (symbol)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks (timestamp)"
            )
            self._conn.commit()

    def insert(self, timestamp: float, symbol: str, bid: float, offer: float, state: str):
        with self._lock:
            self._conn.execute(
                "INSERT INTO ticks (timestamp, symbol, bid, offer, state) VALUES (?, ?, ?, ?, ?)",
                (timestamp, symbol, bid, offer, state),
            )
            self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()
