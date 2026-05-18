from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

try:  # optional dependency
    from pydantic import BaseModel
except Exception:  # pragma: no cover
    BaseModel = None

MAX_RECONNECT_ATTEMPTS = 6
MAX_RETRY_DELAY = 60

LOGGER = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(UTC)


def in_order_pause_window(now: datetime) -> bool:
    return now.astimezone(UTC).hour == 22


def backoff_delay(attempt: int, max_retry_delay: int = MAX_RETRY_DELAY) -> int:
    return min(2 ** attempt, max_retry_delay)


def _coerce_model_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return dict(value)


if BaseModel:

    class PriceUpdate(BaseModel):
        received_at: str
        symbol: str
        ig_ts_ms: int
        timestamp: float
        bid: float
        offer: float
        high: float | None = None
        low: float | None = None
        state: str | None = None

    class AccountUpdate(BaseModel):
        available_cash: float
        pnl: float
        received_at: str

else:

    @dataclass
    class PriceUpdate:
        received_at: str
        symbol: str
        ig_ts_ms: int
        timestamp: float
        bid: float
        offer: float
        high: float | None = None
        low: float | None = None
        state: str | None = None

        def model_dump(self) -> dict[str, Any]:
            return self.__dict__.copy()

    @dataclass
    class AccountUpdate:
        available_cash: float
        pnl: float
        received_at: str


class SQLitePriceStore:
    def __init__(self, directory: Path, now_fn: Callable[[], datetime] = utcnow) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        stamp = now_fn().strftime("%Y%m%d_%H%M%S")
        self.path = directory / f"prices_{stamp}.db"
        self.conn = sqlite3.connect(self.path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                received_at  TEXT    NOT NULL,
                symbol       TEXT    NOT NULL,
                ig_ts_ms     INTEGER NOT NULL,
                timestamp    REAL    NOT NULL,
                bid          REAL    NOT NULL,
                offer        REAL    NOT NULL,
                high         REAL,
                low          REAL,
                state        CHAR
            )
            """
        )
        self.conn.commit()

    def write(self, update: PriceUpdate) -> None:
        row = _coerce_model_dict(update)
        self.conn.execute(
            """
            INSERT INTO prices(received_at, symbol, ig_ts_ms, timestamp, bid, offer, high, low, state)
            VALUES(:received_at, :symbol, :ig_ts_ms, :timestamp, :bid, :offer, :high, :low, :state)
            """,
            row,
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class StatusBar:
    def __init__(self) -> None:
        self.last_price: tuple[datetime, str] | None = None
        self.last_confirm: tuple[datetime, str] | None = None
        self.last_account: tuple[datetime, str] | None = None

    @staticmethod
    def _format_entry(now: datetime, event: tuple[datetime, str] | None, label: str) -> str:
        if event is None:
            return f"{label}:none"
        event_time, summary = event
        age = int((now - event_time).total_seconds())
        suffix = f" ({age}s ago)" if age > 60 else ""
        return f"{label}:{summary}{suffix}"

    def render(self, now: datetime | None = None) -> str:
        now = now or utcnow()
        parts = [
            now.strftime("%H:%M:%S"),
            self._format_entry(now, self.last_price, "price"),
            self._format_entry(now, self.last_confirm, "confirm"),
            self._format_entry(now, self.last_account, "account"),
        ]
        return " | ".join(parts)

    def flush(self, now: datetime | None = None) -> None:
        sys.stdout.write("\r" + self.render(now))
        sys.stdout.flush()


class IGStreamService:
    def __init__(
        self,
        account: str,
        stream_adapter: Any,
        order_adapter: Any,
        price_store: SQLitePriceStore,
        *,
        zmq_publisher: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now_fn: Callable[[], datetime] = utcnow,
        max_attempts: int = MAX_RECONNECT_ATTEMPTS,
        max_delay: int = MAX_RETRY_DELAY,
    ) -> None:
        self.account = account
        self.stream_adapter = stream_adapter
        self.order_adapter = order_adapter
        self.price_store = price_store
        self.zmq_publisher = zmq_publisher
        self.sleep = sleep
        self.now_fn = now_fn
        self.max_attempts = max_attempts
        self.max_delay = max_delay
        self.status_bar = StatusBar()
        self.orders_paused = False

    def publish_price(self, epic: str, update: PriceUpdate) -> None:
        topic = f"PRICE:{self.account}:{epic}"
        payload = _coerce_model_dict(update)
        self.price_store.write(update)
        self.status_bar.last_price = (self.now_fn(), f"{update.symbol} {update.bid}/{update.offer}")
        if self.zmq_publisher is not None:
            self.zmq_publisher.send_multipart([topic.encode(), json.dumps(payload).encode()])

    def execute_market_order(self, order: dict[str, Any]) -> bool:
        if self.orders_paused or in_order_pause_window(self.now_fn()):
            LOGGER.warning("Rejecting order during 22:00-23:00 UTC guard window: %s", order)
            return False
        result = self.order_adapter.place_market_order(order)
        self.status_bar.last_confirm = (self.now_fn(), f"order={order.get('epic', 'unknown')}")
        return bool(result)

    def update_account(self, update: AccountUpdate) -> None:
        self.status_bar.last_account = (
            self.now_fn(),
            f"cash={update.available_cash:.2f} pnl={update.pnl:.2f}",
        )

    def _retry(self, action: Callable[[], None], *, context: str) -> bool:
        for attempt in range(1, self.max_attempts + 1):
            try:
                action()
                return True
            except Exception as exc:  # noqa: BLE001
                if attempt >= self.max_attempts:
                    LOGGER.error("%s failed after %s attempts: %s", context, attempt, exc)
                    return False
                delay = backoff_delay(attempt, self.max_delay)
                LOGGER.warning(
                    "%s failed (attempt %s/%s), retrying in %ss: %s",
                    context,
                    attempt,
                    self.max_attempts,
                    delay,
                    exc,
                )
                self.sleep(delay)
        return False

    def reconnect_stream(self) -> bool:
        def _do_reconnect() -> None:
            self.stream_adapter.disconnect()
            self.stream_adapter.authenticate()
            self.stream_adapter.connect()
            self.stream_adapter.subscribe_all()

        return self._retry(_do_reconnect, context="stream reconnect")

    def rebind_loop(self) -> bool:
        return self._retry(self.stream_adapter.rebind, context="loop rebind")

    def apply_nightly_guard(self, now: datetime | None = None) -> None:
        now = (now or self.now_fn()).astimezone(UTC)
        if now.hour == 22 and not self.orders_paused:
            self.orders_paused = True
            LOGGER.info("22:00 UTC guard started: pausing orders and reconnecting stream")
            self.reconnect_stream()
        elif now.hour >= 23 and self.orders_paused:
            self.orders_paused = False
            LOGGER.info("23:00 UTC guard ended: resuming orders")


def run_status_demo() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bar = StatusBar()
    now = utcnow()
    bar.last_price = (now, "CS.D.EURUSD.CFD.IP 1.12345/1.12355")
    bar.last_confirm = (now, "BUY 1")
    bar.last_account = (now, "cash=10000 pnl=12.50")
    for _ in range(2):
        bar.flush()
        time.sleep(1)
    print()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo-status", action="store_true", help="Print in-place status bar demo")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if args.demo_status:
        run_status_demo()


if __name__ == "__main__":
    main()
