from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from ig_stream_service import (
    AccountUpdate,
    IGStreamService,
    PriceUpdate,
    SQLitePriceStore,
    backoff_delay,
)


class _FakeStreamAdapter:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.rebind_failures_remaining = 0

    def disconnect(self) -> None:
        self.calls.append("disconnect")

    def authenticate(self) -> None:
        self.calls.append("authenticate")

    def connect(self) -> None:
        self.calls.append("connect")

    def subscribe_all(self) -> None:
        self.calls.append("subscribe_all")

    def rebind(self) -> None:
        self.calls.append("rebind")
        if self.rebind_failures_remaining > 0:
            self.rebind_failures_remaining -= 1
            raise RuntimeError("rebind failed")


class _FakeOrderAdapter:
    def __init__(self) -> None:
        self.orders: list[dict] = []

    def place_market_order(self, order: dict) -> bool:
        self.orders.append(order)
        return True


class IGStreamServiceTests(unittest.TestCase):
    def test_sqlite_price_store_creates_timestamped_db_and_writes_rows(self) -> None:
        fixed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLitePriceStore(Path(temp_dir), now_fn=lambda: fixed)
            self.assertTrue(store.path.name.startswith("prices_20260102_030405"))
            store.write(
                PriceUpdate(
                    received_at="2026-01-02T03:04:05Z",
                    symbol="CS.D.EURUSD.CFD.IP",
                    ig_ts_ms=1700000000000,
                    timestamp=1700000000.0,
                    bid=1.12,
                    offer=1.13,
                    high=1.2,
                    low=1.1,
                    state="TRADEABLE",
                )
            )
            store.close()

            conn = sqlite3.connect(store.path)
            row = conn.execute(
                "SELECT received_at, symbol, ig_ts_ms, bid, offer, state FROM prices"
            ).fetchone()
            conn.close()
            self.assertEqual(
                row,
                (
                    "2026-01-02T03:04:05Z",
                    "CS.D.EURUSD.CFD.IP",
                    1700000000000,
                    1.12,
                    1.13,
                    "TRADEABLE",
                ),
            )

    def test_guard_window_rejects_orders_and_resumes_after_23(self) -> None:
        fake_stream = _FakeStreamAdapter()
        fake_orders = _FakeOrderAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLitePriceStore(Path(temp_dir))
            service = IGStreamService(
                account="ABC123",
                stream_adapter=fake_stream,
                order_adapter=fake_orders,
                price_store=store,
                now_fn=lambda: datetime(2026, 1, 2, 22, 10, tzinfo=UTC),
            )

            service.apply_nightly_guard(datetime(2026, 1, 2, 22, 0, tzinfo=UTC))
            self.assertTrue(service.orders_paused)
            self.assertFalse(service.execute_market_order({"epic": "CS.D.EURUSD.CFD.IP"}))
            self.assertEqual(fake_orders.orders, [])
            self.assertEqual(fake_stream.calls, ["disconnect", "authenticate", "connect", "subscribe_all"])

            service.apply_nightly_guard(datetime(2026, 1, 2, 23, 0, tzinfo=UTC))
            self.assertFalse(service.orders_paused)
            store.close()

    def test_rebind_retries_with_exponential_backoff(self) -> None:
        fake_stream = _FakeStreamAdapter()
        fake_stream.rebind_failures_remaining = 2
        sleeps: list[float] = []
        with tempfile.TemporaryDirectory() as temp_dir:
            service = IGStreamService(
                account="ABC123",
                stream_adapter=fake_stream,
                order_adapter=_FakeOrderAdapter(),
                price_store=SQLitePriceStore(Path(temp_dir)),
                sleep=lambda seconds: sleeps.append(seconds),
            )
            self.assertTrue(service.rebind_loop())
            self.assertEqual(sleeps, [2, 4])

    def test_status_bar_tracks_account_update(self) -> None:
        fake_stream = _FakeStreamAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            service = IGStreamService(
                account="ABC123",
                stream_adapter=fake_stream,
                order_adapter=_FakeOrderAdapter(),
                price_store=SQLitePriceStore(Path(temp_dir)),
            )
            service.update_account(AccountUpdate(available_cash=101.5, pnl=-2.0, received_at="now"))
            line = service.status_bar.render(datetime(2026, 1, 2, 0, 0, tzinfo=UTC))
            self.assertIn("account:cash=101.50 pnl=-2.00", line)


class BackoffTests(unittest.TestCase):
    def test_backoff_delay_caps_to_max_delay(self) -> None:
        self.assertEqual(backoff_delay(1, max_retry_delay=5), 2)
        self.assertEqual(backoff_delay(2, max_retry_delay=5), 4)
        self.assertEqual(backoff_delay(3, max_retry_delay=5), 5)


if __name__ == "__main__":
    unittest.main()
