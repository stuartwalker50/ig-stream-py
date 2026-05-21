import argparse
import json
import signal
import sys
import logging
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from dataclasses import dataclass

import zmq

from lightstreamer.client import (
    Subscription,
    SubscriptionListener,
    ItemUpdate,
    ClientListener,
)

from trading_ig import IGService, IGStreamService
from archive import TickArchive, default_db_path
from epics import epics

logger = logging.getLogger(__name__)


@dataclass
class StreamMetrics:
    """Shared state for tracking stream metrics."""
    status: str = "DISCONNECTED"
    ticks_received: int = 0
    lock: threading.Lock = None

    def __post_init__(self):
        if self.lock is None:
            self.lock = threading.Lock()


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(message)s",
)

_LONDON = ZoneInfo("Europe/London")
_BLACKOUT_HOUR = 22          # 22:00–22:59 London time
_BACKOFF_INITIAL = 2         # seconds
_BACKOFF_MAX = 1800          # 30 minutes
_ZMQ_PORT_DEMO = 5555        # ZeroMQ PUB port for DEMO accounts
_ZMQ_PORT_LIVE = 5556        # ZeroMQ PUB port for LIVE accounts


def parse_args(args=None):
    """Parse command-line arguments.

    Args:
        args: List of argument strings (defaults to sys.argv[1:]). Used for testing.

    Returns:
        Namespace with parsed arguments.
    """
    parser = argparse.ArgumentParser(description="IG Streaming Market Data Feed")
    parser.add_argument(
        "--no-archive",
        action="store_true",
        default=False,
        help="Disable data archival to disk",
    )
    return parser.parse_args(args)


def next_backoff(current: float, cap: float = _BACKOFF_MAX) -> float:
    """Return the next exponential backoff delay, capped at *cap* seconds."""
    return min(current * 2, cap)


class ZmqPublisher:
    """Thin wrapper around a ZeroMQ PUB socket.

    Publishes JSON-encoded messages on three topics: ``prices``, ``account``,
    and ``trades``.  Each message is sent as a two-frame multipart message:
    ``[topic_bytes, json_payload_bytes]``.
    """

    def __init__(self, endpoint: str = f"tcp://*:{_ZMQ_PORT_DEMO}"):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PUB)
        self._socket.bind(endpoint)
        logger.info(f"ZeroMQ PUB socket bound to {endpoint}")

    def publish(self, topic: str, data: dict) -> None:
        """Send *data* as JSON on *topic*."""
        self._socket.send_multipart([topic.encode(), json.dumps(data).encode()])

    def close(self) -> None:
        self._socket.close()
        self._context.term()


def wait_if_blackout(now: datetime | None = None) -> None:
    """Sleep until 23:00 London time if called during the blackout window (22:xx)."""
    if now is None:
        now = datetime.now(tz=_LONDON)
    if now.hour == _BLACKOUT_HOUR:
        wake = now.replace(hour=23, minute=0, second=0, microsecond=0)
        delay = (wake - now).total_seconds()
        logger.info(f"Blackout window active – sleeping {delay:.0f}s until 23:00 London time")
        time.sleep(delay)


def _status_reporter(metrics: StreamMetrics, stop_event: threading.Event) -> None:
    """Background thread that prints status every 5 seconds."""
    while not stop_event.is_set():
        with metrics.lock:
            now = datetime.now(tz=_LONDON).strftime("%H:%M:%S")
            status_line = f"[{now}] Connection: {metrics.status} | Ticks: {metrics.ticks_received}"
            logger.info(status_line)
        
        # Sleep in small increments to respond quickly to stop_event
        for _ in range(10):
            if stop_event.is_set():
                break
            time.sleep(0.5)


def _connect_once(ig_service: IGService, acc_number: str, archive: TickArchive | None,
                  stop_event: threading.Event,
                  publisher: ZmqPublisher | None = None,
                  metrics: StreamMetrics | None = None) -> str:
    """Create one Lightstreamer session and block until disconnected or stopped.

    Returns:
        "stopped"    – caller should shut down cleanly.
        "disconnect" – caller should reconnect after a backoff delay.
    """
    disconnect_event = threading.Event()

    ig_stream_service = IGStreamService(ig_service)
    ig_stream_service.create_session()

    # PRICE subscription
    price_subscription = Subscription(
        mode="MERGE",
        items=[f"PRICE:{acc_number}:{epic}" for epic in epics],
        fields=[
            "TIMESTAMP",
            "BIDPRICE1",
            "ASKPRICE1",
            "NET_CHG",
            "DLG_FLAG",
            "HIGH",
            "LOW",
        ],
    )
    price_subscription.setDataAdapter("Pricing")
    price_subscription.addListener(PriceListener(archive=archive, publisher=publisher, metrics=metrics))
    ig_stream_service.subscribe(price_subscription)

    # ACCOUNT subscription
    account_subscription = Subscription(
        mode="MERGE",
        items=[f"ACCOUNT:{acc_number}"],
        fields=["FUNDS", "MARGIN", "AVAILABLE_TO_DEAL", "PNL", "EQUITY", "EQUITY_USED"],
    )
    account_subscription.addListener(AccountListener(publisher=publisher))
    ig_stream_service.subscribe(account_subscription)

    # TRADE subscription
    trade_subscription = Subscription(
        mode="DISTINCT",
        items=[f"TRADE:{acc_number}"],
        fields=["CONFIRMS", "OPU", "WOU"],
    )
    trade_subscription.addListener(TradeListener(publisher=publisher))
    ig_stream_service.subscribe(trade_subscription)

    # Status listener – signals disconnect_event on terminal status
    ig_stream_service.add_client_listener(StatusListener(disconnect_event, metrics=metrics))

    # Block until the session drops or the user requests a stop
    while not stop_event.is_set() and not disconnect_event.is_set():
        time.sleep(0.5)

    ig_stream_service.disconnect()

    if stop_event.is_set():
        return "stopped"
    return "disconnect"


def ig_stream():
    from trading_ig.config import config

    args = parse_args()
    archive_enabled = not args.no_archive

    archive: TickArchive | None = None
    if archive_enabled:
        db_path = default_db_path()
        archive = TickArchive(db_path)
        logger.info(f"Data archival enabled → {db_path}")
    else:
        logger.info("Data archival is disabled")

    metrics = StreamMetrics()
    publisher = ZmqPublisher(
        f"tcp://*:{_ZMQ_PORT_LIVE if config.acc_type.upper() == 'LIVE' else _ZMQ_PORT_DEMO}"
    )

    ig_service = IGService(
        config.username,
        config.password,
        config.api_key,
        config.acc_type,
        acc_number=config.acc_number,
    )

    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        logger.info(f"Signal {signum} received – shutting down…")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Start status reporter thread
    reporter_thread = threading.Thread(target=_status_reporter, args=(metrics, stop_event), daemon=True)
    reporter_thread.start()

    backoff = _BACKOFF_INITIAL
    while not stop_event.is_set():
        wait_if_blackout()
        if stop_event.is_set():
            break

        try:
            result = _connect_once(ig_service, config.acc_number, archive, stop_event,
                                   publisher=publisher, metrics=metrics)
            # Session was established; reset backoff so the next reconnect starts fresh
            backoff = _BACKOFF_INITIAL
        except Exception as exc:
            logger.error(f"Connection error: {exc}")
            result = "disconnect"

        if result == "stopped":
            break

        logger.info(f"Disconnected – retrying in {backoff}s…")
        for _ in range(int(backoff * 2)):
            if stop_event.is_set():
                break
            time.sleep(0.5)
        backoff = next_backoff(backoff)

    if archive is not None:
        archive.close()
    publisher.close()
    logger.info("ig-stream-py stopped.")


class PriceListener(SubscriptionListener):
    def __init__(self, archive: TickArchive | None = None,
                 publisher: ZmqPublisher | None = None,
                 metrics: StreamMetrics | None = None):
        self._archive = archive
        self._publisher = publisher
        self._metrics = metrics

    def onItemUpdate(self, update: ItemUpdate):
        ts_raw = update.getValue("TIMESTAMP")
        bid_raw = update.getValue("BIDPRICE1")
        offer_raw = update.getValue("ASKPRICE1")
        dlg_raw = update.getValue("DLG_FLAG")

        # Increment tick counter
        if self._metrics is not None:
            with self._metrics.lock:
                self._metrics.ticks_received += 1

        # logger.info(
        #     f"{datetime.fromtimestamp(int(ts_raw) / 1000).strftime('%Y-%m-%d %H:%M:%S')} "
        #     f"{update.getItemName()} "
        #     f"Bid: {bid_raw}, "
        #     f"Offer: {offer_raw}, "
        #     f"Price change: {update.getValue('NET_CHG')}, "
        #     f"State: {dlg_raw.strip() if dlg_raw else None}, "
        #     f"Change: {update.getValue('NET_CHG_')}%, "
        #     f"High: {update.getValue('HIGH')}, "
        #     f"Low: {update.getValue('LOW')}"
        # )

        if self._publisher is not None:
            self._publisher.publish("prices", {
                "item": update.getItemName(),
                "TIMESTAMP": ts_raw,
                "BIDPRICE1": bid_raw,
                "ASKPRICE1": offer_raw,
                "NET_CHG": update.getValue("NET_CHG"),
                "DLG_FLAG": dlg_raw,
                "HIGH": update.getValue("HIGH"),
                "LOW": update.getValue("LOW"),
            })

        if self._archive is not None and None not in (ts_raw, bid_raw, offer_raw, dlg_raw):
            # Extract plain epic from "PRICE:{acc}:{epic}"
            parts = update.getItemName().split(":", 2)
            symbol = parts[2] if len(parts) == 3 else update.getItemName()
            self._archive.insert(
                timestamp=int(ts_raw) / 1000,
                symbol=symbol,
                bid=float(bid_raw),
                offer=float(offer_raw),
                state=dlg_raw.strip(),
            )

    def onSubscription(self):
        logger.info("PriceListener onSubscription()")

    def onSubscriptionError(self, code, message):
        logger.info(f"PriceListener onSubscriptionError(): '{code}' {message}")

    def onUnsubscription(self):
        logger.info("PriceListener onUnsubscription()")


class AccountListener(SubscriptionListener):
    def __init__(self, publisher: ZmqPublisher | None = None):
        self._publisher = publisher

    def onItemUpdate(self, update: ItemUpdate):
        logger.info(
            f"{update.getItemName()} "
            f"Funds: {update.getValue('FUNDS')}, "
            f"Margin: {update.getValue('MARGIN')}, "
            f"Available: {update.getValue('AVAILABLE_TO_DEAL')}, "
            f"P&L: {update.getValue('PNL')}, "
            f"Equity: {update.getValue('EQUITY')}, "
            f"Equity used: {update.getValue('EQUITY_USED')}%"
        )

        if self._publisher is not None:
            self._publisher.publish("account", {
                "item": update.getItemName(),
                "FUNDS": update.getValue("FUNDS"),
                "MARGIN": update.getValue("MARGIN"),
                "AVAILABLE_TO_DEAL": update.getValue("AVAILABLE_TO_DEAL"),
                "PNL": update.getValue("PNL"),
                "EQUITY": update.getValue("EQUITY"),
                "EQUITY_USED": update.getValue("EQUITY_USED"),
            })

    def onSubscription(self):
        logger.info("AccountListener onSubscription()")

    def onSubscriptionError(self, code, message):
        logger.info(f"AccountListener onSubscriptionError(): '{code}' {message}")

    def onUnsubscription(self):
        logger.info("AccountListener onUnsubscription()")


class TradeListener(SubscriptionListener):
    def __init__(self, publisher: ZmqPublisher | None = None):
        self._publisher = publisher

    def onItemUpdate(self, update: ItemUpdate):
        logger.info(
            f"{update.getItemName()} "
            f"Confirms: {update.getValue('CONFIRMS')}, "
            f"Open position updates: {update.getValue('OPU')}, "
            f"Working order updates: {update.getValue('WOU')}, "
        )

        if self._publisher is not None:
            self._publisher.publish("trades", {
                "item": update.getItemName(),
                "CONFIRMS": update.getValue("CONFIRMS"),
                "OPU": update.getValue("OPU"),
                "WOU": update.getValue("WOU"),
            })

    def onSubscription(self):
        logger.info("TradeListener onSubscription()")

    def onSubscriptionError(self, code, message):
        logger.info(f"TradeListener onSubscriptionError(): '{code}' {message}")

    def onUnsubscription(self):
        logger.info("TradeListener onUnsubscription()")


class StatusListener(ClientListener):
    # Statuses that mean the server closed the connection and we must reconnect
    _TERMINAL_STATUSES = frozenset({
        "DISCONNECTED",
        "DISCONNECTED:WILL-RETRY",
        "DISCONNECTED:TRYING-RECOVERY",
    })

    def __init__(self, disconnect_event: threading.Event, metrics: StreamMetrics | None = None):
        self._disconnect_event = disconnect_event
        self._metrics = metrics

    def onStatusChange(self, status: str):
        logger.info(f"{datetime.now()}: ***** {status} *****")
        
        # Update metrics
        if self._metrics is not None:
            with self._metrics.lock:
                self._metrics.status = status
        
        if status in self._TERMINAL_STATUSES:
            self._disconnect_event.set()


if __name__ == "__main__":
    ig_stream()