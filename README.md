# ig-stream-py

A service that connects to IG Markets via the `trading_ig` library, streaming market data (prices, account info, trades) over Lightstreamer and executing orders.

## Usage

```bash
# Setup
uv sync

# Run (account mode is read from the environment variables sourced first)
source .env.demo && uv run stream.py
 OR
source .env.live && uv run stream.py

# Run without data archival
uv run stream.py --no-archive

# See all options
uv run stream.py --help
```

### Options

| Flag | Description |
| --- | --- |
| `--no-archive` | Disable data archival to disk |
| `--help` | Show help message and exit |

## Receiving live data over ZeroMQ

`ig-stream-py` publishes all Lightstreamer messages on a ZeroMQ PUB socket.
The port is chosen automatically based on the account type:

| Account type | Port |
| --- | --- |
| DEMO | 5555 |
| LIVE | 5556 |

Use `recv_zmq.py` to print messages from any of the three topics to stdout.
Source the same `.env` file as `stream.py` so the receiver connects to the correct port.

```bash
# Prices
source .env.demo && uv run recv_zmq.py --prices

# Account updates
source .env.demo && uv run recv_zmq.py --account

# Trade updates (confirms, open-position updates, working-order updates)
source .env.demo && uv run recv_zmq.py --trades

# LIVE equivalents — connects to port 5556 automatically
source .env.live && uv run recv_zmq.py --prices
```

### recv_zmq.py options

| Flag | Description |
| --- | --- |
| `--prices` | Subscribe to price updates |
| `--account` | Subscribe to account updates |
| `--trades` | Subscribe to trade updates |
| `--help` | Show help message and exit |

## Features

- **SQLite tick archival** – each run creates `./data/raw_price_YYYY-MM-DD_HHMMSS.db` and stores every price tick with timestamp, symbol, bid, offer, and market state.
- **Blackout window** – if started between 22:00–22:59 London time the process sleeps until 23:00 before connecting (avoids the IG daily rollover outage).
- **Automatic reconnection** – on disconnect the process waits with exponential backoff (starting at 2 s, capped at 30 min) then re-establishes the Lightstreamer session transparently.
- **ZeroMQ fanout** – all Lightstreamer messages are re-published on a local ZMQ PUB socket (port 5555 for DEMO, 5556 for LIVE) so any number of downstream consumers can subscribe independently.

## Config

Copy `.env.example` to `.env` and fill in your IG credentials:

```bash
cp .env.example .env.demo   # or .env.live
```

## Development

```bash
# Run tests
uv run pytest

# Run with verbose output
uv run pytest -v
```