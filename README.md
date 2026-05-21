# ig-stream-py

A service that connects to IG Markets via the `trading_ig` library, streaming market data (prices, account info, trades) over Lightstreamer and executing orders.

## Usage

```bash
# Run (account mode is read from the environment variables sourced first)
source .env.demo && python3 stream.py
 OR
source .env.live && python3 stream.py

# Run without data archival
python stream.py --no-archive

# See all options
python stream.py --help
```

### Options

| Flag | Description |
| --- | --- |
| `--no-archive` | Disable data archival to disk |
| `--help` | Show help message and exit |

## Features

- **SQLite tick archival** – each run creates `./data/raw_price_YYYY-MM-DD_HHMMSS.db` and stores every price tick with timestamp, symbol, bid, offer, and market state.
- **Blackout window** – if started between 22:00–22:59 London time the process sleeps until 23:00 before connecting (avoids the IG daily rollover outage).
- **Automatic reconnection** – on disconnect the process waits with exponential backoff (starting at 2 s, capped at 30 min) then re-establishes the Lightstreamer session transparently.

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