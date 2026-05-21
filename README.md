# ig-stream-py

A service that connects to IG Markets via the `trading_ig` library, streaming market data (prices, account info, trades) over Lightstreamer and executing orders.

## Usage

```bash
# Run (account mode is read from the environment variables sourced first)
source .env.demo && python3 stream.py
 OR
source .env.live && python3 stream.py

# Run without data archival (feature planned)
python stream.py --no-archive

# See all options
python stream.py --help
```

### Options

| Flag | Description |
| --- | --- |
| `--no-archive` | Disable data archival to disk (currently a placeholder) |
| `--help` | Show help message and exit |

## Config

## Development

```bash
# Run tests
uv run pytest

# Run with verbose output
uv run pytest -v
```