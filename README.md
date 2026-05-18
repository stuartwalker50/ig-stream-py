# ig-stream-py

A Python service that connects to IG Markets via `trading_ig`, normalises market data, publishes `PRICE:{account}:{epic}` topics over ZeroMQ, executes market orders, and stores live prices in SQLite for backtesting.

## Implemented service behavior

- SQLite store creates a new `prices_YYYYMMDD_HHMMSS.db` file per run and writes each price to a `prices` table.
- 22:00 UTC nightly guard pauses order execution, reconnects the stream, and resumes orders at 23:00 UTC.
- Orders received in the pause window are rejected and logged as warnings.
- Stream reconnect and LOOP rebind use exponential backoff (`2s, 4s, 8s, ...`) up to `MAX_RETRY_DELAY`, with capped retries via `MAX_RECONNECT_ATTEMPTS`.
- In-place status bar reports current time plus latest price/confirm/account update summaries.

## Local demo

```bash
python ig_stream_service.py --demo-status
```

## Tests

```bash
python -m unittest -q
```
