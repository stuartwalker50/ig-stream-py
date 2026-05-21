"""ZeroMQ subscriber – display live messages from ig-stream-py.

Usage::

    source .env.demo && uv run recv_zmq.py --prices
    source .env.live && uv run recv_zmq.py --account
    source .env.demo && uv run recv_zmq.py --trades
"""

import argparse
import json
import os
import sys

import zmq

# Port numbers must match the constants in stream.py.
_ZMQ_PORT_DEMO = 5555
_ZMQ_PORT_LIVE = 5556


def parse_args(args=None):
    """Parse command-line arguments.

    Args:
        args: List of argument strings (defaults to sys.argv[1:]). Used for testing.

    Returns:
        Namespace with parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Receive and display live messages published by ig-stream-py over ZeroMQ."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--prices", action="store_true", help="Subscribe to price updates")
    group.add_argument("--account", action="store_true", help="Subscribe to account updates")
    group.add_argument("--trades", action="store_true", help="Subscribe to trade updates")
    return parser.parse_args(args)


def recv_zmq():
    args = parse_args()

    acc_type = os.environ.get("IG_SERVICE_ACC_TYPE", "DEMO").upper()
    port = _ZMQ_PORT_LIVE if acc_type == "LIVE" else _ZMQ_PORT_DEMO

    if args.prices:
        topic = "prices"
    elif args.account:
        topic = "account"
    else:
        topic = "trades"

    print(f"{acc_type} | Subscribing to '{topic}' on tcp://localhost:{port}")
    sys.stdout.flush()

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://localhost:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, topic.encode())

    try:
        while True:
            _topic, payload = socket.recv_multipart()
            data = json.loads(payload)
            print(json.dumps(data, indent=2))
            sys.stdout.flush()
    except KeyboardInterrupt:
        pass
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    recv_zmq()
