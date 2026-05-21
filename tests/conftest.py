"""Stub out heavy optional dependencies so pure-logic tests can run without them."""
import sys
import types


def _make_stub(*names):
    for name in names:
        parts = name.split(".")
        # Ensure each prefix is also registered
        for i in range(1, len(parts) + 1):
            mod_name = ".".join(parts[:i])
            if mod_name not in sys.modules:
                sys.modules[mod_name] = types.ModuleType(mod_name)


_make_stub(
    "lightstreamer",
    "lightstreamer.client",
    "trading_ig",
    "trading_ig.config",
    "munch",
    "pandas",
    "zmq",
)

# Minimal attribute stubs so `from x import Y` works
import sys

_ls_client = sys.modules["lightstreamer.client"]
for _cls_name in ("Subscription", "SubscriptionListener", "ItemUpdate", "ClientListener"):
    setattr(_ls_client, _cls_name, type(_cls_name, (), {}))

_tig = sys.modules["trading_ig"]
for _cls_name in ("IGService", "IGStreamService"):
    setattr(_tig, _cls_name, type(_cls_name, (), {}))

# Stub ZMQ socket-type constants used in ZmqPublisher
sys.modules["zmq"].PUB = 1
sys.modules["zmq"].Context = type("Context", (), {})