import sys
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from stream import parse_args, next_backoff, wait_if_blackout, _BACKOFF_MAX, _BACKOFF_INITIAL
import stream as stream_mod

_LONDON = ZoneInfo("Europe/London")


def _make_ig_stream_patches(connect_side_effect):
    """Return a context-manager stack that stubs out all I/O in ig_stream()."""
    fake_config = MagicMock()
    fake_config.acc_number = "ACC001"
    sys.modules["trading_ig.config"].config = fake_config

    return [
        patch("stream._connect_once", side_effect=connect_side_effect),
        patch("stream.wait_if_blackout"),
        patch("stream.IGService"),
        patch("stream.parse_args", return_value=MagicMock(no_archive=True)),
        patch("stream.time.sleep"),
    ]


class TestParseArgs:
    def test_no_archive_default_false(self):
        args = parse_args([])
        assert args.no_archive is False

    def test_no_archive_flag(self):
        args = parse_args(["--no-archive"])
        assert args.no_archive is True


class TestNextBackoff:
    def test_doubles(self):
        assert next_backoff(2) == 4
        assert next_backoff(4) == 8

    def test_capped_at_max(self):
        assert next_backoff(_BACKOFF_MAX) == _BACKOFF_MAX

    def test_caps_before_doubling_over_max(self):
        assert next_backoff(_BACKOFF_MAX / 2 + 1) == _BACKOFF_MAX

    def test_custom_cap(self):
        assert next_backoff(64, cap=100) == 100
        assert next_backoff(32, cap=100) == 64


class TestWaitIfBlackout:
    def _make_london_dt(self, hour, minute=30, second=0):
        return datetime(2026, 5, 21, hour, minute, second, tzinfo=_LONDON)

    def test_inside_blackout_sleeps(self):
        now = self._make_london_dt(22, 15, 0)
        expected_delay = (now.replace(hour=23, minute=0, second=0, microsecond=0) - now).total_seconds()
        with patch("stream.time.sleep") as mock_sleep:
            wait_if_blackout(now=now)
        mock_sleep.assert_called_once()
        assert abs(mock_sleep.call_args[0][0] - expected_delay) < 1

    def test_outside_blackout_no_sleep(self):
        for hour in [0, 21, 23]:
            now = self._make_london_dt(hour)
            with patch("stream.time.sleep") as mock_sleep:
                wait_if_blackout(now=now)
            mock_sleep.assert_not_called()

    def test_blackout_boundary_hour_22_exactly(self):
        now = self._make_london_dt(22, 0, 0)
        with patch("stream.time.sleep") as mock_sleep:
            wait_if_blackout(now=now)
        mock_sleep.assert_called_once()


class TestBackoffReset:
    """Verify that ig_stream() resets the backoff after a successful connection."""

    def _run(self, connect_side_effect):
        """Run ig_stream() with the given _connect_once side-effect; return sleep call args."""
        sleep_calls = []

        def _record_sleep(s):
            sleep_calls.append(s)

        patches = _make_ig_stream_patches(connect_side_effect)
        patches.append(patch("stream.time.sleep", side_effect=_record_sleep))

        from contextlib import ExitStack
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stream_mod.ig_stream()

        return sleep_calls

    def test_backoff_resets_after_successful_connection(self):
        """After a session establishes and drops, backoff must restart from _BACKOFF_INITIAL."""
        call_count = [0]

        def fake_connect(ig_service, acc_number, archive, stop_event):
            call_count[0] += 1
            if call_count[0] >= 3:
                stop_event.set()
                return "stopped"
            return "disconnect"

        sleep_calls = self._run(fake_connect)

        # With reset: each disconnect uses backoff=_BACKOFF_INITIAL (2).
        # int(2 * 2) = 4 sleep(0.5) calls per disconnect × 2 disconnects = 8.
        # Without reset: second disconnect would use backoff=4 → 8 more → total 12.
        half_second_calls = [s for s in sleep_calls if s == 0.5]
        assert len(half_second_calls) == int(_BACKOFF_INITIAL * 2) * 2

    def test_backoff_grows_after_failed_connection(self):
        """When _connect_once raises, backoff must NOT reset – it should keep growing."""
        call_count = [0]

        def fake_connect(ig_service, acc_number, archive, stop_event):
            call_count[0] += 1
            if call_count[0] >= 3:
                stop_event.set()
                return "stopped"
            raise ConnectionError("simulated failure")

        sleep_calls = self._run(fake_connect)

        # Fail 1: backoff=2 → 4 × 0.5s sleeps; backoff grows to 4.
        # Fail 2: backoff=4 → 8 × 0.5s sleeps.  Total = 12.
        half_second_calls = [s for s in sleep_calls if s == 0.5]
        assert len(half_second_calls) == int(_BACKOFF_INITIAL * 2) + int(next_backoff(_BACKOFF_INITIAL) * 2)

