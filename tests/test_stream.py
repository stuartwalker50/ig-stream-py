import pytest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from stream import parse_args, next_backoff, wait_if_blackout, _BACKOFF_MAX

_LONDON = ZoneInfo("Europe/London")


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
