import pytest

from stream import parse_args


class TestParseArgs:
    def test_no_archive_default_false(self):
        args = parse_args([])
        assert args.no_archive is False

    def test_no_archive_flag(self):
        args = parse_args(["--no-archive"])
        assert args.no_archive is True