"""Tests for served release versioning model (T7-002)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from akshare_data.served.versioning import (
    ReleaseVersion,
    ReleaseVersionError,
    next_release_version,
)


@pytest.mark.unit
@pytest.mark.served
class TestReleaseVersionModel:
    def test_parse_and_to_string(self):
        raw = "market_quote_daily-r202604230930-02"
        parsed = ReleaseVersion.parse(raw)
        assert parsed.dataset == "market_quote_daily"
        assert parsed.sequence == 2
        assert parsed.to_string() == raw

    def test_parse_invalid_format(self):
        with pytest.raises(ReleaseVersionError):
            ReleaseVersion.parse("rv_20260423_abcd")

    def test_next_release_version_increments_sequence_within_same_minute(self):
        now = datetime(2026, 4, 23, 9, 30, tzinfo=timezone.utc)
        versions = [
            "market_quote_daily-r202604230930-01",
            "market_quote_daily-r202604230930-02",
        ]
        nxt = next_release_version("market_quote_daily", now=now, existing_versions=versions)
        assert nxt == "market_quote_daily-r202604230930-03"

    def test_next_release_version_resets_for_new_minute(self):
        now = datetime(2026, 4, 23, 9, 31, tzinfo=timezone.utc)
        versions = ["market_quote_daily-r202604230930-02"]
        nxt = next_release_version("market_quote_daily", now=now, existing_versions=versions)
        assert nxt == "market_quote_daily-r202604230931-01"
