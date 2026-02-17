"""Tests for the fetch orchestrator."""

from datetime import datetime, timezone

from unqork_audit_logs.fetcher import (
    generate_windows,
    parse_datetime_input,
    parse_relative_time,
    DATETIME_FORMAT,
)


class TestGenerateWindows:
    def test_exact_hours(self):
        start = datetime(2025, 2, 17, 9, 0, tzinfo=timezone.utc)
        end = datetime(2025, 2, 17, 12, 0, tzinfo=timezone.utc)
        windows = generate_windows(start, end)
        assert len(windows) == 3
        assert windows[0] == ("2025-02-17T09:00:00.000Z", "2025-02-17T10:00:00.000Z")
        assert windows[1] == ("2025-02-17T10:00:00.000Z", "2025-02-17T11:00:00.000Z")
        assert windows[2] == ("2025-02-17T11:00:00.000Z", "2025-02-17T12:00:00.000Z")

    def test_partial_last_window(self):
        start = datetime(2025, 2, 17, 9, 0, tzinfo=timezone.utc)
        end = datetime(2025, 2, 17, 10, 30, tzinfo=timezone.utc)
        windows = generate_windows(start, end)
        assert len(windows) == 2
        assert windows[1] == ("2025-02-17T10:00:00.000Z", "2025-02-17T10:30:00.000Z")

    def test_single_hour(self):
        start = datetime(2025, 2, 17, 9, 0, tzinfo=timezone.utc)
        end = datetime(2025, 2, 17, 10, 0, tzinfo=timezone.utc)
        windows = generate_windows(start, end)
        assert len(windows) == 1

    def test_24_hours(self):
        start = datetime(2025, 2, 17, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 2, 18, 0, 0, tzinfo=timezone.utc)
        windows = generate_windows(start, end)
        assert len(windows) == 24

    def test_same_time(self):
        start = datetime(2025, 2, 17, 9, 0, tzinfo=timezone.utc)
        windows = generate_windows(start, start)
        assert len(windows) == 0


class TestParseDatetimeInput:
    def test_iso_with_z(self):
        dt = parse_datetime_input("2025-02-17T09:00:00.000Z")
        assert dt.year == 2025
        assert dt.month == 2
        assert dt.hour == 9
        assert dt.tzinfo is not None

    def test_date_and_time(self):
        dt = parse_datetime_input("2025-02-17 09:00")
        assert dt.hour == 9
        assert dt.minute == 0

    def test_date_only(self):
        dt = parse_datetime_input("2025-02-17")
        assert dt.hour == 0
        assert dt.minute == 0

    def test_date_time_seconds(self):
        dt = parse_datetime_input("2025-02-17 09:30:45")
        assert dt.hour == 9
        assert dt.minute == 30
        assert dt.second == 45

    def test_invalid_raises(self):
        import pytest
        with pytest.raises(ValueError, match="Cannot parse datetime"):
            parse_datetime_input("not-a-date")


class TestParseRelativeTime:
    def test_hours(self):
        start, end = parse_relative_time("24h")
        diff = end - start
        assert abs(diff.total_seconds() - 86400) < 1

    def test_days(self):
        start, end = parse_relative_time("7d")
        diff = end - start
        assert abs(diff.total_seconds() - 604800) < 1

    def test_minutes(self):
        start, end = parse_relative_time("30m")
        diff = end - start
        assert abs(diff.total_seconds() - 1800) < 1

    def test_invalid(self):
        import pytest
        with pytest.raises(ValueError, match="Cannot parse relative time"):
            parse_relative_time("24x")
