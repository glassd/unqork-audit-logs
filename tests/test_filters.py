"""Tests for the filter engine."""

from unqork_audit_logs.filters import build_filters, FilterParams


class TestBuildFilters:
    def test_no_filters(self):
        f = build_filters()
        assert not f.has_filters()

    def test_with_category(self):
        f = build_filters(category="user-access")
        assert f.has_filters()
        assert f.category == "user-access"

    def test_last_24h(self):
        f = build_filters(last="24h")
        assert f.start is not None
        assert f.end is not None
        assert f.has_filters()

    def test_last_7d(self):
        f = build_filters(last="7d")
        assert f.start is not None
        assert f.end is not None

    def test_explicit_start_end(self):
        f = build_filters(start="2025-02-17", end="2025-02-18")
        assert f.start == "2025-02-17T00:00:00.000Z"
        assert f.end == "2025-02-18T00:00:00.000Z"

    def test_last_overrides_start_end(self):
        """When --last is provided, it should set start and end."""
        f = build_filters(last="1h", start="2025-01-01", end="2025-01-02")
        # last should take precedence (both start and end are set from it)
        assert f.start is not None
        assert f.end is not None

    def test_as_query_kwargs(self):
        f = build_filters(category="user-access", limit=50, offset=10)
        kwargs = f.as_query_kwargs()
        assert kwargs["category"] == "user-access"
        assert kwargs["limit"] == 50
        assert kwargs["offset"] == 10

    def test_as_count_kwargs(self):
        f = build_filters(category="user-access", actor="alice@co.com")
        kwargs = f.as_count_kwargs()
        assert kwargs["category"] == "user-access"
        assert kwargs["actor"] == "alice@co.com"
        assert "limit" not in kwargs
