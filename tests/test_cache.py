"""Tests for the SQLite cache."""

from unqork_audit_logs.cache import LogCache


class TestLogCache:
    def test_store_and_query(self, tmp_cache):
        entries = tmp_cache.query_entries()
        assert len(entries) == 5

    def test_window_tracking(self, tmp_cache):
        assert tmp_cache.is_window_fetched(
            "2025-02-17T00:00:00.000Z", "2025-02-17T01:00:00.000Z"
        )
        assert not tmp_cache.is_window_fetched(
            "2025-02-17T05:00:00.000Z", "2025-02-17T06:00:00.000Z"
        )

    def test_query_by_category(self, tmp_cache):
        results = tmp_cache.query_entries(category="user-access")
        assert len(results) == 2
        for r in results:
            assert "user-access" in r["category"]

    def test_query_by_action(self, tmp_cache):
        results = tmp_cache.query_entries(action="login")
        assert len(results) == 1
        assert "login" in results[0]["action"]

    def test_query_by_actor(self, tmp_cache):
        results = tmp_cache.query_entries(actor="alice")
        assert len(results) == 3

    def test_query_by_outcome(self, tmp_cache):
        results = tmp_cache.query_entries(outcome="failure")
        assert len(results) == 1
        assert results[0]["outcome_type"] == "failure"

    def test_query_with_search(self, tmp_cache):
        results = tmp_cache.query_entries(search="delete")
        assert len(results) == 1
        assert "delete" in results[0]["action"]

    def test_query_with_limit(self, tmp_cache):
        results = tmp_cache.query_entries(limit=2)
        assert len(results) == 2

    def test_count_entries(self, tmp_cache):
        total = tmp_cache.count_entries()
        assert total == 5
        failures = tmp_cache.count_entries(outcome="failure")
        assert failures == 1

    def test_get_entry_by_id(self, tmp_cache):
        all_entries = tmp_cache.query_entries()
        first_id = all_entries[0]["id"]
        entry = tmp_cache.get_entry_by_id(first_id)
        assert entry is not None
        assert entry["id"] == first_id

    def test_get_entry_by_id_not_found(self, tmp_cache):
        assert tmp_cache.get_entry_by_id("nonexistent") is None

    def test_cache_stats(self, tmp_cache):
        stats = tmp_cache.get_cache_stats()
        assert stats["total_entries"] == 5
        assert stats["total_windows"] == 2
        assert "user-access" in stats["categories"]

    def test_clear(self, tmp_cache):
        tmp_cache.clear()
        assert tmp_cache.count_entries() == 0
        assert not tmp_cache.is_window_fetched(
            "2025-02-17T00:00:00.000Z", "2025-02-17T01:00:00.000Z"
        )

    def test_dedup_on_reinsert(self, tmp_cache, sample_parsed_entries):
        """Re-storing the same entries should not create duplicates."""
        initial_count = tmp_cache.count_entries()
        new_count = tmp_cache.store_window(
            "2025-02-17T00:00:00.000Z",
            "2025-02-17T01:00:00.000Z",
            sample_parsed_entries[:3],
            2,
        )
        assert new_count == 0  # All already exist
        assert tmp_cache.count_entries() == initial_count

    def test_fetched_windows(self, tmp_cache):
        windows = tmp_cache.get_fetched_windows()
        assert len(windows) == 2
        assert windows[0]["window_start"] == "2025-02-17T00:00:00.000Z"
