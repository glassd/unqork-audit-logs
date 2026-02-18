"""Tests for the SQLite cache."""

import json
import tempfile
from pathlib import Path

from unqork_audit_logs.cache import LogCache, _extract_fields, _safe_get
from unqork_audit_logs.models import AuditLogEntry
from unqork_audit_logs.parser import ParsedEntry


# The exact example from https://docs.unqork.io/docs/audit-logs
DOCS_SAMPLE = {
    "date": "2022-12-19T19:46:38.000000Z",
    "messageType": "system-event",
    "schemaVersion": "1.0",
    "timestamp": "2022-12-19T19:46:38.338Z",
    "eventType": "designer-action",
    "category": "access-management",
    "action": "delete-designer-role",
    "source": "designer-api",
    "tags": {},
    "object": {
        "type": "designer-role",
        "identifier": {"type": "name", "value": "Unqork User Name"},
        "attributes": {},
        "outcome": {"type": "success"},
        "actor": {
            "type": "user",
            "identifier": {"type": "user-id", "value": "unqork-user@unqork.com"},
            "attributes": {},
        },
        "context": {
            "environment": "training-staging",
            "sessionId": "8a83187f-40cb-4bd2-a0fc-8dd3987a771a",
            "clientIp": "73.33.37.100",
            "protocol": "https",
            "host": "training.unqork.io",
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        },
    },
}


class TestSafeGet:
    def test_nested_access(self):
        d = {"a": {"b": {"c": "val"}}}
        assert _safe_get(d, "a", "b", "c") == "val"

    def test_missing_key(self):
        d = {"a": {"b": {}}}
        assert _safe_get(d, "a", "b", "c") == ""

    def test_none_value(self):
        d = {"a": None}
        assert _safe_get(d, "a", "b") == ""

    def test_non_dict(self):
        d = {"a": "string"}
        assert _safe_get(d, "a", "b") == ""

    def test_empty_dict(self):
        assert _safe_get({}, "a", "b") == ""


class TestExtractFields:
    def test_docs_sample(self):
        fields = _extract_fields(DOCS_SAMPLE)
        assert fields["category"] == "access-management"
        assert fields["action"] == "delete-designer-role"
        assert fields["source"] == "designer-api"
        assert fields["outcome_type"] == "success"
        assert fields["actor_type"] == "user"
        assert fields["actor_id"] == "unqork-user@unqork.com"
        assert fields["environment"] == "training-staging"
        assert fields["client_ip"] == "73.33.37.100"
        assert fields["host"] == "training.unqork.io"
        assert fields["session_id"] == "8a83187f-40cb-4bd2-a0fc-8dd3987a771a"
        assert fields["object_type"] == "designer-role"
        assert fields["event_type"] == "designer-action"

    def test_empty_dict(self):
        fields = _extract_fields({})
        assert fields["actor_id"] == ""
        assert fields["outcome_type"] == ""
        assert fields["client_ip"] == ""

    def test_missing_object(self):
        raw = {"category": "test", "action": "test-action"}
        fields = _extract_fields(raw)
        assert fields["category"] == "test"
        assert fields["action"] == "test-action"
        assert fields["actor_id"] == ""

    def test_null_object(self):
        raw = {"category": "test", "object": None}
        fields = _extract_fields(raw)
        assert fields["actor_id"] == ""


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

    def test_docs_sample_round_trip(self):
        """The exact example from https://docs.unqork.io/docs/audit-logs
        should store and query back with all nested fields populated."""
        db_path = Path(tempfile.mktemp(suffix=".db"))
        cache = LogCache(db_path)

        entry = AuditLogEntry.model_validate(DOCS_SAMPLE)
        raw_json = json.dumps(DOCS_SAMPLE, separators=(",", ":"))
        parsed = ParsedEntry(entry=entry, raw_json=raw_json)

        cache.store_window(
            "2022-12-19T19:00:00.000Z",
            "2022-12-19T20:00:00.000Z",
            [parsed],
            1,
        )

        rows = cache.query_entries()
        assert len(rows) == 1
        row = rows[0]

        assert row["category"] == "access-management"
        assert row["action"] == "delete-designer-role"
        assert row["actor_id"] == "unqork-user@unqork.com"
        assert row["outcome_type"] == "success"
        assert row["client_ip"] == "73.33.37.100"
        assert row["environment"] == "training-staging"
        assert row["host"] == "training.unqork.io"
        assert row["session_id"] == "8a83187f-40cb-4bd2-a0fc-8dd3987a771a"
        assert row["actor_type"] == "user"
        assert row["object_type"] == "designer-role"
        assert row["event_type"] == "designer-action"
        assert row["source"] == "designer-api"

        cache.close()
        db_path.unlink(missing_ok=True)
