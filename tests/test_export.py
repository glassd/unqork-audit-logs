"""Tests for the export functionality."""

import csv
import json
import tempfile
from io import StringIO

from unqork_audit_logs.export import export_entries, _to_csv, _to_json, _to_jsonl
from tests.conftest import make_entry_dict


def _make_cache_rows():
    """Create sample cache row dicts (as they come from SQLite)."""
    return [
        {
            "id": "abc123",
            "raw_json": json.dumps(make_entry_dict(0, action="designer-user-login")),
            "timestamp": "2025-02-17T00:30:00.000Z",
            "date": "2025-02-17T00:30:00.000000Z",
            "event_type": "designer-action",
            "category": "user-access",
            "action": "designer-user-login",
            "source": "designer-api",
            "outcome_type": "success",
            "actor_type": "user",
            "actor_id": "alice@co.com",
            "environment": "test-env",
            "client_ip": "10.0.0.1",
            "host": "test.unqork.io",
            "session_id": "sess-0000",
            "object_type": "session",
        },
        {
            "id": "def456",
            "raw_json": json.dumps(make_entry_dict(1, action="save-module-update", category="configuration")),
            "timestamp": "2025-02-17T01:30:00.001Z",
            "date": "2025-02-17T01:30:00.000000Z",
            "event_type": "designer-action",
            "category": "configuration",
            "action": "save-module-update",
            "source": "designer-api",
            "outcome_type": "failure",
            "actor_type": "user",
            "actor_id": "bob@co.com",
            "environment": "test-env",
            "client_ip": "10.0.0.2",
            "host": "test.unqork.io",
            "session_id": "sess-0001",
            "object_type": "module",
        },
    ]


class TestToJson:
    def test_valid_output(self):
        rows = _make_cache_rows()
        result = _to_json(rows)
        parsed = json.loads(result)
        assert len(parsed) == 2
        # Should use the raw_json content
        assert parsed[0]["action"] == "designer-user-login"
        assert parsed[1]["action"] == "save-module-update"


class TestToJsonl:
    def test_valid_output(self):
        rows = _make_cache_rows()
        result = _to_jsonl(rows)
        lines = [l for l in result.strip().split("\n") if l]
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["action"] == "designer-user-login"


class TestToCsv:
    def test_valid_output(self):
        rows = _make_cache_rows()
        result = _to_csv(rows)
        reader = csv.DictReader(StringIO(result))
        csv_rows = list(reader)
        assert len(csv_rows) == 2
        assert csv_rows[0]["action"] == "designer-user-login"
        assert csv_rows[0]["actor_id"] == "alice@co.com"
        assert csv_rows[1]["category"] == "configuration"

    def test_empty(self):
        assert _to_csv([]) == ""


class TestExportEntries:
    def test_export_to_file(self):
        rows = _make_cache_rows()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name

        count = export_entries(rows, format="json", output_path=path)
        assert count == 2

        with open(path) as f:
            parsed = json.loads(f.read())
        assert len(parsed) == 2

    def test_json_export_matches_input(self):
        """JSON export should faithfully reproduce the original API data."""
        original_dict = make_entry_dict(0, action="designer-user-login")
        raw_json = json.dumps(original_dict, separators=(",", ":"))
        rows = [{
            "id": "test123",
            "raw_json": raw_json,
            "timestamp": original_dict["timestamp"],
            "category": original_dict["category"],
            "action": original_dict["action"],
        }]
        result = _to_json(rows)
        exported = json.loads(result)
        assert len(exported) == 1
        # The exported entry should match the original - no Pydantic artifacts
        assert exported[0] == original_dict
        # Specifically, no extra null fields that Pydantic would add
        assert "priorAttributes" not in json.dumps(exported[0])
        assert "failureReason" not in json.dumps(exported[0])
