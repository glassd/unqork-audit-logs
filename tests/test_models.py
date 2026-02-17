"""Tests for Pydantic audit log models."""

from unqork_audit_logs.models import AuditLogEntry
from tests.conftest import make_entry_dict


class TestAuditLogEntry:
    def test_parse_from_dict(self):
        raw = make_entry_dict(0)
        entry = AuditLogEntry.model_validate(raw)
        assert entry.category == "user-access"
        assert entry.action == "designer-user-login"
        assert entry.source == "designer-api"

    def test_convenience_properties(self):
        entry = AuditLogEntry.model_validate(
            make_entry_dict(0, actor_email="test@example.com", ip="1.2.3.4")
        )
        assert entry.actor_id == "test@example.com"
        assert entry.outcome_type == "success"
        assert entry.environment == "test-env"
        assert entry.client_ip == "1.2.3.4"
        assert entry.host == "test.unqork.io"
        assert entry.session_id == "sess-0000"

    def test_parsed_timestamp(self):
        entry = AuditLogEntry.model_validate(make_entry_dict(5))
        ts = entry.parsed_timestamp
        assert ts.year == 2025
        assert ts.month == 2
        assert ts.day == 17
        assert ts.hour == 5
        assert ts.minute == 30

    def test_empty_entry(self):
        """Model should handle minimal/empty data gracefully."""
        entry = AuditLogEntry.model_validate({})
        assert entry.category == ""
        assert entry.action == ""
        assert entry.actor_id == ""
        assert entry.outcome_type == ""

    def test_alias_fields(self):
        raw = make_entry_dict(0)
        entry = AuditLogEntry.model_validate(raw)
        assert entry.message_type == "system-event"
        assert entry.schema_version == "1.0"
        assert entry.event_type == "designer-action"

    def test_model_dump_by_alias(self):
        raw = make_entry_dict(0)
        entry = AuditLogEntry.model_validate(raw)
        dumped = entry.model_dump(by_alias=True)
        assert "messageType" in dumped
        assert "schemaVersion" in dumped
        assert "eventType" in dumped
