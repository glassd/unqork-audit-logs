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

    def test_nested_models_accept_python_field_names(self):
        """Nested models should parse both alias (camelCase) and Python (snake_case) names.

        This ensures populate_by_name works on all nested models, not just AuditLogEntry.
        """
        raw = {
            "date": "2025-01-01T00:00:00.000000Z",
            "message_type": "system-event",
            "schema_version": "1.0",
            "timestamp": "2025-01-01T00:00:00.000Z",
            "event_type": "designer-action",
            "category": "user-access",
            "action": "login",
            "source": "designer-api",
            "tags": {},
            "object": {
                "type": "session",
                "identifier": {"type": "name", "value": "item-1"},
                "attributes": {},
                "prior_attributes": {"old_key": "old_val"},
                "outcome": {
                    "type": "failure",
                    "failure_reason": "bad credentials",
                },
                "actor": {
                    "type": "user",
                    "identifier": {"type": "user-id", "value": "admin@co.com"},
                    "attributes": {},
                },
                "context": {
                    "environment": "production",
                    "session_id": "sess-9999",
                    "client_ip": "192.168.1.1",
                    "protocol": "https",
                    "host": "prod.unqork.io",
                    "user_agent": "Mozilla/5.0",
                },
            },
        }
        entry = AuditLogEntry.model_validate(raw)
        # Top-level aliased fields
        assert entry.message_type == "system-event"
        assert entry.event_type == "designer-action"
        # Nested Context fields by Python name
        assert entry.client_ip == "192.168.1.1"
        assert entry.session_id == "sess-9999"
        assert entry.object.context.user_agent == "Mozilla/5.0"
        # Nested Outcome fields by Python name
        assert entry.outcome_type == "failure"
        assert entry.object.outcome.failure_reason == "bad credentials"
        # Nested ObjectDetail field by Python name
        assert entry.object.prior_attributes == {"old_key": "old_val"}
        # Actor
        assert entry.actor_id == "admin@co.com"

    def test_nested_models_accept_alias_names(self):
        """Nested models should parse camelCase alias names from the API."""
        raw = {
            "date": "2025-01-01T00:00:00.000000Z",
            "messageType": "system-event",
            "schemaVersion": "1.0",
            "timestamp": "2025-01-01T00:00:00.000Z",
            "eventType": "designer-action",
            "category": "user-access",
            "action": "login",
            "source": "designer-api",
            "tags": {},
            "object": {
                "type": "session",
                "identifier": {"type": "name", "value": "item-1"},
                "attributes": {},
                "priorAttributes": {"old_key": "old_val"},
                "outcome": {
                    "type": "success",
                    "failureReason": None,
                },
                "actor": {
                    "type": "user",
                    "identifier": {"type": "user-id", "value": "user@co.com"},
                    "attributes": {},
                },
                "context": {
                    "environment": "staging",
                    "sessionId": "sess-1234",
                    "clientIp": "10.0.0.5",
                    "protocol": "https",
                    "host": "staging.unqork.io",
                    "userAgent": "curl/7.88",
                },
            },
        }
        entry = AuditLogEntry.model_validate(raw)
        assert entry.client_ip == "10.0.0.5"
        assert entry.session_id == "sess-1234"
        assert entry.object.context.user_agent == "curl/7.88"
        assert entry.outcome_type == "success"
        assert entry.object.prior_attributes == {"old_key": "old_val"}
        assert entry.actor_id == "user@co.com"
