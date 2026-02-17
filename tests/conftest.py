"""Shared test fixtures for Unqork Audit Logs tests."""

from __future__ import annotations

import gzip
import json
import tempfile
from pathlib import Path

import pytest

from unqork_audit_logs.cache import LogCache
from unqork_audit_logs.models import AuditLogEntry
from unqork_audit_logs.parser import ParsedEntry


def make_entry_dict(
    index: int = 0,
    category: str = "user-access",
    action: str = "designer-user-login",
    actor_email: str = "test@test.com",
    outcome: str = "success",
    ip: str = "10.0.0.1",
) -> dict:
    """Create a sample audit log entry dict."""
    return {
        "date": f"2025-02-17T{index:02d}:30:00.000000Z",
        "messageType": "system-event",
        "schemaVersion": "1.0",
        "timestamp": f"2025-02-17T{index:02d}:30:00.{index:03d}Z",
        "eventType": "designer-action",
        "category": category,
        "action": action,
        "source": "designer-api",
        "tags": {},
        "object": {
            "type": "session",
            "identifier": {"type": "name", "value": f"item-{index}"},
            "attributes": {"detail": f"test attribute {index}"},
            "outcome": {"type": outcome},
            "actor": {
                "type": "user",
                "identifier": {"type": "user-id", "value": actor_email},
                "attributes": {},
            },
            "context": {
                "environment": "test-env",
                "sessionId": f"sess-{index:04d}",
                "clientIp": ip,
                "protocol": "https",
                "host": "test.unqork.io",
                "userAgent": "TestAgent/1.0",
            },
        },
    }


def make_parsed_entry(index: int = 0, **kwargs) -> ParsedEntry:
    """Create a sample ParsedEntry (model + original raw JSON)."""
    raw_dict = make_entry_dict(index, **kwargs)
    entry = AuditLogEntry.model_validate(raw_dict)
    raw_json = json.dumps(raw_dict, separators=(",", ":"))
    return ParsedEntry(entry=entry, raw_json=raw_json)


def make_entry(index: int = 0, **kwargs) -> AuditLogEntry:
    """Create a sample AuditLogEntry model."""
    return AuditLogEntry.model_validate(make_entry_dict(index, **kwargs))


def make_compressed_ndjson(entries: list[dict]) -> bytes:
    """Create gzip-compressed NDJSON from a list of entry dicts."""
    ndjson = "\n".join(json.dumps(e) for e in entries)
    return gzip.compress(ndjson.encode("utf-8"))


@pytest.fixture
def sample_parsed_entries() -> list[ParsedEntry]:
    """Return a list of 5 diverse ParsedEntry objects."""
    return [
        make_parsed_entry(0, category="user-access", action="designer-user-login", actor_email="alice@co.com", outcome="success"),
        make_parsed_entry(1, category="access-management", action="delete-designer-role", actor_email="bob@co.com", outcome="success"),
        make_parsed_entry(2, category="configuration", action="save-module-update", actor_email="alice@co.com", outcome="failure"),
        make_parsed_entry(3, category="data-access", action="get-module-submissions", actor_email="charlie@co.com", outcome="success"),
        make_parsed_entry(4, category="user-access", action="designer-user-logout", actor_email="alice@co.com", outcome="success"),
    ]


@pytest.fixture
def sample_entries() -> list[AuditLogEntry]:
    """Return a list of 5 diverse sample AuditLogEntry models."""
    return [
        make_entry(0, category="user-access", action="designer-user-login", actor_email="alice@co.com", outcome="success"),
        make_entry(1, category="access-management", action="delete-designer-role", actor_email="bob@co.com", outcome="success"),
        make_entry(2, category="configuration", action="save-module-update", actor_email="alice@co.com", outcome="failure"),
        make_entry(3, category="data-access", action="get-module-submissions", actor_email="charlie@co.com", outcome="success"),
        make_entry(4, category="user-access", action="designer-user-logout", actor_email="alice@co.com", outcome="success"),
    ]


@pytest.fixture
def sample_entry_dicts() -> list[dict]:
    """Return a list of 5 diverse sample entry dicts."""
    return [
        make_entry_dict(0, category="user-access", action="designer-user-login", actor_email="alice@co.com"),
        make_entry_dict(1, category="access-management", action="delete-designer-role", actor_email="bob@co.com"),
        make_entry_dict(2, category="configuration", action="save-module-update", actor_email="alice@co.com", outcome="failure"),
        make_entry_dict(3, category="data-access", action="get-module-submissions", actor_email="charlie@co.com"),
        make_entry_dict(4, category="user-access", action="designer-user-logout", actor_email="alice@co.com"),
    ]


@pytest.fixture
def tmp_cache(sample_parsed_entries) -> LogCache:
    """Create a temporary cache populated with sample entries."""
    db_path = Path(tempfile.mktemp(suffix=".db"))
    cache = LogCache(db_path)
    cache.store_window(
        "2025-02-17T00:00:00.000Z",
        "2025-02-17T01:00:00.000Z",
        sample_parsed_entries[:3],
        2,
    )
    cache.store_window(
        "2025-02-17T01:00:00.000Z",
        "2025-02-17T02:00:00.000Z",
        sample_parsed_entries[3:],
        1,
    )
    yield cache
    cache.close()
    db_path.unlink(missing_ok=True)
