"""Pydantic models for Unqork audit log entries.

Based on the audit log schema documented at:
https://docs.unqork.io/docs/audit-logs
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class Identifier(BaseModel):
    """Identifier within an audit log object or actor."""

    model_config = {"populate_by_name": True}

    type: str = ""
    value: str = ""


class Outcome(BaseModel):
    """Outcome of the audited event."""

    model_config = {"populate_by_name": True}

    type: str = ""  # "success" or "failure"
    failure_reason: str | None = Field(default=None, alias="failureReason")
    error: str | None = None


class Actor(BaseModel):
    """The user or system that performed the action."""

    model_config = {"populate_by_name": True}

    type: str = ""  # e.g., "user"
    identifier: Identifier = Field(default_factory=Identifier)
    attributes: dict[str, Any] = Field(default_factory=dict)


class Context(BaseModel):
    """Request context for the audit event."""

    model_config = {"populate_by_name": True}

    environment: str = ""
    session_id: str | None = Field(default=None, alias="sessionId")
    client_ip: str | None = Field(default=None, alias="clientIp")
    protocol: str = ""
    host: str = ""
    user_agent: str | None = Field(default=None, alias="userAgent")


class ObjectDetail(BaseModel):
    """Details of the object affected by the audited event."""

    model_config = {"populate_by_name": True}

    type: str = ""
    identifier: Identifier = Field(default_factory=Identifier)
    attributes: dict[str, Any] = Field(default_factory=dict)
    prior_attributes: dict[str, Any] | None = Field(
        default=None, alias="priorAttributes"
    )
    outcome: Outcome = Field(default_factory=Outcome)
    actor: Actor = Field(default_factory=Actor)
    context: Context = Field(default_factory=Context)


class AuditLogEntry(BaseModel):
    """A single audit log entry from the Unqork API.

    Example entry:
    {
        "date": "2022-12-19T19:46:38.000000Z",
        "messageType": "system-event",
        "schemaVersion": "1.0",
        "timestamp": "2022-12-19T19:46:38.338Z",
        "eventType": "designer-action",
        "category": "access-management",
        "action": "delete-designer-role",
        "source": "designer-api",
        "tags": {},
        "object": { ... }
    }
    """

    model_config = {"populate_by_name": True}

    date: str = ""
    message_type: str = Field(default="", alias="messageType")
    schema_version: str = Field(default="", alias="schemaVersion")
    timestamp: str = ""
    event_type: str = Field(default="", alias="eventType")
    category: str = ""
    action: str = ""
    source: str = ""
    tags: dict[str, Any] = Field(default_factory=dict)
    object: ObjectDetail = Field(default_factory=ObjectDetail)

    @property
    def actor_id(self) -> str:
        """Convenience accessor for the actor's identifier value (usually email)."""
        return self.object.actor.identifier.value

    @property
    def outcome_type(self) -> str:
        """Convenience accessor for the outcome type."""
        return self.object.outcome.type

    @property
    def environment(self) -> str:
        """Convenience accessor for the environment."""
        return self.object.context.environment

    @property
    def client_ip(self) -> str | None:
        """Convenience accessor for the client IP."""
        return self.object.context.client_ip

    @property
    def host(self) -> str:
        """Convenience accessor for the host."""
        return self.object.context.host

    @property
    def session_id(self) -> str | None:
        """Convenience accessor for the session ID."""
        return self.object.context.session_id

    @property
    def parsed_timestamp(self) -> datetime:
        """Parse the timestamp string into a datetime object."""
        # Handle multiple timestamp formats from Unqork
        ts = self.timestamp or self.date
        # Remove trailing Z and handle microseconds
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
