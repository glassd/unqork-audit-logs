"""Filter engine for querying cached audit log entries.

Provides a unified FilterParams object that can be passed to cache.query_entries()
and used by the CLI for consistent filter handling across commands.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from unqork_audit_logs.fetcher import parse_datetime_input, parse_relative_time


@dataclass
class FilterParams:
    """Parameters for filtering audit log entries."""

    start: str | None = None
    end: str | None = None
    category: str | None = None
    action: str | None = None
    actor: str | None = None
    outcome: str | None = None
    source: str | None = None
    ip: str | None = None
    search: str | None = None
    limit: int = 100
    offset: int = 0

    def has_filters(self) -> bool:
        """Check if any filters are active."""
        return any([
            self.start, self.end, self.category, self.action,
            self.actor, self.outcome, self.source, self.ip, self.search,
        ])

    def as_query_kwargs(self) -> dict:
        """Convert to kwargs for LogCache.query_entries()."""
        return {
            "start": self.start,
            "end": self.end,
            "category": self.category,
            "action": self.action,
            "actor": self.actor,
            "outcome": self.outcome,
            "source": self.source,
            "ip": self.ip,
            "search": self.search,
            "limit": self.limit,
            "offset": self.offset,
        }

    def as_count_kwargs(self) -> dict:
        """Convert to kwargs for LogCache.count_entries()."""
        return {
            "start": self.start,
            "end": self.end,
            "category": self.category,
            "action": self.action,
            "actor": self.actor,
            "outcome": self.outcome,
        }


def build_filters(
    start: str | None = None,
    end: str | None = None,
    last: str | None = None,
    category: str | None = None,
    action: str | None = None,
    actor: str | None = None,
    outcome: str | None = None,
    source: str | None = None,
    ip: str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> FilterParams:
    """Build a FilterParams from CLI arguments.

    Handles --last as a shorthand for --start/--end relative to now.

    Args:
        start: Start datetime string or None.
        end: End datetime string or None.
        last: Relative time like '24h', '7d', '30m' or None.
        category: Filter by category.
        action: Filter by action.
        actor: Filter by actor ID/email.
        outcome: Filter by outcome (success/failure).
        source: Filter by source.
        ip: Filter by client IP.
        search: Free-text search term.
        limit: Max results to return.
        offset: Offset for pagination.

    Returns:
        A FilterParams ready for querying.
    """
    start_str = None
    end_str = None

    if last:
        s, e = parse_relative_time(last)
        start_str = s.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = e.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    else:
        if start:
            dt = parse_datetime_input(start)
            start_str = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if end:
            dt = parse_datetime_input(end)
            end_str = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    return FilterParams(
        start=start_str,
        end=end_str,
        category=category,
        action=action,
        actor=actor,
        outcome=outcome,
        source=source,
        ip=ip,
        search=search,
        limit=limit,
        offset=offset,
    )
