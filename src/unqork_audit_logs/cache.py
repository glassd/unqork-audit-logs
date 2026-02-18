"""SQLite-based local cache for fetched audit log entries.

Stores parsed log entries and tracks which 1-hour windows have been fetched,
enabling incremental fetches and fast local querying.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from unqork_audit_logs.parser import ParsedEntry

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fetched_windows (
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    file_count INTEGER DEFAULT 0,
    entry_count INTEGER DEFAULT 0,
    PRIMARY KEY (window_start, window_end)
);

CREATE TABLE IF NOT EXISTS log_entries (
    id TEXT PRIMARY KEY,
    raw_json TEXT NOT NULL,
    date TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    event_type TEXT DEFAULT '',
    category TEXT DEFAULT '',
    action TEXT DEFAULT '',
    source TEXT DEFAULT '',
    outcome_type TEXT DEFAULT '',
    actor_type TEXT DEFAULT '',
    actor_id TEXT DEFAULT '',
    environment TEXT DEFAULT '',
    client_ip TEXT DEFAULT '',
    host TEXT DEFAULT '',
    session_id TEXT DEFAULT '',
    object_type TEXT DEFAULT '',
    window_start TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_log_date ON log_entries(date);
CREATE INDEX IF NOT EXISTS idx_log_timestamp ON log_entries(timestamp);
CREATE INDEX IF NOT EXISTS idx_log_category ON log_entries(category);
CREATE INDEX IF NOT EXISTS idx_log_action ON log_entries(action);
CREATE INDEX IF NOT EXISTS idx_log_outcome ON log_entries(outcome_type);
CREATE INDEX IF NOT EXISTS idx_log_actor ON log_entries(actor_id);
CREATE INDEX IF NOT EXISTS idx_log_source ON log_entries(source);
CREATE INDEX IF NOT EXISTS idx_log_environment ON log_entries(environment);
CREATE INDEX IF NOT EXISTS idx_log_client_ip ON log_entries(client_ip);
"""


def _entry_id(raw_json: str) -> str:
    """Generate a deterministic ID for a log entry based on its content."""
    return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()[:16]


def _safe_get(d: dict, *keys: str, default: str = "") -> str:
    """Safely traverse nested dicts, returning default if any key is missing."""
    current = d
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return str(current) if current is not None else default


def _extract_fields(raw: dict) -> dict:
    """Extract indexed fields from raw audit log JSON.

    Extracts from the raw dict directly rather than relying on Pydantic model
    traversal, to be resilient to any structural differences between the
    documented schema and actual API responses.

    Based on the documented structure at https://docs.unqork.io/docs/audit-logs:
        object.actor.identifier.value  -> actor_id
        object.actor.type              -> actor_type
        object.outcome.type            -> outcome_type
        object.context.clientIp        -> client_ip
        object.context.environment     -> environment
        object.context.host            -> host
        object.context.sessionId       -> session_id
        object.type                    -> object_type
    """
    obj = raw.get("object", {}) or {}

    return {
        "date": raw.get("date", ""),
        "timestamp": raw.get("timestamp", ""),
        "event_type": raw.get("eventType", raw.get("event_type", "")),
        "category": raw.get("category", ""),
        "action": raw.get("action", ""),
        "source": raw.get("source", ""),
        "outcome_type": _safe_get(obj, "outcome", "type"),
        "actor_type": _safe_get(obj, "actor", "type"),
        "actor_id": _safe_get(obj, "actor", "identifier", "value"),
        "environment": _safe_get(obj, "context", "environment"),
        "client_ip": (
            _safe_get(obj, "context", "clientIp")
            or _safe_get(obj, "context", "client_ip")
        ),
        "host": _safe_get(obj, "context", "host"),
        "session_id": (
            _safe_get(obj, "context", "sessionId")
            or _safe_get(obj, "context", "session_id")
        ),
        "object_type": _safe_get(obj, "type"),
    }


class LogCache:
    """SQLite-backed cache for audit log entries."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(SCHEMA_SQL)
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def is_window_fetched(self, window_start: str, window_end: str) -> bool:
        """Check if a given time window has already been fetched."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT 1 FROM fetched_windows WHERE window_start = ? AND window_end = ?",
            (window_start, window_end),
        ).fetchone()
        return row is not None

    def get_fetched_windows(self) -> list[dict]:
        """Get all fetched windows with their metadata."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM fetched_windows ORDER BY window_start"
        ).fetchall()
        return [dict(row) for row in rows]

    def store_window(
        self,
        window_start: str,
        window_end: str,
        entries: list[ParsedEntry],
        file_count: int,
    ) -> int:
        """Store log entries for a fetched window.

        Args:
            window_start: ISO 8601 UTC window start.
            window_end: ISO 8601 UTC window end.
            entries: Parsed entries (model + original raw JSON) to store.
            file_count: Number of files downloaded for this window.

        Returns:
            Number of new entries inserted (excludes duplicates).
        """
        conn = self._get_conn()
        inserted = 0

        for parsed in entries:
            raw = parsed.raw_json
            entry_id = _entry_id(raw)

            # Extract fields directly from raw JSON for resilience —
            # avoids dependency on Pydantic model correctly parsing all
            # nested structures, which may vary from documented schema.
            try:
                raw_dict = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Skipping entry with invalid JSON: %s", entry_id)
                continue

            fields = _extract_fields(raw_dict)

            # Diagnostic: log entries where key nested fields are empty
            if not fields["actor_id"] and not fields["outcome_type"]:
                logger.debug(
                    "Entry %s has empty actor_id and outcome_type. "
                    "Raw JSON (first 500 chars): %s",
                    entry_id,
                    raw[:500],
                )

            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO log_entries (
                        id, raw_json, date, timestamp, event_type, category,
                        action, source, outcome_type, actor_type, actor_id,
                        environment, client_ip, host, session_id, object_type,
                        window_start
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry_id,
                        raw,
                        fields["date"],
                        fields["timestamp"],
                        fields["event_type"],
                        fields["category"],
                        fields["action"],
                        fields["source"],
                        fields["outcome_type"],
                        fields["actor_type"],
                        fields["actor_id"],
                        fields["environment"],
                        fields["client_ip"],
                        fields["host"],
                        fields["session_id"],
                        fields["object_type"],
                        window_start,
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    inserted += 1
            except sqlite3.Error as e:
                logger.warning("Failed to insert entry: %s", e)

        # Record the window as fetched
        conn.execute(
            """
            INSERT OR REPLACE INTO fetched_windows
            (window_start, window_end, fetched_at, file_count, entry_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                window_start,
                window_end,
                datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                file_count,
                len(entries),
            ),
        )

        conn.commit()
        logger.debug(
            "Stored %d entries (%d new) for window %s - %s",
            len(entries),
            inserted,
            window_start,
            window_end,
        )
        return inserted

    def query_entries(
        self,
        start: str | None = None,
        end: str | None = None,
        category: str | None = None,
        action: str | None = None,
        actor: str | None = None,
        outcome: str | None = None,
        source: str | None = None,
        ip: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]:
        """Query cached log entries with optional filters.

        All string filters use case-insensitive LIKE matching.
        The search parameter does a free-text search across multiple fields.

        Returns:
            List of row dicts from the log_entries table.
        """
        conn = self._get_conn()
        conditions: list[str] = []
        params: list[str | int] = []

        if start:
            conditions.append("timestamp >= ?")
            params.append(start)
        if end:
            conditions.append("timestamp <= ?")
            params.append(end)
        if category:
            conditions.append("category LIKE ?")
            params.append(f"%{category}%")
        if action:
            conditions.append("action LIKE ?")
            params.append(f"%{action}%")
        if actor:
            conditions.append("actor_id LIKE ?")
            params.append(f"%{actor}%")
        if outcome:
            conditions.append("outcome_type LIKE ?")
            params.append(f"%{outcome}%")
        if source:
            conditions.append("source LIKE ?")
            params.append(f"%{source}%")
        if ip:
            conditions.append("client_ip LIKE ?")
            params.append(f"%{ip}%")
        if search:
            conditions.append(
                "(raw_json LIKE ? OR action LIKE ? OR category LIKE ? "
                "OR actor_id LIKE ? OR environment LIKE ?)"
            )
            term = f"%{search}%"
            params.extend([term, term, term, term, term])

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT * FROM log_entries
            {where}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def count_entries(
        self,
        start: str | None = None,
        end: str | None = None,
        category: str | None = None,
        action: str | None = None,
        actor: str | None = None,
        outcome: str | None = None,
    ) -> int:
        """Count entries matching the given filters."""
        conn = self._get_conn()
        conditions: list[str] = []
        params: list[str] = []

        if start:
            conditions.append("timestamp >= ?")
            params.append(start)
        if end:
            conditions.append("timestamp <= ?")
            params.append(end)
        if category:
            conditions.append("category LIKE ?")
            params.append(f"%{category}%")
        if action:
            conditions.append("action LIKE ?")
            params.append(f"%{action}%")
        if actor:
            conditions.append("actor_id LIKE ?")
            params.append(f"%{actor}%")
        if outcome:
            conditions.append("outcome_type LIKE ?")
            params.append(f"%{outcome}%")

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        row = conn.execute(
            f"SELECT COUNT(*) FROM log_entries {where}", params
        ).fetchone()
        return row[0]

    def get_entry_by_id(self, entry_id: str) -> dict | None:
        """Get a single log entry by its ID."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM log_entries WHERE id = ?", (entry_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_cache_stats(self) -> dict:
        """Get statistics about the cache."""
        conn = self._get_conn()
        total_entries = conn.execute("SELECT COUNT(*) FROM log_entries").fetchone()[0]
        total_windows = conn.execute(
            "SELECT COUNT(*) FROM fetched_windows"
        ).fetchone()[0]

        date_range = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM log_entries"
        ).fetchone()

        categories = conn.execute(
            "SELECT category, COUNT(*) as cnt FROM log_entries "
            "GROUP BY category ORDER BY cnt DESC"
        ).fetchall()

        db_size = self._db_path.stat().st_size if self._db_path.exists() else 0

        return {
            "total_entries": total_entries,
            "total_windows": total_windows,
            "earliest_entry": date_range[0] if date_range else None,
            "latest_entry": date_range[1] if date_range else None,
            "categories": {row[0]: row[1] for row in categories},
            "db_size_bytes": db_size,
        }

    def clear(self) -> None:
        """Clear all cached data."""
        conn = self._get_conn()
        conn.execute("DELETE FROM log_entries")
        conn.execute("DELETE FROM fetched_windows")
        conn.commit()
        logger.info("Cache cleared")
