"""Export audit log entries to CSV, JSON, and JSONL formats."""

from __future__ import annotations

import csv
import json
import sys
from io import StringIO


def export_entries(
    entries: list[dict],
    format: str = "json",
    output_path: str = "-",
) -> int:
    """Export log entries to the specified format and destination.

    Args:
        entries: List of row dicts from cache query.
        format: Output format - 'json', 'csv', or 'jsonl'.
        output_path: File path to write to, or '-' for stdout.

    Returns:
        Number of entries exported.
    """
    if format == "json":
        content = _to_json(entries)
    elif format == "jsonl":
        content = _to_jsonl(entries)
    elif format == "csv":
        content = _to_csv(entries)
    else:
        raise ValueError(f"Unsupported export format: {format}")

    if output_path == "-":
        sys.stdout.write(content)
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

    return len(entries)


def _to_json(entries: list[dict]) -> str:
    """Convert entries to a JSON array, using the full raw_json for each."""
    parsed = []
    for entry in entries:
        raw = entry.get("raw_json", "{}")
        try:
            parsed.append(json.loads(raw))
        except json.JSONDecodeError:
            parsed.append(entry)
    return json.dumps(parsed, indent=2) + "\n"


def _to_jsonl(entries: list[dict]) -> str:
    """Convert entries to newline-delimited JSON."""
    lines = []
    for entry in entries:
        raw = entry.get("raw_json", "{}")
        try:
            # Parse and re-dump to ensure valid JSON on each line
            obj = json.loads(raw)
            lines.append(json.dumps(obj, separators=(",", ":")))
        except json.JSONDecodeError:
            lines.append(json.dumps(entry, separators=(",", ":")))
    return "\n".join(lines) + "\n"


def _to_csv(entries: list[dict]) -> str:
    """Convert entries to CSV with flattened columns."""
    if not entries:
        return ""

    # Define the flat columns we export
    columns = [
        "id",
        "timestamp",
        "date",
        "event_type",
        "category",
        "action",
        "source",
        "outcome_type",
        "actor_type",
        "actor_id",
        "environment",
        "client_ip",
        "host",
        "session_id",
        "object_type",
    ]

    output = StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=columns,
        extrasaction="ignore",
    )
    writer.writeheader()

    for entry in entries:
        writer.writerow(entry)

    return output.getvalue()
