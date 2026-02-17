"""Parse compressed NDJSON audit log files into structured models.

Each file downloaded from the Unqork API is a compressed file containing
newline-delimited JSON (NDJSON), where each line is a single audit log entry.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import zipfile
from dataclasses import dataclass

from unqork_audit_logs.models import AuditLogEntry

logger = logging.getLogger(__name__)


@dataclass
class ParsedEntry:
    """An audit log entry paired with its original raw JSON string.

    This preserves the exact original JSON from the API so exports
    can round-trip without Pydantic serialization artifacts.
    """

    entry: AuditLogEntry
    raw_json: str


def decompress(data: bytes) -> str:
    """Decompress a log file, trying gzip first then zip.

    Args:
        data: Raw bytes of the compressed file.

    Returns:
        Decompressed text content.

    Raises:
        ValueError: If the data cannot be decompressed.
    """
    # Try gzip first (most common for single-file compressed data)
    try:
        return gzip.decompress(data).decode("utf-8")
    except (gzip.BadGzipFile, OSError):
        pass

    # Try zip
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            parts = []
            for name in zf.namelist():
                parts.append(zf.read(name).decode("utf-8"))
            return "\n".join(parts)
    except zipfile.BadZipFile:
        pass

    # Maybe it's not compressed at all - try reading as utf-8
    try:
        text = data.decode("utf-8")
        # Quick sanity check: does it look like JSON lines?
        if text.strip() and (text.strip()[0] == "{" or text.strip()[0] == "["):
            logger.debug("Data appears to be uncompressed text")
            return text
    except UnicodeDecodeError:
        pass

    raise ValueError("Unable to decompress log file data (tried gzip, zip, raw text)")


def parse_ndjson(text: str) -> list[dict]:
    """Parse newline-delimited JSON into a list of dicts.

    Handles:
    - Standard NDJSON (one JSON object per line)
    - JSON arrays (single array of objects)
    - Empty lines (skipped)

    Args:
        text: NDJSON or JSON text content.

    Returns:
        List of parsed dictionaries.
    """
    text = text.strip()
    if not text:
        return []

    # If it looks like a JSON array, parse it as one
    if text.startswith("["):
        try:
            result = json.loads(text)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    # Parse as NDJSON
    entries = []
    for line_num, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError as e:
            logger.warning("Skipping malformed JSON on line %d: %s", line_num, e)

    return entries


def parse_log_file(data: bytes) -> list[ParsedEntry]:
    """Decompress and parse a single log file into ParsedEntry objects.

    Each ParsedEntry contains both the Pydantic model (for indexing/filtering)
    and the original raw JSON string (for faithful export).

    Args:
        data: Raw bytes of the compressed log file.

    Returns:
        List of ParsedEntry objects.
    """
    text = decompress(data)
    raw_entries = parse_ndjson(text)

    entries = []
    for i, raw in enumerate(raw_entries):
        try:
            entry = AuditLogEntry.model_validate(raw)
            # Preserve the original JSON exactly as it came from the API
            raw_json = json.dumps(raw, separators=(",", ":"), sort_keys=False)
            entries.append(ParsedEntry(entry=entry, raw_json=raw_json))
        except Exception as e:
            logger.warning(
                "Skipping unparseable log entry %d: %s (data: %s)",
                i,
                e,
                str(raw)[:200],
            )

    return entries


def parse_log_files(file_data_list: list[bytes]) -> list[ParsedEntry]:
    """Parse multiple compressed log files into a combined list of entries.

    Args:
        file_data_list: List of raw bytes, each from a downloaded log file.

    Returns:
        Combined list of ParsedEntry objects from all files.
    """
    all_entries: list[ParsedEntry] = []
    for i, data in enumerate(file_data_list):
        try:
            entries = parse_log_file(data)
            all_entries.extend(entries)
        except ValueError as e:
            logger.warning("Failed to process log file %d: %s", i, e)

    return all_entries
