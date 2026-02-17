"""Orchestrator for fetching audit logs across multi-hour date ranges.

Handles splitting date ranges into 1-hour windows, checking the cache,
fetching missing windows, downloading files concurrently, and storing results.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

import httpx

from unqork_audit_logs.auth import TokenManager
from unqork_audit_logs.cache import LogCache
from unqork_audit_logs.client import AuditLogClient
from unqork_audit_logs.config import Settings
from unqork_audit_logs.parser import parse_log_files

logger = logging.getLogger(__name__)

# Unqork API requires ISO 8601 UTC with milliseconds
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


def generate_windows(
    start: datetime, end: datetime
) -> list[tuple[str, str]]:
    """Split a date range into 1-hour windows.

    Args:
        start: Range start (UTC).
        end: Range end (UTC).

    Returns:
        List of (start_str, end_str) tuples in Unqork's required format.
    """
    windows = []
    current = start
    while current < end:
        window_end = min(current + timedelta(hours=1), end)
        windows.append((
            current.strftime(DATETIME_FORMAT),
            window_end.strftime(DATETIME_FORMAT),
        ))
        current = window_end
    return windows


class FetchProgress:
    """Tracks progress across the full fetch operation."""

    def __init__(self, total_windows: int) -> None:
        self.total_windows = total_windows
        self.completed_windows = 0
        self.skipped_windows = 0
        self.total_files = 0
        self.downloaded_files = 0
        self.total_entries = 0
        self.new_entries = 0
        self.errors: list[str] = []

        # Callbacks
        self.on_window_start: Callable[[str, str, int], None] | None = None
        self.on_file_progress: Callable[[int, int], None] | None = None
        self.on_window_complete: Callable[[str, str, int, int], None] | None = None
        self.on_window_skip: Callable[[str, str], None] | None = None
        self.on_error: Callable[[str, str, str], None] | None = None


async def fetch_audit_logs(
    settings: Settings,
    cache: LogCache,
    start: datetime,
    end: datetime,
    progress: FetchProgress | None = None,
) -> FetchProgress:
    """Fetch audit logs for a date range, storing results in the cache.

    Splits the range into 1-hour windows, skips already-cached windows,
    and fetches the rest concurrently (file downloads within each window).

    Args:
        settings: Application settings.
        cache: The log cache to store results in.
        start: Range start (UTC).
        end: Range end (UTC).
        progress: Optional progress tracker for UI updates.

    Returns:
        The FetchProgress with final statistics.
    """
    windows = generate_windows(start, end)

    if progress is None:
        progress = FetchProgress(total_windows=len(windows))
    else:
        progress.total_windows = len(windows)

    token_manager = TokenManager(settings)
    api_client = AuditLogClient(settings, token_manager)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60.0),
        verify=settings.verify_ssl,
    ) as client:
        for window_start, window_end in windows:
            # Skip already-fetched windows
            if cache.is_window_fetched(window_start, window_end):
                progress.skipped_windows += 1
                progress.completed_windows += 1
                if progress.on_window_skip:
                    progress.on_window_skip(window_start, window_end)
                continue

            # Fetch log file locations for this window
            try:
                locations = await api_client.fetch_log_locations(
                    client, window_start, window_end
                )
            except Exception as e:
                error_msg = f"Failed to fetch locations for {window_start}: {e}"
                logger.error(error_msg)
                progress.errors.append(error_msg)
                if progress.on_error:
                    progress.on_error(window_start, window_end, str(e))
                continue

            progress.total_files += len(locations)

            if progress.on_window_start:
                progress.on_window_start(
                    window_start, window_end, len(locations)
                )

            if not locations:
                # No logs for this window - still mark as fetched
                cache.store_window(window_start, window_end, [], 0)
                progress.completed_windows += 1
                if progress.on_window_complete:
                    progress.on_window_complete(window_start, window_end, 0, 0)
                continue

            # Download all files for this window concurrently
            def _file_progress(completed: int, total: int) -> None:
                progress.downloaded_files = (
                    progress.downloaded_files - total + completed
                )
                if progress.on_file_progress:
                    progress.on_file_progress(completed, total)

            try:
                file_data_list = await api_client.download_log_files(
                    client, locations, on_progress=_file_progress
                )
                progress.downloaded_files += len(locations)
            except Exception as e:
                error_msg = f"Failed downloading files for {window_start}: {e}"
                logger.error(error_msg)
                progress.errors.append(error_msg)
                if progress.on_error:
                    progress.on_error(window_start, window_end, str(e))
                continue

            # Parse all downloaded files
            entries = parse_log_files(file_data_list)
            progress.total_entries += len(entries)

            # Store in cache
            new_count = cache.store_window(
                window_start, window_end, entries, len(locations)
            )
            progress.new_entries += new_count
            progress.completed_windows += 1

            if progress.on_window_complete:
                progress.on_window_complete(
                    window_start, window_end, len(entries), new_count
                )

    return progress


def parse_datetime_input(value: str) -> datetime:
    """Parse a user-provided datetime string into a UTC datetime.

    Accepts various formats:
    - ISO 8601: 2025-02-17T09:00:00.000Z
    - Date + time: 2025-02-17 09:00
    - Date + time with seconds: 2025-02-17 09:00:00
    - Date only: 2025-02-17 (assumes 00:00:00)

    Returns:
        A timezone-aware UTC datetime.
    """
    value = value.strip()

    # Full ISO format with Z
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)

    # Try various formats
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    raise ValueError(
        f"Cannot parse datetime '{value}'. "
        f"Expected format like '2025-02-17', '2025-02-17 09:00', "
        f"or '2025-02-17T09:00:00.000Z'"
    )


def parse_relative_time(value: str) -> tuple[datetime, datetime]:
    """Parse a relative time expression like '24h', '7d', '30m'.

    Returns:
        Tuple of (start, end) datetimes in UTC, where end is now.
    """
    value = value.strip().lower()
    now = datetime.now(timezone.utc)

    if value.endswith("h"):
        hours = int(value[:-1])
        start = now - timedelta(hours=hours)
    elif value.endswith("d"):
        days = int(value[:-1])
        start = now - timedelta(days=days)
    elif value.endswith("m"):
        minutes = int(value[:-1])
        start = now - timedelta(minutes=minutes)
    else:
        raise ValueError(
            f"Cannot parse relative time '{value}'. "
            f"Use format like '24h', '7d', or '30m'."
        )

    return start, now
