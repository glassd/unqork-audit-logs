"""CLI entry point for the Unqork Audit Logs tool.

Provides commands: fetch, list, show, export, summary, config, cache.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional

import typer
from rich.console import Console

from unqork_audit_logs.display import console as display_console

app = typer.Typer(
    name="unqork-logs",
    help="Fetch, view, search, and export Unqork audit logs.",
    no_args_is_help=True,
)
cache_app = typer.Typer(help="Manage the local log cache.")
config_app = typer.Typer(help="Manage configuration.")
app.add_typer(cache_app, name="cache")
app.add_typer(config_app, name="config")

console = Console(stderr=True)


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _get_settings():
    """Load settings, printing a helpful error on failure."""
    from unqork_audit_logs.config import load_settings
    try:
        return load_settings()
    except ValueError as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        raise typer.Exit(1)


def _get_cache(settings=None):
    """Get a LogCache instance."""
    from unqork_audit_logs.cache import LogCache
    if settings is None:
        settings = _get_settings()
    return LogCache(settings.cache_db_path)


# ── fetch ────────────────────────────────────────────────────────────────────


@app.command()
def fetch(
    start: Optional[str] = typer.Option(
        None, "--start", "-s",
        help="Start datetime (e.g. '2025-02-17', '2025-02-17 09:00', '2025-02-17T09:00:00.000Z').",
    ),
    end: Optional[str] = typer.Option(
        None, "--end", "-e",
        help="End datetime (same formats as --start).",
    ),
    last: Optional[str] = typer.Option(
        None, "--last", "-l",
        help="Relative time range (e.g. '24h', '7d', '30m'). Alternative to --start/--end.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging."),
) -> None:
    """Fetch audit logs from the Unqork API and cache them locally."""
    _setup_logging(verbose)

    from unqork_audit_logs.fetcher import (
        FetchProgress,
        fetch_audit_logs,
        parse_datetime_input,
        parse_relative_time,
    )
    from unqork_audit_logs.display import create_fetch_progress, display_fetch_summary

    if last:
        start_dt, end_dt = parse_relative_time(last)
    elif start and end:
        start_dt = parse_datetime_input(start)
        end_dt = parse_datetime_input(end)
    else:
        console.print(
            "[red]Error:[/red] Provide either --start and --end, or --last.\n"
            "Examples:\n"
            "  unqork-logs fetch --start 2025-02-17 --end 2025-02-18\n"
            "  unqork-logs fetch --last 24h"
        )
        raise typer.Exit(1)

    settings = _get_settings()
    cache = _get_cache(settings)

    from unqork_audit_logs.fetcher import generate_windows
    from datetime import timezone

    # Ensure datetimes are UTC
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    windows = generate_windows(start_dt, end_dt)
    console.print(
        f"Fetching audit logs: {start_dt.strftime('%Y-%m-%d %H:%M')} "
        f"to {end_dt.strftime('%Y-%m-%d %H:%M')} UTC "
        f"({len(windows)} hour-windows)"
    )

    progress_tracker = FetchProgress(total_windows=len(windows))

    # Set up Rich progress bar
    with create_fetch_progress() as rich_progress:
        window_task = rich_progress.add_task(
            "Windows", total=len(windows)
        )
        file_task = None

        def on_window_start(ws: str, we: str, file_count: int) -> None:
            nonlocal file_task
            # Show which window we're working on
            rich_progress.update(
                window_task,
                description=f"[{ws[:16]}] {file_count} files",
            )
            if file_task is not None:
                rich_progress.remove_task(file_task)
            if file_count > 0:
                file_task = rich_progress.add_task(
                    "  Downloading", total=file_count
                )

        def on_file_progress(completed: int, total: int) -> None:
            if file_task is not None:
                rich_progress.update(file_task, completed=completed)

        def on_window_complete(ws: str, we: str, entries: int, new: int) -> None:
            rich_progress.advance(window_task)

        def on_window_skip(ws: str, we: str) -> None:
            rich_progress.advance(window_task)

        def on_error(ws: str, we: str, err: str) -> None:
            rich_progress.advance(window_task)
            console.print(f"  [red]Error[/red] [{ws[:16]}]: {err}")

        progress_tracker.on_window_start = on_window_start
        progress_tracker.on_file_progress = on_file_progress
        progress_tracker.on_window_complete = on_window_complete
        progress_tracker.on_window_skip = on_window_skip
        progress_tracker.on_error = on_error

        asyncio.run(
            fetch_audit_logs(settings, cache, start_dt, end_dt, progress_tracker)
        )

    display_fetch_summary(
        total_windows=progress_tracker.total_windows,
        skipped_windows=progress_tracker.skipped_windows,
        total_entries=progress_tracker.total_entries,
        new_entries=progress_tracker.new_entries,
        errors=progress_tracker.errors,
    )

    cache.close()


# ── list ─────────────────────────────────────────────────────────────────────


@app.command(name="list")
def list_entries(
    start: Optional[str] = typer.Option(None, "--start", "-s", help="Filter from datetime."),
    end: Optional[str] = typer.Option(None, "--end", "-e", help="Filter to datetime."),
    last: Optional[str] = typer.Option(None, "--last", "-l", help="Relative time (e.g. '24h', '7d')."),
    category: Optional[str] = typer.Option(None, "--category", "-c", help="Filter by category."),
    action: Optional[str] = typer.Option(None, "--action", "-a", help="Filter by action."),
    actor: Optional[str] = typer.Option(None, "--actor", help="Filter by actor ID/email."),
    outcome: Optional[str] = typer.Option(None, "--outcome", "-o", help="Filter by outcome (success/failure)."),
    source: Optional[str] = typer.Option(None, "--source", help="Filter by source."),
    ip: Optional[str] = typer.Option(None, "--ip", help="Filter by client IP."),
    search: Optional[str] = typer.Option(None, "--search", "-q", help="Free-text search across all fields."),
    limit: int = typer.Option(100, "--limit", "-n", help="Max entries to display."),
    offset: int = typer.Option(0, "--offset", help="Offset for pagination."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """List cached audit log entries with optional filters."""
    _setup_logging(verbose)

    from unqork_audit_logs.filters import build_filters
    from unqork_audit_logs.display import display_entries_table

    filters = build_filters(
        start=start, end=end, last=last,
        category=category, action=action, actor=actor,
        outcome=outcome, source=source, ip=ip, search=search,
        limit=limit, offset=offset,
    )

    cache = _get_cache()
    entries = cache.query_entries(**filters.as_query_kwargs())
    total = cache.count_entries(**filters.as_count_kwargs())
    cache.close()

    display_entries_table(entries, total, offset=offset)


# ── show ─────────────────────────────────────────────────────────────────────


@app.command()
def show(
    entry_id: str = typer.Argument(help="The entry ID (or prefix) to display."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Show detailed view of a single audit log entry."""
    _setup_logging(verbose)

    from unqork_audit_logs.display import display_entry_detail

    cache = _get_cache()

    # Try exact match first
    entry = cache.get_entry_by_id(entry_id)

    # If not found, try prefix match
    if entry is None:
        results = cache._get_conn().execute(
            "SELECT * FROM log_entries WHERE id LIKE ? LIMIT 2",
            (f"{entry_id}%",),
        ).fetchall()
        if len(results) == 1:
            entry = dict(results[0])
        elif len(results) > 1:
            console.print(
                f"[yellow]Ambiguous ID prefix '{entry_id}'. "
                f"Multiple matches found:[/yellow]"
            )
            for r in results:
                console.print(f"  {r['id']}  {r['action']}  {r['timestamp']}")
            cache.close()
            raise typer.Exit(1)

    cache.close()

    if entry is None:
        console.print(f"[red]Entry not found:[/red] {entry_id}")
        raise typer.Exit(1)

    display_entry_detail(entry)


# ── export ───────────────────────────────────────────────────────────────────


@app.command()
def export(
    format: str = typer.Option("json", "--format", "-f", help="Export format: json, csv, jsonl."),
    output: str = typer.Option("-", "--output", "-o", help="Output file path (- for stdout)."),
    start: Optional[str] = typer.Option(None, "--start", "-s"),
    end: Optional[str] = typer.Option(None, "--end", "-e"),
    last: Optional[str] = typer.Option(None, "--last", "-l"),
    category: Optional[str] = typer.Option(None, "--category", "-c"),
    action: Optional[str] = typer.Option(None, "--action", "-a"),
    actor: Optional[str] = typer.Option(None, "--actor"),
    outcome: Optional[str] = typer.Option(None, "--outcome"),
    search: Optional[str] = typer.Option(None, "--search", "-q"),
    limit: int = typer.Option(10000, "--limit", "-n", help="Max entries to export."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Export cached audit log entries to a file."""
    _setup_logging(verbose)

    from unqork_audit_logs.filters import build_filters
    from unqork_audit_logs.export import export_entries

    filters = build_filters(
        start=start, end=end, last=last,
        category=category, action=action, actor=actor,
        outcome=outcome, search=search,
        limit=limit,
    )

    cache = _get_cache()
    entries = cache.query_entries(**filters.as_query_kwargs())
    cache.close()

    if not entries:
        console.print("[dim]No entries to export.[/dim]")
        raise typer.Exit(0)

    count = export_entries(entries, format=format, output_path=output)
    if output != "-":
        console.print(f"Exported {count} entries to {output} ({format})")


# ── summary ──────────────────────────────────────────────────────────────────


@app.command()
def summary(
    start: Optional[str] = typer.Option(None, "--start", "-s"),
    end: Optional[str] = typer.Option(None, "--end", "-e"),
    last: Optional[str] = typer.Option(None, "--last", "-l"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Show summary statistics for cached audit log entries."""
    _setup_logging(verbose)

    from unqork_audit_logs.filters import build_filters
    from unqork_audit_logs.summary import display_summary

    filters = build_filters(start=start, end=end, last=last, limit=100000)

    cache = _get_cache()
    entries = cache.query_entries(**filters.as_query_kwargs())
    cache.close()

    if not entries:
        console.print("[dim]No entries found for the specified range.[/dim]")
        raise typer.Exit(0)

    display_summary(entries)


# ── config commands ──────────────────────────────────────────────────────────


@config_app.command(name="check")
def config_check(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Verify configuration and test authentication."""
    _setup_logging(verbose)

    import httpx
    from unqork_audit_logs.display import display_config_status
    from unqork_audit_logs.auth import TokenManager

    settings = _get_settings()
    token_manager = TokenManager(settings)

    auth_ok = False
    error = None

    try:
        async def _test_auth():
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                verify=settings.verify_ssl,
            ) as client:
                await token_manager.get_token(client)

        asyncio.run(_test_auth())
        auth_ok = True
    except Exception as e:
        error = str(e)

    display_config_status(
        settings_ok=True,
        auth_ok=auth_ok,
        base_url=settings.base_url,
        error=error,
    )

    if not auth_ok:
        raise typer.Exit(1)


# ── cache commands ───────────────────────────────────────────────────────────


@cache_app.command(name="info")
def cache_info(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Show cache statistics and information."""
    _setup_logging(verbose)

    from unqork_audit_logs.display import display_cache_stats

    cache = _get_cache()
    stats = cache.get_cache_stats()
    cache.close()

    display_cache_stats(stats)


@cache_app.command(name="clear")
def cache_clear(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Clear all cached log data."""
    _setup_logging(verbose)

    if not confirm:
        confirmed = typer.confirm("Are you sure you want to clear the entire cache?")
        if not confirmed:
            raise typer.Abort()

    cache = _get_cache()
    cache.clear()
    cache.close()

    console.print("[green]Cache cleared.[/green]")


@cache_app.command(name="windows")
def cache_windows(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """List all fetched time windows in the cache."""
    _setup_logging(verbose)

    from rich.table import Table

    cache = _get_cache()
    windows = cache.get_fetched_windows()
    cache.close()

    if not windows:
        console.print("[dim]No windows fetched yet.[/dim]")
        return

    table = Table(title=f"Fetched Windows ({len(windows)})")
    table.add_column("Start", width=24)
    table.add_column("End", width=24)
    table.add_column("Fetched At", width=24)
    table.add_column("Files", justify="right", width=6)
    table.add_column("Entries", justify="right", width=8)

    for w in windows:
        table.add_row(
            w["window_start"],
            w["window_end"],
            w["fetched_at"],
            str(w["file_count"]),
            str(w["entry_count"]),
        )

    display_console.print(table)


if __name__ == "__main__":
    app()
