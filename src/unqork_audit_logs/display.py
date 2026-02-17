"""Rich terminal display for audit log data.

Provides formatted tables, detail views, progress bars,
and color-coded output for the CLI.
"""

from __future__ import annotations

import json

from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

console = Console()

# Color mapping for outcome types
OUTCOME_COLORS = {
    "success": "green",
    "failure": "red",
}

# Color mapping for categories
CATEGORY_COLORS = {
    "access-management": "cyan",
    "user-access": "blue",
    "user-management": "magenta",
    "configuration": "yellow",
    "data-access": "green",
}


def _outcome_text(outcome: str) -> Text:
    """Create a colored Text object for an outcome type."""
    color = OUTCOME_COLORS.get(outcome, "white")
    return Text(outcome, style=color)


def _category_text(category: str) -> Text:
    """Create a colored Text object for a category."""
    color = CATEGORY_COLORS.get(category, "white")
    return Text(category, style=color)


def display_entries_table(entries: list[dict], total_count: int, offset: int = 0) -> None:
    """Display log entries in a Rich table.

    Args:
        entries: List of row dicts from cache query.
        total_count: Total matching entries (for pagination info).
        offset: Current offset for pagination display.
    """
    if not entries:
        console.print("[dim]No log entries found matching the criteria.[/dim]")
        return

    table = Table(
        title=f"Audit Log Entries ({offset + 1}-{offset + len(entries)} of {total_count})",
        show_lines=False,
        padding=(0, 1),
        expand=True,
    )

    table.add_column("ID", style="dim", no_wrap=True, max_width=12)
    table.add_column("Timestamp", no_wrap=True)
    table.add_column("Category")
    table.add_column("Action")
    table.add_column("Actor")
    table.add_column("Outcome", justify="center", max_width=10)
    table.add_column("IP", no_wrap=True)

    for entry in entries:
        # Truncate timestamp for display (remove milliseconds)
        ts = entry.get("timestamp", "")
        if "." in ts:
            ts = ts.split(".")[0] + "Z"

        table.add_row(
            entry.get("id", "")[:10],
            ts,
            _category_text(entry.get("category", "")),
            entry.get("action", ""),
            entry.get("actor_id", ""),
            _outcome_text(entry.get("outcome_type", "")),
            entry.get("client_ip", ""),
        )

    console.print(table)

    if total_count > offset + len(entries):
        remaining = total_count - offset - len(entries)
        console.print(
            f"[dim]  ... {remaining} more entries. "
            f"Use --offset {offset + len(entries)} to see more.[/dim]"
        )


def display_entry_detail(entry: dict) -> None:
    """Display detailed view of a single log entry.

    Shows the full JSON with syntax highlighting in a panel.
    """
    entry_id = entry.get("id", "unknown")
    raw_json = entry.get("raw_json", "{}")

    # Parse and re-format for pretty display
    try:
        parsed = json.loads(raw_json)
        formatted = json.dumps(parsed, indent=2)
    except json.JSONDecodeError:
        formatted = raw_json

    # Summary header
    header_parts = [
        f"[bold]ID:[/bold] {entry_id}",
        f"[bold]Timestamp:[/bold] {entry.get('timestamp', '')}",
        f"[bold]Category:[/bold] {entry.get('category', '')}",
        f"[bold]Action:[/bold] {entry.get('action', '')}",
        f"[bold]Actor:[/bold] {entry.get('actor_id', '')}",
        f"[bold]Outcome:[/bold] {entry.get('outcome_type', '')}",
        f"[bold]Environment:[/bold] {entry.get('environment', '')}",
        f"[bold]Client IP:[/bold] {entry.get('client_ip', '')}",
        f"[bold]Source:[/bold] {entry.get('source', '')}",
    ]
    header = "\n".join(header_parts)

    console.print(Panel(header, title="Entry Summary", border_style="blue"))
    console.print(Panel(JSON(formatted), title="Full JSON", border_style="dim"))


def display_cache_stats(stats: dict) -> None:
    """Display cache statistics."""
    table = Table(title="Cache Statistics", show_lines=True)
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("Total Entries", f"{stats['total_entries']:,}")
    table.add_row("Fetched Windows", f"{stats['total_windows']:,}")
    table.add_row("Earliest Entry", stats.get("earliest_entry") or "N/A")
    table.add_row("Latest Entry", stats.get("latest_entry") or "N/A")

    db_size = stats.get("db_size_bytes", 0)
    if db_size > 1_048_576:
        size_str = f"{db_size / 1_048_576:.1f} MB"
    elif db_size > 1024:
        size_str = f"{db_size / 1024:.1f} KB"
    else:
        size_str = f"{db_size} bytes"
    table.add_row("Database Size", size_str)

    console.print(table)

    categories = stats.get("categories", {})
    if categories:
        cat_table = Table(title="Entries by Category")
        cat_table.add_column("Category")
        cat_table.add_column("Count", justify="right")
        for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
            cat_table.add_row(_category_text(cat), str(count))
        console.print(cat_table)


def create_fetch_progress() -> Progress:
    """Create a Rich Progress bar for fetch operations."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    )


def display_fetch_summary(
    total_windows: int,
    skipped_windows: int,
    total_entries: int,
    new_entries: int,
    errors: list[str],
) -> None:
    """Display a summary after a fetch operation."""
    console.print()

    table = Table(title="Fetch Summary", show_lines=True)
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("Time Windows", f"{total_windows}")
    table.add_row("Already Cached", f"{skipped_windows}")
    table.add_row("Newly Fetched", f"{total_windows - skipped_windows}")
    table.add_row("Total Entries", f"{total_entries:,}")
    table.add_row("New Entries", f"{new_entries:,}")

    if errors:
        table.add_row("Errors", f"[red]{len(errors)}[/red]")

    console.print(table)

    if errors:
        console.print()
        for err in errors:
            console.print(f"  [red]Error:[/red] {err}")


def display_config_status(settings_ok: bool, auth_ok: bool, base_url: str, error: str | None = None) -> None:
    """Display configuration check results."""
    table = Table(title="Configuration Status", show_lines=True)
    table.add_column("Check", style="bold")
    table.add_column("Status")

    check_mark = "[green]OK[/green]"
    x_mark = "[red]FAILED[/red]"

    table.add_row("Environment URL", base_url)
    table.add_row("Settings Loaded", check_mark if settings_ok else x_mark)
    table.add_row("Authentication", check_mark if auth_ok else x_mark)

    console.print(table)

    if error:
        console.print(f"\n[red]Error:[/red] {error}")
