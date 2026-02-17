"""Summary statistics and analytics for audit log entries."""

from __future__ import annotations

from collections import Counter

from rich.console import Console
from rich.table import Table

console = Console()


def display_summary(entries: list[dict]) -> None:
    """Display comprehensive summary statistics for a set of log entries.

    Includes:
    - Overall stats
    - Breakdown by category
    - Breakdown by action (top 20)
    - User activity (top 20 actors)
    - Failure analysis
    """
    if not entries:
        console.print("[dim]No entries to summarize.[/dim]")
        return

    total = len(entries)

    # Counters
    categories = Counter()
    actions = Counter()
    actors = Counter()
    outcomes = Counter()
    sources = Counter()
    ips = Counter()
    failure_actions = Counter()

    timestamps = []

    for entry in entries:
        categories[entry.get("category", "unknown")] += 1
        actions[entry.get("action", "unknown")] += 1
        actors[entry.get("actor_id", "unknown")] += 1
        outcomes[entry.get("outcome_type", "unknown")] += 1
        sources[entry.get("source", "unknown")] += 1

        ip = entry.get("client_ip", "")
        if ip:
            ips[ip] += 1

        ts = entry.get("timestamp", "")
        if ts:
            timestamps.append(ts)

        if entry.get("outcome_type") == "failure":
            failure_actions[entry.get("action", "unknown")] += 1

    timestamps.sort()

    # ── Overall Stats ──

    overall = Table(title=f"Summary ({total:,} entries)", show_lines=True)
    overall.add_column("Metric", style="bold")
    overall.add_column("Value")

    overall.add_row("Total Events", f"{total:,}")
    overall.add_row("Date Range", f"{timestamps[0] if timestamps else 'N/A'} to {timestamps[-1] if timestamps else 'N/A'}")
    overall.add_row("Unique Categories", str(len(categories)))
    overall.add_row("Unique Actions", str(len(actions)))
    overall.add_row("Unique Actors", str(len(actors)))
    overall.add_row("Success", f"[green]{outcomes.get('success', 0):,}[/green]")
    overall.add_row("Failure", f"[red]{outcomes.get('failure', 0):,}[/red]")

    success_count = outcomes.get("success", 0)
    if total > 0:
        success_rate = (success_count / total) * 100
        overall.add_row("Success Rate", f"{success_rate:.1f}%")

    console.print(overall)

    # ── By Category ──

    cat_table = Table(title="Events by Category")
    cat_table.add_column("Category")
    cat_table.add_column("Count", justify="right")
    cat_table.add_column("% of Total", justify="right")

    for cat, count in categories.most_common():
        pct = (count / total) * 100
        cat_table.add_row(cat, f"{count:,}", f"{pct:.1f}%")

    console.print(cat_table)

    # ── Top Actions ──

    action_table = Table(title="Top 20 Actions")
    action_table.add_column("Action")
    action_table.add_column("Count", justify="right")
    action_table.add_column("% of Total", justify="right")

    for act, count in actions.most_common(20):
        pct = (count / total) * 100
        action_table.add_row(act, f"{count:,}", f"{pct:.1f}%")

    console.print(action_table)

    # ── Top Actors ──

    actor_table = Table(title="Top 20 Actors (Most Active Users)")
    actor_table.add_column("Actor")
    actor_table.add_column("Events", justify="right")
    actor_table.add_column("% of Total", justify="right")

    for act, count in actors.most_common(20):
        pct = (count / total) * 100
        actor_table.add_row(act or "[dim]empty[/dim]", f"{count:,}", f"{pct:.1f}%")

    console.print(actor_table)

    # ── Top IPs ──

    if ips:
        ip_table = Table(title="Top 10 Client IPs")
        ip_table.add_column("IP Address")
        ip_table.add_column("Events", justify="right")

        for ip, count in ips.most_common(10):
            ip_table.add_row(ip, f"{count:,}")

        console.print(ip_table)

    # ── Failure Analysis ──

    failure_count = outcomes.get("failure", 0)
    if failure_count > 0:
        fail_table = Table(title=f"Failure Analysis ({failure_count:,} failures)")
        fail_table.add_column("Action")
        fail_table.add_column("Failures", justify="right")

        for act, count in failure_actions.most_common(20):
            fail_table.add_row(act, f"{count:,}")

        console.print(fail_table)
