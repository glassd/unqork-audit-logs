"""Interactive terminal UI for browsing cached audit logs.

Launched via ``unqork-logs view``. Provides a scrollable, filterable table
backed by the local SQLite cache, a detail view for individual entries, and
the ability to export the current filtered result set or save the active
filters to a file for reuse.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Static,
)
from rich.json import JSON
from rich.text import Text

from unqork_audit_logs.cache import LogCache
from unqork_audit_logs.display import CATEGORY_COLORS, OUTCOME_COLORS
from unqork_audit_logs.export import export_entries
from unqork_audit_logs.fetcher import parse_datetime_input, parse_relative_time

# Filter fields exposed as inputs in the filter bar, in display order.
FILTER_FIELDS = [
    ("search", "search… (/)"),
    ("category", "category"),
    ("action", "action"),
    ("actor", "actor"),
    ("outcome", "outcome"),
    ("ip", "client ip"),
    ("since", "since (e.g. 24h)"),
]

PAGE_SIZE = 200


def parse_since(value: str) -> str | None:
    """Convert a 'since' filter value into a cache timestamp lower bound.

    Accepts relative expressions ('24h', '7d', '30m') or absolute datetimes
    ('2025-02-17', '2025-02-17 09:00'). Returns an ISO-8601 string suitable
    for the cache's ``start`` filter, or ``None`` if the value is blank.

    Raises:
        ValueError: If the value cannot be parsed.
    """
    value = value.strip()
    if not value:
        return None

    lowered = value.lower()
    if lowered and lowered[-1] in "hmd" and lowered[:-1].isdigit():
        start, _ = parse_relative_time(lowered)
    else:
        start = parse_datetime_input(value)
    return start.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def build_view_filters(values: dict[str, str]) -> dict[str, Any]:
    """Build cache query kwargs from raw filter-bar input values.

    Blank values are dropped. The ``since`` field is translated into a
    ``start`` lower bound. Raises ValueError on an unparseable ``since``.
    """
    filters: dict[str, Any] = {}
    for key in ("search", "category", "action", "actor", "outcome", "ip"):
        v = values.get(key, "").strip()
        if v:
            filters[key] = v

    since = parse_since(values.get("since", ""))
    if since:
        filters["start"] = since
    return filters


def _fmt_timestamp(ts: str) -> str:
    """Trim milliseconds from a timestamp for compact display."""
    if "." in ts:
        return ts.split(".")[0] + "Z"
    return ts


def _category_cell(category: str) -> Text:
    return Text(category, style=CATEGORY_COLORS.get(category, "white"))


def _outcome_cell(outcome: str) -> Text:
    return Text(outcome, style=OUTCOME_COLORS.get(outcome, "white"))


class DetailScreen(ModalScreen):
    """Modal showing the full JSON and key fields of a single entry."""

    BINDINGS = [
        Binding("escape,q,enter", "dismiss", "Close"),
    ]

    def __init__(self, entry: dict) -> None:
        super().__init__()
        self._entry = entry

    def compose(self) -> ComposeResult:
        entry = self._entry
        summary = (
            f"[bold]ID:[/bold] {entry.get('id', '')}    "
            f"[bold]Timestamp:[/bold] {entry.get('timestamp', '')}\n"
            f"[bold]Category:[/bold] {entry.get('category', '')}    "
            f"[bold]Action:[/bold] {entry.get('action', '')}\n"
            f"[bold]Actor:[/bold] {entry.get('actor_id', '')}    "
            f"[bold]Outcome:[/bold] {entry.get('outcome_type', '')}    "
            f"[bold]IP:[/bold] {entry.get('client_ip', '')}"
        )

        raw = entry.get("raw_json", "{}")
        try:
            body: Any = JSON(json.dumps(json.loads(raw), indent=2))
        except json.JSONDecodeError:
            body = raw

        with Vertical(id="detail-box"):
            yield Static(summary, id="detail-summary")
            with VerticalScroll(id="detail-json"):
                yield Static(body)
            yield Label("esc / enter to close", id="detail-hint")

    def action_dismiss(self) -> None:
        self.dismiss()


class ExportScreen(ModalScreen):
    """Modal prompting for an export format and destination path."""

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, total: int, default_path: str = "audit_export.csv") -> None:
        super().__init__()
        self._total = total
        self._default_path = default_path

    def compose(self) -> ComposeResult:
        with Vertical(id="export-box"):
            yield Label(f"Export {self._total:,} filtered entries", id="export-title")
            yield Label("Format (json / jsonl / csv):")
            yield Input(value="csv", id="export-format")
            yield Label("Output path:")
            yield Input(value=self._default_path, id="export-path")
            with Horizontal(id="export-buttons"):
                yield Button("Export", variant="primary", id="export-ok")
                yield Button("Cancel", id="export-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "export-ok":
            self._submit()
        else:
            self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self._submit()

    def _submit(self) -> None:
        fmt = self.query_one("#export-format", Input).value.strip().lower()
        path = self.query_one("#export-path", Input).value.strip()
        self.dismiss((fmt, path))

    def action_cancel(self) -> None:
        self.dismiss(None)


class PromptScreen(ModalScreen):
    """Generic single-line prompt modal returning the entered string."""

    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, title: str, default: str = "") -> None:
        super().__init__()
        self._title = title
        self._default = default

    def compose(self) -> ComposeResult:
        with Vertical(id="prompt-box"):
            yield Label(self._title)
            yield Input(value=self._default, id="prompt-input")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value.strip())

    def action_cancel(self) -> None:
        self.dismiss(None)


class AuditLogApp(App):
    """Interactive browser for the local audit-log cache."""

    CSS = """
    #filter-bar {
        height: auto;
        padding: 0 1;
        background: $panel;
    }
    #filter-bar Input {
        width: 1fr;
        margin: 0 1 0 0;
    }
    DataTable {
        height: 1fr;
    }
    #status {
        height: 1;
        padding: 0 1;
        background: $boost;
        color: $text-muted;
    }
    DetailScreen {
        align: center middle;
    }
    #detail-box {
        width: 90%;
        height: 90%;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }
    #detail-summary { height: auto; padding-bottom: 1; }
    #detail-json { height: 1fr; border: round $panel; }
    #detail-hint { color: $text-muted; padding-top: 1; }
    ExportScreen, PromptScreen {
        align: center middle;
    }
    #export-box, #prompt-box {
        width: 60;
        height: auto;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }
    #export-title { padding-bottom: 1; text-style: bold; }
    #export-buttons { height: auto; padding-top: 1; }
    #export-buttons Button { margin: 0 1 0 0; }
    """

    BINDINGS = [
        Binding("slash", "focus_search", "Search"),
        Binding("f", "focus_filters", "Filters"),
        Binding("ctrl+r", "apply_filters", "Apply"),
        Binding("x", "clear_filters", "Clear"),
        Binding("e", "export", "Export"),
        Binding("w", "save_filters", "Save filters"),
        Binding("r", "reload", "Reload"),
        Binding("escape", "focus_table", "Table"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, cache: LogCache, initial_filters: dict | None = None) -> None:
        super().__init__()
        self._cache = cache
        self._initial = initial_filters or {}
        self._offset = 0
        self._total = 0
        self._entries: dict[str, dict] = {}

    # ── composition ───────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="filter-bar"):
            for key, placeholder in FILTER_FIELDS:
                yield Input(
                    value=str(self._initial.get(key, "")),
                    placeholder=placeholder,
                    id=f"f-{key}",
                )
        yield DataTable(id="table", cursor_type="row", zebra_stripes=True)
        yield Static("", id="status")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#table", DataTable)
        table.add_columns("Timestamp", "Category", "Action", "Actor", "Outcome", "IP")
        self.title = "Unqork Audit Logs"
        # Load the next page when the viewport is scrolled near the bottom by
        # any means (mouse wheel, scrollbar, keyboard) — not just when the
        # row cursor moves.
        self.watch(table, "scroll_y", self._on_table_scroll, init=False)
        self._reload()
        table.focus()

    # ── data loading ──────────────────────────────────────────────────────

    def _filter_values(self) -> dict[str, str]:
        return {
            key: self.query_one(f"#f-{key}", Input).value
            for key, _ in FILTER_FIELDS
        }

    def _current_filters(self) -> dict[str, Any]:
        return build_view_filters(self._filter_values())

    def _reload(self) -> None:
        """Re-run the query from scratch with the current filters."""
        table = self.query_one("#table", DataTable)
        table.clear()
        self._entries.clear()
        self._offset = 0

        try:
            filters = self._current_filters()
        except ValueError as e:
            self._set_status(f"[red]Filter error:[/red] {e}")
            return

        self._total = self._cache.count_entries(**filters)
        self._load_more(filters)

    def _load_more(self, filters: dict[str, Any] | None = None) -> None:
        """Append the next page of results to the table."""
        if filters is None:
            try:
                filters = self._current_filters()
            except ValueError:
                return
        if self._offset >= self._total and self._offset > 0:
            return

        rows = self._cache.query_entries(
            limit=PAGE_SIZE, offset=self._offset, **filters
        )
        table = self.query_one("#table", DataTable)
        for entry in rows:
            key = entry["id"]
            self._entries[key] = entry
            table.add_row(
                _fmt_timestamp(entry.get("timestamp", "")),
                _category_cell(entry.get("category", "")),
                entry.get("action", ""),
                entry.get("actor_id", ""),
                _outcome_cell(entry.get("outcome_type", "")),
                entry.get("client_ip", ""),
                key=key,
            )
        self._offset += len(rows)
        self._update_status()

    def _update_status(self) -> None:
        self._set_status(
            f"Showing [b]{self._offset:,}[/b] of [b]{self._total:,}[/b] entries   "
            f"·  [b]/[/b] search  [b]f[/b] filters  [b]enter[/b] detail  "
            f"[b]e[/b] export  [b]w[/b] save filters  [b]x[/b] clear  [b]q[/b] quit"
        )

    def _set_status(self, markup: str) -> None:
        self.query_one("#status", Static).update(markup)

    # ── events ────────────────────────────────────────────────────────────

    def on_input_submitted(self, event: Input.Submitted) -> None:
        # Enter in any filter field re-runs the query.
        self.action_apply_filters()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key = event.row_key.value
        entry = self._entries.get(key)
        if entry is not None:
            self.push_screen(DetailScreen(entry))

    def on_data_table_row_highlighted(
        self, event: DataTable.RowHighlighted
    ) -> None:
        # Infinite scroll: load the next page as the cursor nears the bottom.
        if event.cursor_row >= self._offset - 20 and self._offset < self._total:
            self._load_more()

    def _on_table_scroll(self) -> None:
        # Infinite scroll on viewport movement (mouse wheel, scrollbar drag).
        if self._offset >= self._total:
            return
        table = self.query_one("#table", DataTable)
        if table.max_scroll_y - table.scroll_y <= 3:
            self._load_more()

    # ── actions ───────────────────────────────────────────────────────────

    def action_apply_filters(self) -> None:
        self._reload()
        self.query_one("#table", DataTable).focus()

    def action_focus_search(self) -> None:
        self.query_one("#f-search", Input).focus()

    def action_focus_filters(self) -> None:
        self.query_one("#f-category", Input).focus()

    def action_focus_table(self) -> None:
        self.query_one("#table", DataTable).focus()

    def action_clear_filters(self) -> None:
        for key, _ in FILTER_FIELDS:
            self.query_one(f"#f-{key}", Input).value = ""
        self._reload()

    def action_reload(self) -> None:
        self._reload()

    def action_export(self) -> None:
        try:
            filters = self._current_filters()
        except ValueError as e:
            self.notify(f"Filter error: {e}", severity="error")
            return
        total = self._cache.count_entries(**filters)
        self.push_screen(ExportScreen(total), self._do_export)

    def _do_export(self, result: tuple[str, str] | None) -> None:
        if not result:
            return
        fmt, path = result
        if fmt not in ("json", "jsonl", "csv"):
            self.notify(f"Unsupported format: {fmt}", severity="error")
            return
        try:
            filters = self._current_filters()
            # Export the full filtered set, not just the loaded page.
            entries = self._cache.query_entries(limit=0, **filters)
            count = export_entries(entries, format=fmt, output_path=path)
        except Exception as e:  # noqa: BLE001 - surface any failure to the UI
            self.notify(f"Export failed: {e}", severity="error")
            return
        self.notify(f"Exported {count:,} entries to {path}", severity="information")

    def action_save_filters(self) -> None:
        self.push_screen(
            PromptScreen("Save current filters to file:", "filters.json"),
            self._do_save_filters,
        )

    def _do_save_filters(self, path: str | None) -> None:
        if not path:
            return
        values = {k: v for k, v in self._filter_values().items() if v.strip()}
        try:
            Path(path).write_text(json.dumps(values, indent=2), encoding="utf-8")
        except OSError as e:
            self.notify(f"Could not save filters: {e}", severity="error")
            return
        self.notify(f"Saved filters to {path}", severity="information")


def load_filters_file(path: str) -> dict:
    """Load a saved filter set (field -> value) from a JSON file."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Filter file must contain a JSON object of field: value")
    return {str(k): str(v) for k, v in data.items()}


def run_tui(cache: LogCache, initial_filters: dict | None = None) -> None:
    """Launch the interactive viewer against the given cache."""
    AuditLogApp(cache, initial_filters=initial_filters).run()
