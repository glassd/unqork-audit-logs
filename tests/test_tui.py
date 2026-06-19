"""Tests for the interactive viewer (helpers + a headless app drive)."""

from __future__ import annotations

import json

import pytest

from textual.widgets import DataTable, Input

from unqork_audit_logs.tui import (
    AuditLogApp,
    build_view_filters,
    load_filters_file,
    parse_since,
)


class TestParseSince:
    def test_blank_returns_none(self):
        assert parse_since("") is None
        assert parse_since("   ") is None

    def test_relative(self):
        result = parse_since("24h")
        assert result is not None and result.endswith(".000Z")

    def test_absolute_date(self):
        assert parse_since("2025-02-17") == "2025-02-17T00:00:00.000Z"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_since("not-a-time")


class TestBuildViewFilters:
    def test_drops_blanks(self):
        filters = build_view_filters(
            {"search": "login", "category": "", "actor": "  "}
        )
        assert filters == {"search": "login"}

    def test_since_becomes_start(self):
        filters = build_view_filters({"since": "2025-02-17", "action": "delete"})
        assert filters["action"] == "delete"
        assert filters["start"] == "2025-02-17T00:00:00.000Z"


class TestLoadFiltersFile:
    def test_round_trip(self, tmp_path):
        p = tmp_path / "filters.json"
        p.write_text(json.dumps({"category": "user-access", "search": "x"}))
        assert load_filters_file(str(p)) == {
            "category": "user-access",
            "search": "x",
        }

    def test_rejects_non_object(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text(json.dumps(["not", "an", "object"]))
        with pytest.raises(ValueError):
            load_filters_file(str(p))


class TestApp:
    async def test_loads_all_rows(self, tmp_cache):
        app = AuditLogApp(tmp_cache)
        async with app.run_test() as pilot:
            await pilot.pause()
            table = app.query_one("#table", DataTable)
            assert table.row_count == 5  # tmp_cache fixture has 5 entries

    async def test_search_filter_narrows_rows(self, tmp_cache):
        app = AuditLogApp(tmp_cache)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.query_one("#f-search", Input).value = "delete"
            app.action_apply_filters()
            await pilot.pause()
            table = app.query_one("#table", DataTable)
            assert table.row_count == 1  # only the delete-designer-role entry

    async def test_initial_filters_applied(self, tmp_cache):
        app = AuditLogApp(tmp_cache, initial_filters={"outcome": "failure"})
        async with app.run_test() as pilot:
            await pilot.pause()
            table = app.query_one("#table", DataTable)
            assert table.row_count == 1  # one failure in the fixture

    async def test_clear_filters_restores_all(self, tmp_cache):
        app = AuditLogApp(tmp_cache, initial_filters={"outcome": "failure"})
        async with app.run_test() as pilot:
            await pilot.pause()
            app.action_clear_filters()
            await pilot.pause()
            table = app.query_one("#table", DataTable)
            assert table.row_count == 5

    async def test_export_callback_writes_file(self, tmp_cache, tmp_path):
        out = tmp_path / "export.csv"
        app = AuditLogApp(tmp_cache)
        async with app.run_test() as pilot:
            await pilot.pause()
            app._do_export(("csv", str(out)))
            await pilot.pause()
        assert out.exists()
        # Header + 5 data rows.
        assert len(out.read_text().strip().splitlines()) == 6

    async def test_save_filters_callback_writes_file(self, tmp_cache, tmp_path):
        out = tmp_path / "filters.json"
        app = AuditLogApp(tmp_cache, initial_filters={"category": "user-access"})
        async with app.run_test() as pilot:
            await pilot.pause()
            app._do_save_filters(str(out))
            await pilot.pause()
        saved = load_filters_file(str(out))
        assert saved == {"category": "user-access"}

    async def test_row_selected_opens_detail(self, tmp_cache):
        app = AuditLogApp(tmp_cache)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.query_one("#table", DataTable).focus()
            await pilot.press("enter")
            await pilot.pause()
            # A DetailScreen modal should now be on top of the stack.
            assert app.screen.__class__.__name__ == "DetailScreen"
            await pilot.press("escape")
            await pilot.pause()
            assert app.screen.__class__.__name__ != "DetailScreen"
