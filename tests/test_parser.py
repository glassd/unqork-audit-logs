"""Tests for the log file parser (decompress + NDJSON parsing)."""

import gzip
import json
import zipfile
import io

from unqork_audit_logs.parser import decompress, parse_ndjson, parse_log_file, parse_log_files
from tests.conftest import make_entry_dict, make_compressed_ndjson


class TestDecompress:
    def test_gzip(self):
        original = "hello world"
        compressed = gzip.compress(original.encode("utf-8"))
        assert decompress(compressed) == original

    def test_zip(self):
        original = "hello world"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("test.json", original)
        assert decompress(buf.getvalue()) == original

    def test_raw_json(self):
        raw = '{"key": "value"}'
        assert decompress(raw.encode("utf-8")) == raw

    def test_invalid_data(self):
        import pytest
        with pytest.raises(ValueError, match="Unable to decompress"):
            decompress(b"\x00\x01\x02\x03")


class TestParseNdjson:
    def test_standard_ndjson(self):
        text = '{"a": 1}\n{"a": 2}\n{"a": 3}'
        result = parse_ndjson(text)
        assert len(result) == 3
        assert result[0]["a"] == 1

    def test_empty_lines(self):
        text = '{"a": 1}\n\n{"a": 2}\n\n'
        result = parse_ndjson(text)
        assert len(result) == 2

    def test_json_array(self):
        text = '[{"a": 1}, {"a": 2}]'
        result = parse_ndjson(text)
        assert len(result) == 2

    def test_empty_string(self):
        assert parse_ndjson("") == []
        assert parse_ndjson("   ") == []

    def test_malformed_line_skipped(self):
        text = '{"a": 1}\n{bad json\n{"a": 3}'
        result = parse_ndjson(text)
        assert len(result) == 2


class TestParseLogFile:
    def test_compressed_ndjson(self):
        entries_raw = [make_entry_dict(0), make_entry_dict(1)]
        compressed = make_compressed_ndjson(entries_raw)
        parsed = parse_log_file(compressed)
        assert len(parsed) == 2
        assert parsed[0].entry.action == "designer-user-login"
        assert parsed[1].entry.action == "designer-user-login"
        # raw_json should be the original dict, not Pydantic's serialization
        assert '"priorAttributes"' not in parsed[0].raw_json

    def test_diverse_entries(self):
        entries_raw = [
            make_entry_dict(0, action="designer-user-login"),
            make_entry_dict(1, action="save-module-update", category="configuration"),
        ]
        compressed = make_compressed_ndjson(entries_raw)
        parsed = parse_log_file(compressed)
        assert parsed[0].entry.action == "designer-user-login"
        assert parsed[1].entry.action == "save-module-update"
        assert parsed[1].entry.category == "configuration"


class TestParseLogFiles:
    def test_multiple_files(self):
        file1 = make_compressed_ndjson([make_entry_dict(0)])
        file2 = make_compressed_ndjson([make_entry_dict(1), make_entry_dict(2)])
        entries = parse_log_files([file1, file2])
        assert len(entries) == 3

    def test_empty_list(self):
        assert parse_log_files([]) == []
