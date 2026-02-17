# unqork-audit-logs

A CLI tool to fetch, view, search, and export audit logs from the [Unqork](https://www.unqork.com/) platform.

Unqork provides an API for retrieving audit logs but no built-in interface for viewing them. This tool bridges that gap with a terminal-based dashboard that handles the quirks of the API (1-hour request windows, paginated file downloads, compressed NDJSON responses) and provides fast local querying, filtering, and export.

## Features

- **Fetch** audit logs from the Unqork API with automatic 1-hour window splitting, concurrent file downloads, and progress tracking
- **Cache** fetched logs locally in SQLite for instant re-querying without hitting the API
- **Filter** by date range, category, action, actor, outcome, source, IP, or free-text search
- **View** entries in a color-coded terminal table or a detailed JSON panel
- **Export** to JSON, JSONL, or CSV (JSON export preserves the original API response exactly)
- **Summarize** with breakdowns by category, action, actor, IP, and failure analysis
- **Incremental fetching** -- already-fetched time windows are skipped automatically

## Prerequisites

- Python 3.11+
- An Unqork environment with Designer Administrator access
- OAuth 2.0 client credentials (created in Unqork's API Access Management)

## Installation

```bash
git clone https://github.com/glassd/unqork-audit-logs.git
cd unqork-audit-logs
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

The `requirements.txt` installs all dependencies. The `pip install -e .` step registers the `unqork-logs` command on your PATH.

If you just want the dependencies without the CLI shortcut, `pip install -r requirements.txt` is sufficient and you can run the tool with:

```bash
python -m unqork_audit_logs --help
```

## Configuration

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```
UNQORK_BASE_URL=https://yourenv.unqork.io
UNQORK_CLIENT_ID=your-client-id
UNQORK_CLIENT_SECRET=your-client-secret
```

Verify everything is configured correctly:

```bash
unqork-logs config check
```

## Usage

### Fetching logs

Pull logs from the API and store them in the local cache. The tool automatically splits your date range into 1-hour windows (the API's maximum) and downloads all log files concurrently.

```bash
# Fetch the last 24 hours
unqork-logs fetch --last 24h

# Fetch a specific date range
unqork-logs fetch --start 2025-02-17 --end 2025-02-18

# Fetch with specific times
unqork-logs fetch --start "2025-02-17 09:00" --end "2025-02-17 15:00"
```

Re-running a fetch for the same time range is fast -- windows already in the cache are skipped.

### Listing and filtering

Query the local cache. All filters are optional and can be combined.

```bash
# List all cached entries
unqork-logs list

# Filter by category
unqork-logs list --category user-access

# Filter by action
unqork-logs list --action designer-user-login

# Filter by actor (email)
unqork-logs list --actor alice@company.com

# Filter by outcome
unqork-logs list --outcome failure

# Filter by client IP
unqork-logs list --ip 10.0.0.1

# Free-text search across all fields
unqork-logs list --search "delete"

# Combine filters
unqork-logs list --category user-access --outcome failure --last 7d

# Pagination
unqork-logs list --limit 50 --offset 100
```

### Viewing a single entry

Show the full detail of a log entry, including syntax-highlighted JSON. You can use the full ID or just a prefix.

```bash
unqork-logs show fc64fa7da8
```

### Exporting

Export filtered results to a file. JSON export preserves the original Unqork API response format exactly.

```bash
# Export to JSON (default)
unqork-logs export -o audit_report.json

# Export to CSV (flattened columns)
unqork-logs export --format csv -o audit_report.csv

# Export to JSONL (one entry per line)
unqork-logs export --format jsonl -o audit_report.jsonl

# Export with filters
unqork-logs export --format csv -o failures.csv --outcome failure --last 30d

# Export to stdout
unqork-logs export --format jsonl | jq '.action'
```

### Summary statistics

Get a high-level overview of audit activity.

```bash
# Summarize all cached data
unqork-logs summary

# Summarize a specific time range
unqork-logs summary --last 7d
unqork-logs summary --start 2025-02-01 --end 2025-02-28
```

The summary includes:
- Total events, success/failure counts, and success rate
- Events by category
- Top 20 actions
- Top 20 most active users
- Top 10 client IPs
- Failure analysis (which actions are failing)

### Managing the cache

```bash
# View cache statistics
unqork-logs cache info

# List all fetched time windows
unqork-logs cache windows

# Clear the cache
unqork-logs cache clear
```

## How it works

The Unqork audit logs API (`GET /api/1.0/logs/audit-logs`) accepts a maximum 1-hour time window per request and returns a list of signed URLs pointing to compressed NDJSON files. This tool handles the complexity:

```
1. Authenticate    POST /api/1.0/oauth2/access_token (OAuth 2.0 Client Credentials)
2. Split range     User's date range --> 1-hour windows
3. Check cache     Skip windows that have already been fetched
4. Fetch URLs      GET /api/1.0/logs/audit-logs?startDatetime=...&endDatetime=...
5. Download files  Concurrent download of all file URLs (up to ~80 per window)
6. Decompress      Each file is gzip-compressed NDJSON
7. Parse           Each line is a JSON audit log entry
8. Cache           Store in SQLite with indexed columns for fast querying
```

The OAuth token (1-hour lifetime) is automatically refreshed when it approaches expiry during long fetches.

Fetched data is stored in `~/.unqork-logs/cache.db` by default. Set `UNQORK_DATA_DIR` to change the location.

## Audit log categories

Unqork organizes audit logs into four categories:

| Category | Examples |
|---|---|
| **User Access & Security** | Logins, logouts, password changes, role changes, SSO configuration, lockouts |
| **Access Management** | Creator/Express user and role administration, group management |
| **Configuration** | Module saves, service changes, style updates, template uploads, environment settings |
| **Data Access** | Submission CRUD operations, data model record access, workflow submissions |

See the [Unqork Audit Logs documentation](https://docs.unqork.io/docs/audit-logs) for the full reference.

## Project structure

```
src/unqork_audit_logs/
    cli.py          CLI entry point (Typer)
    config.py       Environment variable loading and validation
    auth.py         OAuth 2.0 client credentials with automatic token refresh
    client.py       Async HTTP client for the audit logs API
    fetcher.py      Orchestrates multi-hour fetches with progress tracking
    parser.py       Decompresses and parses NDJSON log files
    models.py       Pydantic models for audit log entries
    cache.py        SQLite storage with indexed querying
    filters.py      Filter parameter construction
    display.py      Rich terminal output (tables, panels, progress bars)
    export.py       CSV, JSON, JSONL export
    summary.py      Statistics and analytics

tests/
    test_models.py, test_parser.py, test_cache.py,
    test_fetcher.py, test_filters.py, test_export.py
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=unqork_audit_logs
```

## License

MIT
