"""Configuration management for Unqork Audit Logs CLI."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, field_validator


# Default data directory for cache and local storage
DEFAULT_DATA_DIR = Path.home() / ".unqork-logs"

# API path constants
API_BASE_PATH = "/api/1.0"
TOKEN_PATH = f"{API_BASE_PATH}/oauth2/access_token"
AUDIT_LOGS_PATH = f"{API_BASE_PATH}/logs/audit-logs"


class Settings(BaseModel):
    """Application settings loaded from environment variables."""

    base_url: str
    client_id: str
    client_secret: str
    data_dir: Path = DEFAULT_DATA_DIR

    # Concurrency settings for file downloads
    max_concurrent_downloads: int = 15

    # Token refresh buffer - re-authenticate when this many seconds remain
    token_refresh_buffer_seconds: int = 300  # 5 minutes

    # SSL verification - set to False for self-signed certificates
    verify_ssl: bool = True

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Strip trailing slashes and validate URL format."""
        v = v.rstrip("/")
        if not v.startswith("https://"):
            raise ValueError("UNQORK_BASE_URL must start with https://")
        return v

    @property
    def token_url(self) -> str:
        return f"{self.base_url}{TOKEN_PATH}"

    @property
    def audit_logs_url(self) -> str:
        return f"{self.base_url}{AUDIT_LOGS_PATH}"

    @property
    def cache_db_path(self) -> Path:
        return self.data_dir / "cache.db"


def load_settings() -> Settings:
    """Load settings from environment variables.

    Looks for a .env file in the current directory and parent directories.
    Environment variables take precedence over .env file values.

    Raises:
        ValueError: If required environment variables are missing.
    """
    load_dotenv()

    base_url = os.getenv("UNQORK_BASE_URL")
    client_id = os.getenv("UNQORK_CLIENT_ID")
    client_secret = os.getenv("UNQORK_CLIENT_SECRET")

    missing = []
    if not base_url:
        missing.append("UNQORK_BASE_URL")
    if not client_id:
        missing.append("UNQORK_CLIENT_ID")
    if not client_secret:
        missing.append("UNQORK_CLIENT_SECRET")

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them in a .env file or export them in your shell. "
            f"See .env.example for reference."
        )

    data_dir = Path(os.getenv("UNQORK_DATA_DIR", str(DEFAULT_DATA_DIR)))
    verify_ssl = os.getenv("UNQORK_VERIFY_SSL", "true").lower() not in ("false", "0", "no")

    return Settings(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
        data_dir=data_dir,
        verify_ssl=verify_ssl,
    )
