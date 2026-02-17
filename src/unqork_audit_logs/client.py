"""HTTP client for the Unqork Audit Logs API.

Handles fetching audit log file locations for a 1-hour window
and downloading individual log files (compressed NDJSON).
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from unqork_audit_logs.auth import AuthError, TokenManager
from unqork_audit_logs.config import Settings

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Raised when an API request fails."""


class AuditLogClient:
    """Async HTTP client for the Unqork audit logs API.

    Manages authenticated requests including:
    - Fetching log file locations for a given 1-hour window
    - Downloading individual compressed log files concurrently
    """

    def __init__(self, settings: Settings, token_manager: TokenManager) -> None:
        self._settings = settings
        self._token_manager = token_manager

    async def _get_auth_header(self, client: httpx.AsyncClient) -> dict[str, str]:
        """Get the Authorization header with a valid Bearer token."""
        token = await self._token_manager.get_token(client)
        return {"Authorization": f"Bearer {token}"}

    async def fetch_log_locations(
        self,
        client: httpx.AsyncClient,
        start_datetime: str,
        end_datetime: str,
    ) -> list[str]:
        """Fetch audit log file URLs for a 1-hour (max) time window.

        Args:
            client: The httpx AsyncClient to use.
            start_datetime: ISO 8601 UTC datetime string (e.g. '2023-05-17T15:00:00.000Z').
            end_datetime: ISO 8601 UTC datetime string (e.g. '2023-05-17T16:00:00.000Z').

        Returns:
            List of URLs pointing to compressed log files.

        Raises:
            APIError: If the request fails.
        """
        headers = await self._get_auth_header(client)
        params = {
            "startDatetime": start_datetime,
            "endDatetime": end_datetime,
        }

        try:
            response = await client.get(
                self._settings.audit_logs_url,
                headers=headers,
                params=params,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            # If 401, try refreshing token once and retry
            if e.response.status_code == 401:
                logger.debug("Got 401, refreshing token and retrying")
                self._token_manager.invalidate()
                headers = await self._get_auth_header(client)
                try:
                    response = await client.get(
                        self._settings.audit_logs_url,
                        headers=headers,
                        params=params,
                    )
                    response.raise_for_status()
                except httpx.HTTPStatusError as retry_err:
                    raise APIError(
                        f"API request failed after token refresh "
                        f"(HTTP {retry_err.response.status_code}): "
                        f"{retry_err.response.text}"
                    ) from retry_err
            else:
                raise APIError(
                    f"API request failed (HTTP {e.response.status_code}): "
                    f"{e.response.text}"
                ) from e
        except httpx.RequestError as e:
            raise APIError(f"API request failed: {e}") from e

        data = response.json()
        locations = data.get("logLocations", [])
        logger.debug(
            "Got %d log file locations for window %s - %s",
            len(locations),
            start_datetime,
            end_datetime,
        )
        return locations

    async def download_log_file(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> bytes:
        """Download a single compressed log file.

        Args:
            client: The httpx AsyncClient to use.
            url: The signed URL to the compressed log file.

        Returns:
            Raw bytes of the compressed file.

        Raises:
            APIError: If the download fails.
        """
        headers = await self._get_auth_header(client)

        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.debug("Got 401 on file download, refreshing token and retrying")
                self._token_manager.invalidate()
                headers = await self._get_auth_header(client)
                try:
                    response = await client.get(url, headers=headers)
                    response.raise_for_status()
                except httpx.HTTPStatusError as retry_err:
                    raise APIError(
                        f"File download failed after token refresh "
                        f"(HTTP {retry_err.response.status_code})"
                    ) from retry_err
            else:
                raise APIError(
                    f"File download failed (HTTP {e.response.status_code})"
                ) from e
        except httpx.RequestError as e:
            raise APIError(f"File download failed: {e}") from e

        return response.content

    async def download_log_files(
        self,
        client: httpx.AsyncClient,
        urls: list[str],
        on_progress: callable | None = None,
    ) -> list[bytes]:
        """Download multiple log files concurrently with bounded concurrency.

        Args:
            client: The httpx AsyncClient to use.
            urls: List of signed URLs to download.
            on_progress: Optional callback called after each file completes.
                Receives (completed_count, total_count).

        Returns:
            List of raw bytes for each downloaded file, in order.
        """
        semaphore = asyncio.Semaphore(self._settings.max_concurrent_downloads)
        results: list[bytes | None] = [None] * len(urls)
        completed = 0

        async def _download_one(index: int, url: str) -> None:
            nonlocal completed
            async with semaphore:
                data = await self.download_log_file(client, url)
                results[index] = data
                completed += 1
                if on_progress:
                    on_progress(completed, len(urls))

        tasks = [_download_one(i, url) for i, url in enumerate(urls)]
        await asyncio.gather(*tasks)

        return [r for r in results if r is not None]
