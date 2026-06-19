"""HTTP client for the Unqork Audit Logs API.

Handles fetching audit log file locations for a 1-hour window
and downloading individual log files (compressed NDJSON).
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from unqork_audit_logs.auth import TokenManager
from unqork_audit_logs.config import Settings

logger = logging.getLogger(__name__)

# HTTP status codes that are worth retrying with backoff.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class APIError(Exception):
    """Raised when an API request fails."""


class AuditLogClient:
    """Async HTTP client for the Unqork audit logs API.

    Manages authenticated requests including:
    - Fetching log file locations for a given 1-hour window
    - Downloading individual compressed log files concurrently

    Transient failures (HTTP 429/5xx and network errors) are retried with
    exponential backoff; a 401 triggers a single token refresh and retry.
    """

    def __init__(self, settings: Settings, token_manager: TokenManager) -> None:
        self._settings = settings
        self._token_manager = token_manager

    async def _get_auth_header(self, client: httpx.AsyncClient) -> dict[str, str]:
        """Get the Authorization header with a valid Bearer token."""
        token = await self._token_manager.get_token(client)
        return {"Authorization": f"Bearer {token}"}

    async def _request_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict | None = None,
        description: str,
    ) -> httpx.Response:
        """GET ``url`` with token refresh on 401 and backoff on transient errors.

        Args:
            client: The httpx AsyncClient to use.
            url: The URL to request.
            params: Optional query parameters.
            description: Short description used in error messages.

        Returns:
            The successful httpx Response.

        Raises:
            APIError: If the request still fails after exhausting retries.
        """
        max_retries = self._settings.max_retries
        backoff_base = self._settings.retry_backoff_base
        refreshed_token = False
        last_error: str | None = None

        # attempt 0 is the initial try; up to max_retries additional attempts.
        for attempt in range(max_retries + 1):
            try:
                headers = await self._get_auth_header(client)
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as e:
                status = e.response.status_code

                # A 401 means the token is stale: refresh once and retry
                # immediately without consuming a backoff attempt.
                if status == 401 and not refreshed_token:
                    logger.debug("Got 401 on %s, refreshing token", description)
                    self._token_manager.invalidate()
                    refreshed_token = True
                    continue

                last_error = f"HTTP {status}: {e.response.text}"
                if status not in RETRYABLE_STATUS or attempt >= max_retries:
                    raise APIError(f"{description} failed ({last_error})") from e

                delay = self._retry_delay(e.response, attempt, backoff_base)
                logger.warning(
                    "%s failed (HTTP %d), retrying in %.1fs (attempt %d/%d)",
                    description, status, delay, attempt + 1, max_retries,
                )
                await asyncio.sleep(delay)
            except httpx.RequestError as e:
                last_error = str(e)
                if attempt >= max_retries:
                    raise APIError(f"{description} failed: {e}") from e
                delay = backoff_base * (2 ** attempt)
                logger.warning(
                    "%s network error (%s), retrying in %.1fs (attempt %d/%d)",
                    description, e, delay, attempt + 1, max_retries,
                )
                await asyncio.sleep(delay)

        # Unreachable in practice — the loop either returns or raises — but
        # guard against falling through.
        raise APIError(f"{description} failed: {last_error or 'unknown error'}")

    @staticmethod
    def _retry_delay(
        response: httpx.Response, attempt: int, backoff_base: float
    ) -> float:
        """Compute the backoff delay, honoring a Retry-After header if present."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass  # HTTP-date form is uncommon here; fall back to backoff.
        return backoff_base * (2 ** attempt)

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
        params = {
            "startDatetime": start_datetime,
            "endDatetime": end_datetime,
        }
        response = await self._request_with_retry(
            client,
            self._settings.audit_logs_url,
            params=params,
            description=f"Fetch log locations for {start_datetime}",
        )

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
        response = await self._request_with_retry(
            client, url, description="File download"
        )
        return response.content

    async def download_log_files(
        self,
        client: httpx.AsyncClient,
        urls: list[str],
        on_progress: callable | None = None,
    ) -> tuple[list[bytes], int]:
        """Download multiple log files concurrently with bounded concurrency.

        A single file that fails after retries does not abort the others —
        the successful downloads are still returned so the caller can store
        whatever was retrieved.

        Args:
            client: The httpx AsyncClient to use.
            urls: List of signed URLs to download.
            on_progress: Optional callback called after each file completes.
                Receives (completed_count, total_count).

        Returns:
            Tuple of (list of raw bytes for each successful download,
            number of files that failed).
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

        outcomes = await asyncio.gather(
            *(_download_one(i, url) for i, url in enumerate(urls)),
            return_exceptions=True,
        )

        failed = 0
        for index, outcome in enumerate(outcomes):
            if isinstance(outcome, Exception):
                failed += 1
                logger.warning("Failed to download file %d: %s", index, outcome)

        return [r for r in results if r is not None], failed
