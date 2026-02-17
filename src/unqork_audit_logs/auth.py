"""OAuth 2.0 Client Credentials authentication for the Unqork API."""

from __future__ import annotations

import time

import httpx

from unqork_audit_logs.config import Settings


class AuthError(Exception):
    """Raised when authentication fails."""


class TokenManager:
    """Manages OAuth 2.0 access tokens with automatic refresh.

    Tokens are obtained via the Client Credentials Grant and cached in memory.
    A new token is fetched when the current one is expired or approaching expiry.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._access_token: str | None = None
        self._expires_at: float = 0.0  # Unix timestamp

    @property
    def is_token_valid(self) -> bool:
        """Check if the current token is valid and not approaching expiry."""
        if self._access_token is None:
            return False
        buffer = self._settings.token_refresh_buffer_seconds
        return time.time() < (self._expires_at - buffer)

    async def get_token(self, client: httpx.AsyncClient) -> str:
        """Get a valid access token, refreshing if necessary.

        Args:
            client: An httpx AsyncClient to use for the token request.

        Returns:
            A valid Bearer access token string.

        Raises:
            AuthError: If the token request fails.
        """
        if self.is_token_valid:
            return self._access_token  # type: ignore[return-value]

        await self._fetch_token(client)
        return self._access_token  # type: ignore[return-value]

    async def _fetch_token(self, client: httpx.AsyncClient) -> None:
        """Fetch a new access token from the Unqork OAuth endpoint.

        Uses HTTP Basic authentication with client_id:client_secret
        and grant_type=client_credentials.
        """
        try:
            response = await client.post(
                self._settings.token_url,
                auth=(self._settings.client_id, self._settings.client_secret),
                data={"grant_type": "client_credentials"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise AuthError(
                f"Authentication failed (HTTP {e.response.status_code}): "
                f"{e.response.text}"
            ) from e
        except httpx.RequestError as e:
            raise AuthError(f"Authentication request failed: {e}") from e

        data = response.json()

        self._access_token = data.get("access_token")
        if not self._access_token:
            raise AuthError(
                "Authentication response did not contain an access_token. "
                f"Response: {data}"
            )

        # Default to 3600 seconds (1 hour) if not specified
        expires_in = data.get("expires_in", 3600)
        self._expires_at = time.time() + expires_in

    def invalidate(self) -> None:
        """Force invalidation of the current token."""
        self._access_token = None
        self._expires_at = 0.0
