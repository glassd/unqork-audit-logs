"""Tests for the audit logs HTTP client: retries and download resilience."""

from __future__ import annotations

import httpx
import pytest

from unqork_audit_logs.auth import TokenManager
from unqork_audit_logs.client import APIError, AuditLogClient
from unqork_audit_logs.config import Settings


def make_settings(**overrides) -> Settings:
    base = dict(
        base_url="https://test.unqork.io",
        client_id="cid",
        client_secret="secret",
        max_retries=3,
        retry_backoff_base=0.0,  # no real sleeping in tests
    )
    base.update(overrides)
    return Settings(**base)


def token_response() -> httpx.Response:
    return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip the backoff sleeps so retry tests run instantly."""
    async def _instant(_seconds):
        return None

    monkeypatch.setattr("unqork_audit_logs.client.asyncio.sleep", _instant)


async def _run(handler, fn):
    """Run a client coroutine against a MockTransport-backed AsyncClient."""
    settings = make_settings()
    token_manager = TokenManager(settings)
    api = AuditLogClient(settings, token_manager)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        return await fn(api, client)


class TestRetry:
    async def test_retries_then_succeeds_on_503(self):
        calls = {"download": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("access_token"):
                return token_response()
            calls["download"] += 1
            if calls["download"] < 3:
                return httpx.Response(503, text="busy")
            return httpx.Response(200, content=b"ok")

        result = await _run(
            handler, lambda api, c: api.download_log_file(c, "https://files/x")
        )
        assert result == b"ok"
        assert calls["download"] == 3  # two failures, one success

    async def test_gives_up_after_max_retries(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("access_token"):
                return token_response()
            return httpx.Response(503, text="busy")

        with pytest.raises(APIError):
            await _run(
                handler, lambda api, c: api.download_log_file(c, "https://files/x")
            )

    async def test_non_retryable_status_raises_immediately(self):
        calls = {"download": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("access_token"):
                return token_response()
            calls["download"] += 1
            return httpx.Response(404, text="nope")

        with pytest.raises(APIError):
            await _run(
                handler, lambda api, c: api.download_log_file(c, "https://files/x")
            )
        assert calls["download"] == 1  # 404 is not retried


class TestDownloadResilience:
    async def test_one_failure_does_not_abort_others(self):
        """A single permanently-failing file is reported, but the rest still
        download successfully."""
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("access_token"):
                return token_response()
            if request.url.path.endswith("/bad"):
                return httpx.Response(500, text="boom")
            return httpx.Response(200, content=b"data")

        urls = [
            "https://files/a",
            "https://files/bad",
            "https://files/c",
        ]
        results, failed = await _run(
            handler, lambda api, c: api.download_log_files(c, urls)
        )
        assert failed == 1
        assert results == [b"data", b"data"]  # the two good files survived

    async def test_all_succeed(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("access_token"):
                return token_response()
            return httpx.Response(200, content=b"data")

        urls = ["https://files/a", "https://files/b"]
        results, failed = await _run(
            handler, lambda api, c: api.download_log_files(c, urls)
        )
        assert failed == 0
        assert len(results) == 2
