"""Readiness must respond even when extraction occupies every AnyIO worker slot."""
import asyncio

import anyio
import httpx

from ades.service.app import create_app


def test_status_does_not_wait_for_extraction_worker_slots(tmp_path, monkeypatch):
    monkeypatch.setenv("ADES_SERVICE_PREWARM_ENABLED", "false")
    app = create_app(storage_root=tmp_path)

    async def check():
        limiter = anyio.to_thread.current_default_thread_limiter()
        old_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        await limiter.acquire()
        try:
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    response = await asyncio.wait_for(client.get("/v0/status"), timeout=3)
                    assert response.status_code == 200
                    assert response.json()["service"] == "ades"
        finally:
            limiter.release()
            limiter.total_tokens = old_tokens

    asyncio.run(check())
