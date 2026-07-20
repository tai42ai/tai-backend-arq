"""App lifecycle hooks: close the shared ArqRedis pool at shutdown."""

from __future__ import annotations

from tai_contract.app import tai_app

from tai_backend_arq.pool import RedisPoolManager


@tai_app.lifecycle.on_shutdown
async def close_arq_pool() -> None:
    await RedisPoolManager.close()
