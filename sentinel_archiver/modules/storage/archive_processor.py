"""Periodic archive loop run by api-sentinel-archiver."""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from sentinel_core.models.core import Account
from sentinel_core.modules.persistence.database import AsyncSessionLocal
from sentinel_core.modules.storage.archiver import archive_once

logger = logging.getLogger(__name__)


class ArchiveProcessor:
    def __init__(self, interval_sec: int = 3600, account_id: int = 0):
        self.interval = interval_sec
        self.account_id = account_id
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._process_once()
            except Exception as exc:
                logger.exception("archive_processor_error: %s", exc)
            await asyncio.sleep(self.interval)

    async def _resolve_account_ids(self) -> list[int]:
        if self.account_id > 0:
            return [self.account_id]

        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Account.id).order_by(Account.id.asc()))
            return [row[0] for row in result.all()]

    async def _process_once(self) -> None:
        account_ids = await self._resolve_account_ids()
        for account_id in account_ids:
            await archive_once(account_id)
