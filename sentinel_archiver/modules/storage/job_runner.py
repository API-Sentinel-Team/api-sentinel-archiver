"""Executes durable archive jobs submitted through the API.

Claims one job at a time (atomically, with a lease so a crashed archiver's job is picked up again),
runs the tenant's archive, and records the outcome. A failure is retried with exponential backoff
until the job's attempts run out.
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
from typing import Any, Awaitable, Callable

from sentinel_archiver.modules.storage.archiver import archive_once
from sentinel_core.modules.persistence.database import AsyncSessionLocal
from sentinel_core.modules.storage import archive_jobs

logger = logging.getLogger(__name__)

Executor = Callable[[int], Awaitable[dict[str, Any]]]


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class ArchiveJobRunner:
    def __init__(
        self,
        *,
        worker_id: str | None = None,
        poll_interval_sec: float = 5.0,
        lease_seconds: int = archive_jobs.DEFAULT_LEASE_SECONDS,
        executor: Executor = archive_once,
    ) -> None:
        self.worker_id = worker_id or default_worker_id()
        self.poll_interval = poll_interval_sec
        self.lease_seconds = lease_seconds
        self._executor = executor
        self._task: asyncio.Task | None = None
        self._running = False

    async def run_once(self) -> dict[str, Any] | None:
        """Claim and execute at most one job. Returns a summary, or ``None`` when nothing was runnable."""
        async with AsyncSessionLocal() as db:
            job = await archive_jobs.claim_next_archive_job(
                db, worker_id=self.worker_id, lease_seconds=self.lease_seconds
            )
            if job is None:
                return None
            job_id, account_id = job.id, job.account_id

        try:
            result = await self._executor(account_id)
        except Exception as exc:  # the job records the failure; the loop keeps serving other tenants
            logger.exception("archive_job_failed", extra={"job_id": job_id, "account_id": account_id})
            async with AsyncSessionLocal() as db:
                status = await archive_jobs.fail_archive_job(db, job_id=job_id, worker_id=self.worker_id, error=exc)
            return {"job_id": job_id, "account_id": account_id, "outcome": status or "lost"}

        async with AsyncSessionLocal() as db:
            recorded = await archive_jobs.complete_archive_job(db, job_id=job_id, worker_id=self.worker_id, result=result)
        return {"job_id": job_id, "account_id": account_id, "outcome": "COMPLETED" if recorded else "lost"}

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
                handled = await self.run_once()
            except Exception:
                logger.exception("archive_job_runner_error")
                handled = None
            if handled is None:  # idle: wait; otherwise go straight to the next job
                await asyncio.sleep(self.poll_interval)
