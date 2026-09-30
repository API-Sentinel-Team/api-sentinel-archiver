"""The archiver executes durable archive jobs: success, retry, exhaustion and crash recovery."""
import datetime

import pytest

from sentinel_archiver.modules.storage.job_runner import ArchiveJobRunner
from sentinel_core.modules.persistence.database import AsyncSessionLocal
from sentinel_core.modules.storage import archive_jobs as jobs


async def _submit(account_id: int, **kwargs):
    async with AsyncSessionLocal() as db:
        job, _ = await jobs.submit_archive_job(db, account_id=account_id, **kwargs)
        return job.id


async def _load(account_id: int, job_id: str):
    async with AsyncSessionLocal() as db:
        return await jobs.get_archive_job(db, account_id=account_id, job_id=job_id)


@pytest.mark.asyncio
async def test_a_queued_job_is_executed_for_its_own_tenant_and_completed(test_engine):
    job_id = await _submit(201)
    seen = []

    async def executor(account_id):
        seen.append(account_id)
        return {"status": "ok", "archived": {"request_logs": 3}}

    summary = await ArchiveJobRunner(worker_id="a1", executor=executor).run_once()

    assert seen == [201]
    assert summary["outcome"] == "COMPLETED"
    job = await _load(201, job_id)
    assert job.status == jobs.COMPLETED and job.result["archived"]["request_logs"] == 3


@pytest.mark.asyncio
async def test_the_runner_is_idle_when_nothing_is_queued(test_engine):
    async def executor(account_id):
        raise AssertionError("must not run")

    assert await ArchiveJobRunner(worker_id="a1", executor=executor).run_once() is None


@pytest.mark.asyncio
async def test_a_failing_archive_is_recorded_and_left_for_retry(test_engine):
    job_id = await _submit(202, max_attempts=2)

    async def executor(account_id):
        raise RuntimeError("object storage unreachable")

    summary = await ArchiveJobRunner(worker_id="a1", executor=executor).run_once()

    assert summary["outcome"] == jobs.PENDING
    job = await _load(202, job_id)
    assert job.status == jobs.PENDING and job.attempts == 1
    assert "unreachable" in job.error and job.next_attempt_at is not None


@pytest.mark.asyncio
async def test_a_job_that_keeps_failing_ends_up_failed(test_engine):
    job_id = await _submit(203, max_attempts=1)

    async def executor(account_id):
        raise RuntimeError("still broken")

    summary = await ArchiveJobRunner(worker_id="a1", executor=executor).run_once()

    assert summary["outcome"] == jobs.FAILED
    assert (await _load(203, job_id)).status == jobs.FAILED


@pytest.mark.asyncio
async def test_a_second_archiver_does_not_run_a_job_the_first_holds(test_engine):
    await _submit(204)
    async with AsyncSessionLocal() as db:
        await jobs.claim_next_archive_job(db, worker_id="crashed-archiver")

    async def executor(account_id):
        raise AssertionError("the lease is still valid, so this job is not theirs to run")

    assert await ArchiveJobRunner(worker_id="a2", executor=executor).run_once() is None


@pytest.mark.asyncio
async def test_a_job_abandoned_by_a_crashed_archiver_is_picked_up_after_its_lease_expires(test_engine):
    job_id = await _submit(205)
    long_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    async with AsyncSessionLocal() as db:
        # The first archiver claimed it an hour ago with a 60s lease and then died.
        await jobs.claim_next_archive_job(db, worker_id="crashed-archiver", lease_seconds=60, now=long_ago)
    ran = []

    async def executor(account_id):
        ran.append(account_id)
        return {"status": "ok", "archived": {}}

    summary = await ArchiveJobRunner(worker_id="a2", executor=executor).run_once()

    assert ran == [205] and summary["outcome"] == "COMPLETED"
    job = await _load(205, job_id)
    assert job.status == jobs.COMPLETED and job.attempts == 2 and job.worker_id == "a2"
