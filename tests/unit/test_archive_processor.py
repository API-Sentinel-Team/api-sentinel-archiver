import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from sentinel_archiver.modules.storage.archive_processor import ArchiveProcessor
from sentinel_core.models.core import Account


@pytest.mark.asyncio
async def test_archive_processor_all_tenants_mode_runs_each_account(
    db_session,
    test_engine,
    monkeypatch,
):
    session_factory = async_sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr("sentinel_archiver.modules.storage.archive_processor.AsyncSessionLocal", session_factory)

    db_session.add_all(
        [
            Account(id=3001, name="Acme"),
            Account(id=3002, name="Bravo"),
        ]
    )
    await db_session.commit()

    seen: list[int] = []

    async def fake_archive_once(account_id: int):
        seen.append(account_id)
        return {"status": "ok"}

    monkeypatch.setattr("sentinel_archiver.modules.storage.archive_processor.archive_once", fake_archive_once)

    processor = ArchiveProcessor(interval_sec=3600, account_id=0)
    await processor._process_once()

    filtered_seen = [account_id for account_id in seen if account_id in {3001, 3002}]
    assert filtered_seen == [3001, 3002]
