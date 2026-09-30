"""Dedicated archive and retention process."""
from __future__ import annotations

import asyncio
import logging
import signal

from sentinel_core.config import settings
from sentinel_archiver.modules.storage.archive_processor import ArchiveProcessor
from sentinel_archiver.modules.storage.job_runner import ArchiveJobRunner

logger = logging.getLogger(__name__)


async def run_archiver_service() -> None:
    processor = ArchiveProcessor(
        interval_sec=max(60, int(getattr(settings, "ARCHIVE_INTERVAL_SECONDS", 3600))),
        account_id=int(getattr(settings, "STARTUP_ARCHIVER_ACCOUNT_ID", 0)),
    )
    job_runner = ArchiveJobRunner(poll_interval_sec=float(getattr(settings, "ARCHIVE_JOB_POLL_SECONDS", 5)))
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass

    await processor.start()
    await job_runner.start()  # executes archive jobs requested through the API
    try:
        await stop_event.wait()
    finally:
        await job_runner.stop()
        await processor.stop()


def main() -> None:
    asyncio.run(run_archiver_service())


if __name__ == "__main__":
    main()
