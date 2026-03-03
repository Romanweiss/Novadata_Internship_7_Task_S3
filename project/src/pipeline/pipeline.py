from __future__ import annotations

import asyncio
import logging
import shutil
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from config import Settings
from pipeline.processor import FileReadRetryError, SUPPORTED_EXTENSIONS, process_file
from pipeline.uploader_async import AsyncS3Uploader
from pipeline.watcher import DirectoryWatcher


async def wait_until_file_ready(
    file_path: Path,
    logger: logging.Logger,
    check_interval_seconds: float = 0.5,
    max_attempts: int = 20,
    max_wait_seconds: float = 12.0,
) -> bool:
    """
    Wait until file exists, has size > 0, and has stable size in two checks.
    """

    logger.info("Waiting for file to be ready: %s", file_path)
    started_at = time.monotonic()
    previous_size: int | None = None

    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - started_at
        if elapsed > max_wait_seconds:
            logger.warning(
                "File readiness timeout for %s after %.2fs and %s attempts.",
                file_path,
                elapsed,
                attempt - 1,
            )
            return False

        if not file_path.exists():
            logger.debug(
                "Waiting for file to be ready ... attempt %s/%s: file does not exist yet.",
                attempt,
                max_attempts,
            )
            previous_size = None
            await asyncio.sleep(check_interval_seconds)
            continue

        try:
            current_size = file_path.stat().st_size
        except OSError:
            logger.debug(
                "Waiting for file to be ready ... attempt %s/%s: stat failed.",
                attempt,
                max_attempts,
            )
            previous_size = None
            await asyncio.sleep(check_interval_seconds)
            continue

        if current_size <= 0:
            logger.debug(
                "Waiting for file to be ready ... attempt %s/%s: size=%s bytes.",
                attempt,
                max_attempts,
                current_size,
            )
            previous_size = current_size
            await asyncio.sleep(check_interval_seconds)
            continue

        logger.debug(
            "Waiting for file to be ready ... attempt %s/%s: current size=%s bytes, "
            "previous size=%s.",
            attempt,
            max_attempts,
            current_size,
            previous_size,
        )
        if previous_size is not None and current_size == previous_size:
            logger.info("File ready: size=%s bytes, path=%s", current_size, file_path)
            return True

        previous_size = current_size
        await asyncio.sleep(check_interval_seconds)

    elapsed = time.monotonic() - started_at
    logger.warning(
        "File readiness timeout for %s after %.2fs and %s attempts.",
        file_path,
        elapsed,
        max_attempts,
    )
    return False


class FilePipeline:
    """Watch local folder, process files, upload result and logs to S3."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger

        self.watch_dir = settings.pipeline.watch_dir
        self.archive_dir = settings.pipeline.archive_dir
        self.failed_archive_dir = self.archive_dir / "failed"
        self.tmp_dir = self.watch_dir.parent / "tmp"

        self._queue: asyncio.Queue[Path | None] = asyncio.Queue()
        self._shutdown_event = asyncio.Event()
        self._in_progress: set[Path] = set()
        self._watcher: DirectoryWatcher | None = None

        self._uploader = AsyncS3Uploader(
            endpoint=settings.s3.endpoint,
            access_key=settings.s3.access_key,
            secret_key=settings.s3.secret_key,
            bucket=settings.s3.bucket,
            region=settings.s3.region,
            verify_ssl=settings.s3.verify_ssl,
            ca_bundle=settings.s3.ca_bundle,
            logger=logger,
        )

    async def run(self) -> None:
        self._prepare_directories()
        loop = asyncio.get_running_loop()
        self._configure_signal_handlers(loop)

        self._watcher = DirectoryWatcher(
            watch_dir=self.watch_dir,
            loop=loop,
            queue=self._queue,
            logger=self.logger,
            debounce_seconds=0.8,
            use_polling=self.settings.pipeline.watch_use_polling,
        )
        self._watcher.start()
        await self._enqueue_existing_files()

        worker_task = asyncio.create_task(self._worker(), name="pipeline-worker")
        self.logger.info("Pipeline started. Waiting for new files in %s", self.watch_dir)
        try:
            await self._shutdown_event.wait()
        finally:
            await self._stop(worker_task)

    def request_shutdown(self) -> None:
        if not self._shutdown_event.is_set():
            self.logger.info("Shutdown requested.")
            self._shutdown_event.set()

    def _prepare_directories(self) -> None:
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.failed_archive_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.settings.pipeline.log_file.parent.mkdir(parents=True, exist_ok=True)

    def _configure_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except (NotImplementedError, RuntimeError, ValueError):
                continue

    async def _enqueue_existing_files(self) -> None:
        for file_path in self.watch_dir.iterdir():
            if file_path.is_file():
                await self._queue.put(file_path.resolve())
                self.logger.info("Queued existing file on startup: %s", file_path)

    async def _worker(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                await self._handle_file(item)
            finally:
                self._queue.task_done()

    async def _handle_file(self, file_path: Path) -> None:
        if not file_path.exists():
            self.logger.debug("Skipping missing file event: %s", file_path)
            return

        suffix = file_path.suffix.lower()
        if suffix not in SUPPORTED_EXTENSIONS:
            self.logger.info("Unsupported file extension, ignored: %s", file_path)
            return

        resolved = file_path.resolve()
        if resolved in self._in_progress:
            self.logger.debug("Duplicate event skipped for file in progress: %s", resolved)
            return

        self._in_progress.add(resolved)
        processed_path: Path | None = None
        try:
            ready = await wait_until_file_ready(
                file_path=resolved,
                logger=self.logger,
                check_interval_seconds=0.5,
                max_attempts=24,
                max_wait_seconds=12.0,
            )
            if not ready:
                self.logger.warning("File readiness timed out, skipping for now: %s", resolved)
                return

            processed_path = process_file(
                input_path=resolved,
                output_dir=self.tmp_dir,
                logger=self.logger,
            )
            object_key = self.settings.s3.build_object_key(processed_path.name)
            await self._uploader.upload_file(processed_path, object_key)

            archived_path = self._archive_file(resolved)
            self.logger.info("Source file moved to archive: %s", archived_path)
        except FileReadRetryError:
            failed_path = self._move_to_failed_archive(resolved)
            if failed_path is None:
                self.logger.error(
                    "CSV read retries exhausted for %s. File was not uploaded and could not "
                    "be moved to failed archive.",
                    resolved,
                )
            else:
                self.logger.error(
                    "CSV read retries exhausted for %s. File was not uploaded and moved to "
                    "failed archive: %s",
                    resolved,
                    failed_path,
                )
        except Exception:
            self.logger.exception("Failed to process file: %s", resolved)
        finally:
            self._in_progress.discard(resolved)
            if processed_path and processed_path.exists():
                processed_path.unlink(missing_ok=True)
            await self._upload_log_snapshot()

    def _archive_file(self, source_file: Path) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = self.archive_dir / f"{source_file.stem}_{timestamp}{source_file.suffix}"
        counter = 1
        while destination.exists():
            destination = (
                self.archive_dir
                / f"{source_file.stem}_{timestamp}_{counter}{source_file.suffix}"
            )
            counter += 1

        shutil.move(str(source_file), str(destination))
        return destination

    def _move_to_failed_archive(self, source_file: Path) -> Path | None:
        if not source_file.exists():
            return None

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = (
            self.failed_archive_dir
            / f"{source_file.stem}_{timestamp}{source_file.suffix}"
        )
        counter = 1
        while destination.exists():
            destination = (
                self.failed_archive_dir
                / f"{source_file.stem}_{timestamp}_{counter}{source_file.suffix}"
            )
            counter += 1

        shutil.move(str(source_file), str(destination))
        return destination

    async def _upload_log_snapshot(self) -> None:
        log_file = self.settings.pipeline.log_file
        if not log_file.exists():
            self.logger.warning("Log file does not exist yet: %s", log_file)
            return

        try:
            await self._uploader.upload_file(
                file_path=log_file,
                object_key=self.settings.pipeline.log_object_key,
            )
            self.logger.info(
                "Uploaded log snapshot to s3://%s/%s",
                self.settings.s3.bucket,
                self.settings.pipeline.log_object_key,
            )
        except (ClientError, OSError):
            self.logger.exception("Failed to upload pipeline log snapshot.")

    async def _stop(self, worker_task: asyncio.Task[None]) -> None:
        if self._watcher is not None:
            self._watcher.stop()

        if not worker_task.done():
            await self._queue.put(None)
            await self._queue.join()
            await worker_task

        self.logger.info("Pipeline stopped.")
