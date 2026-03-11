from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from watchdog.events import FileMovedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver


class DebouncedFileEventHandler(FileSystemEventHandler):
    """Pushes file events into an asyncio queue with simple debounce."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue[Path | None],
        logger: logging.Logger,
        debounce_seconds: float = 0.8,
    ) -> None:
        super().__init__()
        self._loop = loop
        self._queue = queue
        self._logger = logger
        self._debounce_seconds = debounce_seconds
        self._last_seen: dict[Path, float] = {}

    def _enqueue_path(self, path: str, is_directory: bool) -> None:
        if is_directory:
            return

        file_path = Path(path).resolve()
        now = time.monotonic()
        last_seen = self._last_seen.get(file_path)
        if last_seen is not None and (now - last_seen) < self._debounce_seconds:
            return

        self._last_seen[file_path] = now
        self._loop.call_soon_threadsafe(self._queue.put_nowait, file_path)
        self._logger.debug("Queued file event for processing: %s", file_path)

    def on_created(self, event: FileSystemEvent) -> None:
        self._enqueue_path(path=event.src_path, is_directory=event.is_directory)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._enqueue_path(path=event.src_path, is_directory=event.is_directory)

    def on_moved(self, event: FileMovedEvent) -> None:
        self._enqueue_path(path=event.dest_path, is_directory=event.is_directory)


class DirectoryWatcher:
    """Wrapper around watchdog observer."""

    def __init__(
        self,
        watch_dir: Path,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue[Path | None],
        logger: logging.Logger,
        debounce_seconds: float = 0.8,
        use_polling: bool = False,
    ) -> None:
        self.watch_dir = watch_dir
        self.logger = logger
        self.observer = PollingObserver() if use_polling else Observer()
        self.handler = DebouncedFileEventHandler(
            loop=loop,
            queue=queue,
            logger=logger,
            debounce_seconds=debounce_seconds,
        )

    def start(self) -> None:
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.observer.schedule(self.handler, str(self.watch_dir), recursive=False)
        self.observer.start()
        self.logger.info("Watcher started for directory: %s", self.watch_dir)

    def stop(self) -> None:
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join(timeout=5)
            self.logger.info("Watcher stopped.")
