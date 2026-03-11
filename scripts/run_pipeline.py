from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import Settings
from logging_setup import setup_logging
from pipeline.pipeline import FilePipeline


def main() -> None:
    settings = Settings.from_env()
    logger = setup_logging(settings.pipeline.log_file, logger_name="s3_pipeline")
    pipeline = FilePipeline(settings=settings, logger=logger)

    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by KeyboardInterrupt.")


if __name__ == "__main__":
    main()
