from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import Settings
from logging_setup import setup_logging
from s3_client import S3Client


def main() -> None:
    settings = Settings.from_env()
    logger = setup_logging(settings.pipeline.log_file, logger_name="demo_task1")
    s3_client = S3Client(
        endpoint=settings.s3.endpoint,
        access_key=settings.s3.access_key,
        secret_key=settings.s3.secret_key,
        bucket=settings.s3.bucket,
        region=settings.s3.region,
        verify_ssl=settings.s3.verify_ssl,
        ca_bundle=settings.s3.ca_bundle,
        logger=logger,
    )

    local_dir = ROOT_DIR / "tmp"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_file = local_dir / "task1_sample.txt"
    if not local_file.exists():
        content = f"Demo file created at {datetime.now(timezone.utc).isoformat()}\n"
        local_file.write_text(content, encoding="utf-8")

    object_key = settings.s3.build_object_key("demo/task1/task1_sample.txt")
    s3_client.upload(local_file, object_key)

    prefix = settings.s3.build_object_key("demo/task1/")
    keys = s3_client.list_files(prefix=prefix)

    print(f"Objects for prefix '{prefix}':")
    for key in keys:
        print(f" - {key}")

    print(f"file_exists('{object_key}') -> {s3_client.file_exists(object_key)}")
    missing_key = settings.s3.build_object_key("demo/task1/missing_file.txt")
    print(f"file_exists('{missing_key}') -> {s3_client.file_exists(missing_key)}")


if __name__ == "__main__":
    main()
