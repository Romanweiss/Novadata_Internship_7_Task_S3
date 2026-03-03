from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import Settings
from logging_setup import setup_logging
from s3_client import S3Client


def ensure_versioning_enabled(s3_client: S3Client) -> None:
    status = s3_client.s3.get_bucket_versioning(Bucket=s3_client.bucket).get("Status")
    if status == "Enabled":
        print("Versioning is already enabled.")
        return

    s3_client.s3.put_bucket_versioning(
        Bucket=s3_client.bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    print("Versioning has been enabled.")


def put_versions(s3_client: S3Client, object_key: str) -> list[str]:
    version_ids: list[str] = []
    for index in range(1, 4):
        body = (
            f"Version {index} generated at {datetime.now(timezone.utc).isoformat()}\n"
        ).encode("utf-8")
        response = s3_client.s3.put_object(
            Bucket=s3_client.bucket,
            Key=object_key,
            Body=body,
            ContentType="text/plain",
        )
        version_id = str(response.get("VersionId"))
        version_ids.append(version_id)
        print(f"Uploaded version {index}, VersionId={version_id}")
        time.sleep(1.0)
    return version_ids


def list_versions(s3_client: S3Client, object_key: str) -> list[dict]:
    versions: list[dict] = []
    paginator = s3_client.s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=s3_client.bucket, Prefix=object_key):
        for version in page.get("Versions", []):
            if version.get("Key") == object_key:
                versions.append(version)

    versions.sort(key=lambda item: item["LastModified"], reverse=True)
    return versions


def download_version(
    s3_client: S3Client,
    object_key: str,
    version_id: str,
    destination: Path,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    response = s3_client.s3.get_object(
        Bucket=s3_client.bucket,
        Key=object_key,
        VersionId=version_id,
    )
    content = response["Body"].read().decode("utf-8")
    destination.write_text(content, encoding="utf-8")
    return destination


def main() -> None:
    settings = Settings.from_env()
    logger = setup_logging(settings.pipeline.log_file, logger_name="demo_task2")
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

    object_key = settings.s3.build_object_key("demo/task2/versioned_demo.txt")

    try:
        ensure_versioning_enabled(s3_client)
        put_versions(s3_client, object_key)
        versions = list_versions(s3_client, object_key)
    except ClientError as error:
        print(f"S3 error during versioning demo: {error}")
        raise

    print("\nVersions returned by list_object_versions:")
    for idx, version in enumerate(versions, start=1):
        print(
            f"{idx}. VersionId={version['VersionId']} | "
            f"IsLatest={version['IsLatest']} | LastModified={version['LastModified']}"
        )

    non_latest = next((v for v in versions if not v.get("IsLatest")), None)
    if non_latest is None:
        raise RuntimeError(
            "Could not find previous version. Check if versioning is really enabled."
        )

    destination = ROOT_DIR / "downloads" / "prev_version.txt"
    downloaded_path = download_version(
        s3_client=s3_client,
        object_key=object_key,
        version_id=non_latest["VersionId"],
        destination=destination,
    )
    print(
        f"\nDownloaded previous version ({non_latest['VersionId']}) to: {downloaded_path}"
    )
    print("Downloaded content:")
    print(downloaded_path.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    main()
