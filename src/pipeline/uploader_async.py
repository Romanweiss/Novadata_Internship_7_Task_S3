from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import aioboto3
from botocore.client import Config
from botocore.exceptions import ClientError


class AsyncS3Uploader:
    """Async uploader built on top of aioboto3."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
        verify_ssl: bool = True,
        ca_bundle: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.region = region
        self.verify_ssl = verify_ssl
        self.ca_bundle = ca_bundle
        self.logger = logger or logging.getLogger(__name__)
        self._session = aioboto3.Session()

    async def upload_file(self, file_path: str | Path, object_key: str) -> None:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Local file does not exist: {path}")

        payload = await asyncio.to_thread(path.read_bytes)

        try:
            async with self._session.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version="s3v4"),
                region_name=self.region,
                verify=self.ca_bundle if self.ca_bundle else self.verify_ssl,
            ) as client:
                await client.put_object(
                    Bucket=self.bucket,
                    Key=object_key,
                    Body=payload,
                )
            self.logger.info("Uploaded %s to s3://%s/%s", path, self.bucket, object_key)
        except ClientError:
            self.logger.exception(
                "Async upload failed for local file %s to object %s", path, object_key
            )
            raise
