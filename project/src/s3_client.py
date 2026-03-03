from __future__ import annotations

import logging
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class S3Client:
    """Thin synchronous S3 client wrapper for S3-compatible endpoints."""

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
        self.bucket = bucket
        self.logger = logger or logging.getLogger(__name__)
        verify: bool | str = ca_bundle if ca_bundle else verify_ssl
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name=region,
            verify=verify,
        )

    def upload(self, file_path: str | Path, object_name: str) -> None:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Local file does not exist: {path}")

        try:
            self.s3.upload_file(str(path), self.bucket, object_name)
            self.logger.info("Uploaded %s to s3://%s/%s", path, self.bucket, object_name)
        except ClientError:
            self.logger.exception(
                "S3 upload failed for local file %s to object %s", path, object_name
            )
            raise

    def download(self, object_name: str, save_path: str | Path) -> Path:
        destination = Path(save_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        try:
            self.s3.download_file(self.bucket, object_name, str(destination))
            self.logger.info(
                "Downloaded s3://%s/%s to %s", self.bucket, object_name, destination
            )
        except ClientError:
            self.logger.exception(
                "S3 download failed for object %s to local path %s",
                object_name,
                destination,
            )
            raise
        return destination

    def list_files(self, prefix: str | None = None) -> list[str]:
        """Return all object keys in bucket, optionally filtered by prefix."""

        params: dict[str, str] = {"Bucket": self.bucket}
        if prefix:
            params["Prefix"] = prefix

        paginator = self.s3.get_paginator("list_objects_v2")
        keys: list[str] = []

        for page in paginator.paginate(**params):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key:
                    keys.append(key)

        self.logger.debug(
            "Listed %s objects in bucket %s with prefix=%r",
            len(keys),
            self.bucket,
            prefix,
        )
        return keys

    def file_exists(self, object_name: str) -> bool:
        """
        Check if object exists.

        Returns False only for not-found errors. Any other S3 error is re-raised.
        """

        try:
            self.s3.head_object(Bucket=self.bucket, Key=object_name)
            return True
        except ClientError as error:
            error_code = (error.response.get("Error") or {}).get("Code")
            status_code = (error.response.get("ResponseMetadata") or {}).get(
                "HTTPStatusCode"
            )

            if error_code in {"404", "NoSuchKey", "NotFound"} or status_code == 404:
                self.logger.info(
                    "Object not found in bucket: s3://%s/%s", self.bucket, object_name
                )
                return False

            self.logger.exception(
                "Unexpected S3 error while checking object existence: s3://%s/%s",
                self.bucket,
                object_name,
            )
            raise
