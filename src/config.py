from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is required.")
    return value


def _reject_template_value(name: str, value: str) -> None:
    template_values = {
        "S3_ENDPOINT": {"https://s3.example.com"},
        "S3_ACCESS_KEY": {"your_access_key"},
        "S3_SECRET_KEY": {"your_secret_key"},
        "S3_BUCKET": {"your_bucket_name"},
    }
    if value in template_values.get(name, set()):
        raise ValueError(
            f"Environment variable {name} still has template value '{value}'. "
            "Update project/.env with real credentials."
        )


def _get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class S3Settings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    region: str = "us-east-1"
    prefix: str = ""
    verify_ssl: bool = True
    ca_bundle: str | None = None

    @property
    def normalized_prefix(self) -> str:
        return self.prefix.strip("/")

    def build_object_key(self, key: str) -> str:
        clean_key = key.lstrip("/")
        if not self.normalized_prefix:
            return clean_key
        return f"{self.normalized_prefix}/{clean_key}"


@dataclass(frozen=True)
class PipelineSettings:
    watch_dir: Path
    archive_dir: Path
    log_file: Path
    log_object_key: str
    watch_use_polling: bool = False


@dataclass(frozen=True)
class Settings:
    s3: S3Settings
    pipeline: PipelineSettings

    @classmethod
    def from_env(cls, env_file: str | Path | None = ".env") -> "Settings":
        if env_file:
            load_dotenv(dotenv_path=env_file, override=False)

        s3 = S3Settings(
            endpoint=_require_env("S3_ENDPOINT"),
            access_key=_require_env("S3_ACCESS_KEY"),
            secret_key=_require_env("S3_SECRET_KEY"),
            bucket=_require_env("S3_BUCKET"),
            region=os.getenv("S3_REGION", "us-east-1").strip() or "us-east-1",
            prefix=os.getenv("S3_PREFIX", "").strip(),
            verify_ssl=_get_bool_env("S3_VERIFY_SSL", default=True),
            ca_bundle=(os.getenv("S3_CA_BUNDLE", "").strip() or None),
        )
        _reject_template_value("S3_ENDPOINT", s3.endpoint)
        _reject_template_value("S3_ACCESS_KEY", s3.access_key)
        _reject_template_value("S3_SECRET_KEY", s3.secret_key)
        _reject_template_value("S3_BUCKET", s3.bucket)

        log_object_key = os.getenv("LOG_OBJECT_KEY", "logs/pipeline.log").strip()
        if not log_object_key:
            raise ValueError("Environment variable LOG_OBJECT_KEY cannot be empty.")

        pipeline = PipelineSettings(
            watch_dir=Path(os.getenv("WATCH_DIR", "./watch")).resolve(),
            archive_dir=Path(os.getenv("ARCHIVE_DIR", "./archive")).resolve(),
            log_file=Path(os.getenv("LOG_FILE", "./logs/pipeline.log")).resolve(),
            log_object_key=log_object_key,
            watch_use_polling=_get_bool_env("WATCH_USE_POLLING", default=False),
        )
        return cls(s3=s3, pipeline=pipeline)
