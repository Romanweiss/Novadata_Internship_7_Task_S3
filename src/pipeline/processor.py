from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# NOTE: Pipeline checks only the last suffix via Path.suffix, so .gz/.zip are
# included to allow csv.gz/csv.zip. Real format validation is done in
# _detect_format().
SUPPORTED_EXTENSIONS = {
    ".csv",
    ".parquet",
    ".tsv",
    ".jsonl",
    ".json",
    ".gz",
    ".zip",
}

_FORMAT_TO_OUTPUT_SUFFIX = {
    "csv": ".csv",
    "csv.gz": ".csv.gz",
    "csv.zip": ".csv.zip",
    "tsv": ".tsv",
    "jsonl": ".jsonl",
    "json": ".json",
    "parquet": ".parquet",
}


class UnsupportedFileTypeError(ValueError):
    """Raised when pipeline receives unsupported file extension."""


class FileReadRetryError(RuntimeError):
    """Raised when file reading fails after configured retries."""


def process_file(
    input_path: Path,
    output_dir: Path,
    logger: logging.Logger,
    fallback_head_rows: int = 100,
) -> Path:
    """
    Read dataset, apply simple filtering, save processed file, and return its path.
    """

    input_format = _detect_format(input_path)
    logger.info("Detected input format: %s", input_format)

    dataframe = _read_dataframe(
        input_path=input_path, input_format=input_format, logger=logger
    )
    processed = _filter_dataframe(
        dataframe=dataframe,
        logger=logger,
        fallback_head_rows=fallback_head_rows,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_suffix = _FORMAT_TO_OUTPUT_SUFFIX[input_format]
    input_stem = _derive_stem_without_extension(input_path, output_suffix)
    output_path = output_dir / f"processed_{input_stem}_{timestamp}{output_suffix}"
    _write_dataframe(
        dataframe=processed,
        output_path=output_path,
        input_format=input_format,
    )

    logger.info(
        "Processed file saved: %s (rows before=%s, after=%s)",
        output_path,
        len(dataframe),
        len(processed),
    )
    return output_path


def _detect_format(input_path: Path) -> str:
    suffixes = [suffix.lower() for suffix in input_path.suffixes]
    if not suffixes:
        raise UnsupportedFileTypeError(f"File has no extension: {input_path}")

    if len(suffixes) >= 2 and suffixes[-2:] == [".csv", ".gz"]:
        return "csv.gz"
    if len(suffixes) >= 2 and suffixes[-2:] == [".csv", ".zip"]:
        return "csv.zip"

    last_suffix = suffixes[-1]
    if last_suffix == ".csv":
        return "csv"
    if last_suffix == ".parquet":
        return "parquet"
    if last_suffix == ".tsv":
        return "tsv"
    if last_suffix == ".jsonl":
        return "jsonl"
    if last_suffix == ".json":
        return "json"

    raise UnsupportedFileTypeError(
        f"Unsupported extension chain {''.join(suffixes)} for file {input_path}"
    )


def _derive_stem_without_extension(input_path: Path, output_suffix: str) -> str:
    name = input_path.name
    lower_name = name.lower()
    if lower_name.endswith(output_suffix):
        stem = name[: len(name) - len(output_suffix)]
        return stem or input_path.stem
    return input_path.stem


def _read_dataframe(
    input_path: Path, input_format: str, logger: logging.Logger
) -> pd.DataFrame:
    if input_format in {"csv", "csv.gz", "csv.zip"}:
        return _read_delimited_with_retry(
            input_path=input_path,
            logger=logger,
            sep=",",
            compression="infer",
            format_label=input_format,
        )

    if input_format == "tsv":
        return _read_delimited_with_retry(
            input_path=input_path,
            logger=logger,
            sep="\t",
            compression="infer",
            format_label="tsv",
        )

    if input_format == "jsonl":
        return pd.read_json(input_path, lines=True)

    if input_format == "json":
        try:
            dataframe = pd.read_json(input_path)
            if isinstance(dataframe, pd.Series):
                return dataframe.to_frame().T
            return dataframe
        except ValueError:
            # Scalar dict payloads ({"a": 1, "b": 2}) need manual wrapping.
            with input_path.open("r", encoding="utf-8") as stream:
                payload = json.load(stream)

            if isinstance(payload, list):
                return pd.DataFrame(payload)
            if isinstance(payload, dict):
                return pd.DataFrame([payload])
            raise

    if input_format == "parquet":
        try:
            return pd.read_parquet(input_path)
        except ImportError as error:
            raise RuntimeError(
                "Parquet support is unavailable. Install pyarrow."
            ) from error

    raise UnsupportedFileTypeError(f"Unsupported format: {input_format}")


def _read_delimited_with_retry(
    input_path: Path,
    logger: logging.Logger,
    sep: str,
    compression: str,
    format_label: str,
) -> pd.DataFrame:
    max_attempts = 3
    retry_delays = (0.5, 1.0)

    for attempt in range(1, max_attempts + 1):
        try:
            dataframe = pd.read_csv(
                input_path,
                sep=sep,
                compression=compression,
            )
            if attempt > 1:
                logger.info(
                    "%s read succeeded after retry for file %s on attempt %s.",
                    format_label.upper(),
                    input_path,
                    attempt,
                )
            return dataframe
        except pd.errors.EmptyDataError as error:
            retry_count = max_attempts - 1
            if attempt >= max_attempts:
                raise FileReadRetryError(
                    f"{format_label} remained empty/incomplete after "
                    f"{max_attempts} attempts: {input_path}"
                ) from error

            delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
            logger.warning(
                "EmptyDataError, retry %s/%s for %s after %.1fs",
                attempt,
                retry_count,
                input_path,
                delay,
            )
            time.sleep(delay)


def _filter_dataframe(
    dataframe: pd.DataFrame, logger: logging.Logger, fallback_head_rows: int
) -> pd.DataFrame:
    if "value" in dataframe.columns:
        rows_before = len(dataframe)

        # Normalize decimal commas and coerce non-numeric values to NaN.
        raw = dataframe["value"]
        normalized = raw.astype(str).str.replace(",", ".", regex=False)
        value_num = pd.to_numeric(normalized, errors="coerce")

        count_invalid = int(value_num.isna().sum())

        filtered = dataframe[value_num > 0].copy()
        rows_after = len(filtered)

        logger.info("Applied filter: value > 0")
        logger.info(
            "Filter stats: rows_before=%s, invalid_value=%s, rows_after=%s",
            rows_before,
            count_invalid,
            rows_after,
        )
        return filtered

    logger.info(
        "Column 'value' is absent; using fallback head(%s).", fallback_head_rows
    )
    return dataframe.head(fallback_head_rows).copy()


def _write_dataframe(dataframe: pd.DataFrame, output_path: Path, input_format: str) -> None:
    if input_format in {"csv", "csv.gz", "csv.zip"}:
        dataframe.to_csv(output_path, index=False)
        return

    if input_format == "tsv":
        dataframe.to_csv(output_path, sep="\t", index=False)
        return

    if input_format == "jsonl":
        dataframe.to_json(output_path, orient="records", lines=True)
        return

    if input_format == "json":
        dataframe.to_json(output_path, orient="records")
        return

    if input_format == "parquet":
        dataframe.to_parquet(output_path, index=False)
        return

    raise UnsupportedFileTypeError(f"Unsupported format: {input_format}")
