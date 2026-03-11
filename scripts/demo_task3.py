from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import Settings


def main() -> None:
    settings = Settings.from_env()
    watch_dir = settings.pipeline.watch_dir
    watch_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = watch_dir / f"sample_{timestamp}.csv"
    txt_path = watch_dir / f"ignored_{timestamp}.txt"

    dataframe = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "value": [10, -5, 0, 3],
            "name": ["a", "b", "c", "d"],
        }
    )
    dataframe.to_csv(csv_path, index=False)
    txt_path.write_text("unsupported file extension demo", encoding="utf-8")

    print(f"Created sample CSV in WATCH_DIR: {csv_path}")
    print(f"Created unsupported TXT in WATCH_DIR: {txt_path}")
    print("If pipeline is running, CSV will be processed/uploaded, TXT ignored.")


if __name__ == "__main__":
    main()
