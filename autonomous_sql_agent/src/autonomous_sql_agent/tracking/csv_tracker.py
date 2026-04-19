"""CSV-backed metrics tracker — flat file, zero external dependencies."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import IO, Any

import structlog

logger: structlog.BoundLogger = structlog.get_logger(__name__)


class CSVTracker:
    """Append metric rows to a CSV file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fieldnames: list[str] = []
        self._writer: csv.DictWriter[str] | None = None
        self._file: IO[str] | None = None

    def open(self) -> None:
        self._file = self._path.open("w", newline="")
        logger.info("csv_tracker_opened", path=str(self._path))

    def log(self, metrics: dict[str, float], step: int) -> None:
        row: dict[str, Any] = {"step": step, **metrics}
        if self._writer is None:
            self._fieldnames = list(row.keys())
            self._writer = csv.DictWriter(self._file, fieldnames=self._fieldnames)
            self._writer.writeheader()
        self._writer.writerow(row)
        if self._file:
            self._file.flush()

    def close(self) -> None:
        if self._file:
            self._file.close()
            logger.info("csv_tracker_closed", path=str(self._path))
