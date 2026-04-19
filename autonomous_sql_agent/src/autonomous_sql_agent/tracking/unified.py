"""Unified Tracker — single API that fans out to enabled backends (CSV only by default)."""

from __future__ import annotations

from pathlib import Path

import structlog
from omegaconf import DictConfig

from autonomous_sql_agent.tracking.csv_tracker import CSVTracker
from autonomous_sql_agent.utils.paths import RUNS_DIR

logger: structlog.BoundLogger = structlog.get_logger(__name__)


class Tracker:
    """Single tracking interface; enables/disables backends via config.

    Expected config shape under cfg.tracking:
        csv:
          enabled: true
          path: experiments/runs/<run_dir>/metrics.csv
    """

    def __init__(self) -> None:
        self._csv: CSVTracker | None = None

    def init(self, cfg: DictConfig) -> None:
        """Initialise all enabled backends."""
        csv_cfg = cfg.tracking.get("csv", {})
        if csv_cfg.get("enabled", True):
            csv_path = Path(csv_cfg.get("path", str(RUNS_DIR / "metrics.csv")))
            self._csv = CSVTracker(csv_path)
            self._csv.open()
            logger.info("tracker_init", backend="csv", path=str(csv_path))

    def log(self, metrics: dict[str, float], step: int) -> None:
        if self._csv:
            self._csv.log(metrics, step)

    def log_artifact(self, path: Path | str) -> None:
        logger.info("artifact_logged", path=str(path))

    def finish(self) -> None:
        if self._csv:
            self._csv.close()
        logger.info("tracker_finished")
