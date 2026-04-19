"""Download or generate seed data for the warehouse."""

from __future__ import annotations

import sys
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from autonomous_sql_agent.utils.logging_setup import configure_logging
from autonomous_sql_agent.utils.paths import DATA_DIR

logger: structlog.BoundLogger = structlog.get_logger(__name__)


def main() -> None:
    configure_logging()
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)
    logger.info(
        "data_dirs_ready",
        raw=str(DATA_DIR / "raw"),
        processed=str(DATA_DIR / "processed"),
    )
    logger.info(
        "action_required",
        message="Place raw warehouse exports in data/raw/ then run preprocessing.",
    )


if __name__ == "__main__":
    main()
