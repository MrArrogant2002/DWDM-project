"""Evaluation entry point: python scripts/eval.py +experiment=baseline"""

from __future__ import annotations

import sys
from pathlib import Path

import hydra
import structlog
from omegaconf import DictConfig

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from autonomous_sql_agent.training.evaluate import exact_match
from autonomous_sql_agent.utils.logging_setup import configure_logging

logger: structlog.BoundLogger = structlog.get_logger(__name__)


@hydra.main(version_base=None, config_path="../configs", config_name="config")
def main(cfg: DictConfig) -> None:
    configure_logging(level=cfg.log_level)
    logger.info("eval_start")
    em = exact_match([], [])
    logger.info("eval_complete", exact_match=em)


if __name__ == "__main__":
    main()
