"""CLI entry point: python scripts/train.py +experiment=baseline"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import hydra
import structlog
from omegaconf import DictConfig

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from autonomous_sql_agent.tracking.unified import Tracker
from autonomous_sql_agent.training.train import train
from autonomous_sql_agent.utils.logging_setup import configure_logging
from autonomous_sql_agent.utils.seeding import seed_everything

logger: structlog.BoundLogger = structlog.get_logger(__name__)


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


@hydra.main(version_base=None, config_path="../configs", config_name="config")
def main(cfg: DictConfig) -> None:
    configure_logging(level=cfg.log_level)
    seed_everything(cfg.seed)
    logger.info("run_start", git_sha=_git_sha(), seed=cfg.seed, device=cfg.device)
    tracker = Tracker()
    train(cfg, tracker)


if __name__ == "__main__":
    main()
