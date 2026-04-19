"""Main training loop delegated to by scripts/train.py."""

from __future__ import annotations

import structlog
from omegaconf import DictConfig

from autonomous_sql_agent.models.model import SQLGeneratorModel
from autonomous_sql_agent.tracking.unified import Tracker
from autonomous_sql_agent.utils.seeding import seed_everything

logger: structlog.BoundLogger = structlog.get_logger(__name__)


def train(cfg: DictConfig, tracker: Tracker) -> None:
    """Run the training pipeline described by cfg."""
    seed_everything(cfg.seed)

    model = SQLGeneratorModel(cfg)
    model.load()

    logger.info("training_start", seed=cfg.seed, device=cfg.device)
    tracker.init(cfg)

    for step in range(1, cfg.training.max_steps + 1):
        metrics: dict[str, float] = {"loss": 0.0, "step": float(step)}
        tracker.log(metrics, step=step)
        if step % cfg.training.log_every == 0:
            logger.info("step", **metrics)

    tracker.finish()
    logger.info("training_complete")
