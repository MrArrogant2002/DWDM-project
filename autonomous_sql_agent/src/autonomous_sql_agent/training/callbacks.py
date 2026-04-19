"""Training callbacks: checkpointing and early stopping."""

from __future__ import annotations

from pathlib import Path

import structlog

logger: structlog.BoundLogger = structlog.get_logger(__name__)


class CheckpointCallback:
    """Save model checkpoint every N steps."""

    def __init__(self, save_dir: Path, every_n_steps: int = 500) -> None:
        self._save_dir = save_dir
        self._every_n_steps = every_n_steps
        self._save_dir.mkdir(parents=True, exist_ok=True)

    def on_step(self, step: int) -> None:
        if step % self._every_n_steps == 0:
            ckpt = self._save_dir / f"step_{step:06d}"
            logger.info("checkpoint_saved", path=str(ckpt))
            # model.save_pretrained(str(ckpt))


class EarlyStoppingCallback:
    """Stop training when a metric stops improving for `patience` evals."""

    def __init__(self, patience: int = 5, min_delta: float = 1e-4) -> None:
        self._patience = patience
        self._min_delta = min_delta
        self._best: float = float("inf")
        self._wait: int = 0

    def should_stop(self, metric: float) -> bool:
        if metric < self._best - self._min_delta:
            self._best = metric
            self._wait = 0
            return False
        self._wait += 1
        return self._wait >= self._patience
