"""seed_everything() — deterministic reproducibility across all libraries."""

from __future__ import annotations

import random

import numpy as np
import structlog
import torch

logger: structlog.BoundLogger = structlog.get_logger(__name__)


def seed_everything(seed: int) -> None:
    """Seed Python random, NumPy, and PyTorch (CPU deterministic).

    JAX is not seeded here because it uses explicit PRNGKey threading.
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.use_deterministic_algorithms(True, warn_only=True)
    logger.info("seed_set", seed=seed)
