"""Shared pytest fixtures."""

from __future__ import annotations

import pandas as pd
import pytest
from omegaconf import DictConfig, OmegaConf


@pytest.fixture()
def minimal_cfg() -> DictConfig:
    """Minimal Hydra-like config for unit tests."""
    return OmegaConf.create(
        {
            "seed": 0,
            "device": "cpu",
            "num_workers": 0,
            "model": {
                "name": "distilgpt2",
                "fallback_name": "distilgpt2",
                "temperature": 0.1,
                "top_p": 0.9,
                "max_new_tokens": 32,
            },
            "data": {
                "root": "data/processed",
                "train": "train.parquet",
                "val": "val.parquet",
                "test": "test.parquet",
                "batch_size": 4,
                "preview_row_limit": 10,
                "export_row_limit": 100,
            },
            "training": {
                "max_steps": 2,
                "log_every": 1,
                "eval_every": 2,
                "save_every": 2,
            },
            "tracking": {"csv": {"enabled": True, "path": "/tmp/test_metrics.csv"}},
        }
    )


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    """Small DataFrame for data tests."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "revenue": [100.0, 200.0, 150.0],
            "region": ["north", "south", "east"],
        }
    )
