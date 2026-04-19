"""Tests for model loading and generation."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig

from autonomous_sql_agent.models.model import SQLGeneratorModel


def test_model_not_loaded_raises(minimal_cfg: DictConfig) -> None:
    model = SQLGeneratorModel(minimal_cfg)
    with pytest.raises(RuntimeError, match="Model not loaded"):
        model.generate("SELECT")


def test_model_load_smoke(minimal_cfg: DictConfig) -> None:
    pytest.skip("Network-dependent model load skipped in CI")
    model = SQLGeneratorModel(minimal_cfg)
    model.load()
    assert model.model is not None
    assert model.tokenizer is not None
