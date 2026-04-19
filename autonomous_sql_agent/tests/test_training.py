"""Tests for training callbacks and evaluation helpers."""

from __future__ import annotations

from autonomous_sql_agent.training.callbacks import EarlyStoppingCallback
from autonomous_sql_agent.training.evaluate import exact_match


def test_early_stopping_triggers() -> None:
    cb = EarlyStoppingCallback(patience=3, min_delta=0.01)
    assert not cb.should_stop(1.0)
    assert not cb.should_stop(1.0)
    assert not cb.should_stop(1.0)
    assert cb.should_stop(1.0)


def test_early_stopping_resets_on_improvement() -> None:
    cb = EarlyStoppingCallback(patience=2, min_delta=0.01)
    cb.should_stop(1.0)
    cb.should_stop(1.0)
    assert not cb.should_stop(0.5)  # improvement resets counter


def test_exact_match_perfect() -> None:
    preds = ["SELECT id FROM t", "SELECT name FROM t"]
    gold = ["SELECT id FROM t", "SELECT name FROM t"]
    assert exact_match(preds, gold) == 1.0


def test_exact_match_none() -> None:
    assert exact_match(["SELECT a FROM t"], ["SELECT b FROM t"]) == 0.0


def test_exact_match_empty() -> None:
    assert exact_match([], []) == 0.0
