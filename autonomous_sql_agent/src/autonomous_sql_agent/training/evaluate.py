"""Evaluation helpers — execution-accuracy and exact-match against gold SQL."""

from __future__ import annotations

import structlog

logger: structlog.BoundLogger = structlog.get_logger(__name__)


def execution_accuracy(predictions: list[str], gold: list[str]) -> float:
    """Fraction of predictions that produce the same result set as gold SQL.

    Stub — wire up to DatabaseManager for real evaluation.
    """
    logger.info("execution_accuracy_stub", n=len(predictions))
    return 0.0


def exact_match(predictions: list[str], gold: list[str]) -> float:
    """Exact-string match fraction after whitespace normalization."""
    if not gold:
        return 0.0
    matches = sum(
        " ".join(p.lower().split()) == " ".join(g.lower().split())
        for p, g in zip(predictions, gold)
    )
    return matches / len(gold)
