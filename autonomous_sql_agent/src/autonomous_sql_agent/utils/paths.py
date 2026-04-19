"""Centralised path constants derived from project root — no hardcoded strings."""

from __future__ import annotations

from pathlib import Path

# Project root = three levels up from src/autonomous_sql_agent/utils/paths.py
ROOT: Path = Path(__file__).resolve().parents[3]

DATA_DIR: Path = ROOT / "data"
CONFIGS_DIR: Path = ROOT / "configs"
EXPERIMENTS_DIR: Path = ROOT / "experiments"
RUNS_DIR: Path = EXPERIMENTS_DIR / "runs"
EXPORTS_DIR: Path = ROOT / "exports"

# Ensure runtime dirs exist when this module is imported
RUNS_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
