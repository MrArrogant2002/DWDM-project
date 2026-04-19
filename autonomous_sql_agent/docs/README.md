# Autonomous SQL Agent — Technical Notes

Developer-focused notes. See the project `README.md` for quickstart.

## Directory Layout

| Path | Purpose |
|---|---|
| `configs/` | Hydra YAML configs — model, data, optim, experiment |
| `src/autonomous_sql_agent/` | All library code |
| `scripts/` | CLI entry points (train, eval, download_data) |
| `tests/` | pytest unit + integration tests |
| `experiments/runs/` | Per-run artifacts (gitignored) |
| `data/raw/` | Raw warehouse exports (gitignored) |
| `data/processed/` | Preprocessed files ready for DataLoader (gitignored) |

## Adding a New Experiment

1. Copy `configs/experiment/baseline.yaml` to `configs/experiment/<name>.yaml`.
2. Override any top-level keys (`seed`, `training.max_steps`, `model.name` …).
3. Run: `python scripts/train.py +experiment=<name>`.
4. Metrics land in `experiments/runs/<timestamp>/metrics.csv`.
