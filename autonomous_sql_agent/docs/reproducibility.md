# Reproducibility Guide

## Seeding

Every run calls `seed_everything(cfg.seed)` which seeds:
- Python `random`
- `numpy`
- `torch` (CPU deterministic via `use_deterministic_algorithms`)

The seed is logged at run start and stored in Hydra's auto-saved config snapshot.

## Config Snapshot

Hydra automatically saves `experiments/runs/<run>/.hydra/config.yaml` with the
fully-resolved config (all overrides applied). To replay an exact run:

```bash
python scripts/train.py --config-path experiments/runs/<run>/.hydra --config-name config
```

## Git SHA

`scripts/train.py` logs the short git SHA at startup. To recover the exact code
state of a past run, check the `git_sha` field in the run log, then:

```bash
git checkout <sha>
```

## Dataset State

Record the MD5 hash of your processed parquet files alongside each run.
With DVC (optional), `dvc repro` reproduces the exact data pipeline.

## Full Replay Checklist

1. Same git SHA (`git checkout <sha>`)
2. Same `.env` values (`DATABASE_URL`, `HF_MODEL_ID`, `SEED`)
3. Same Hydra config snapshot from the run dir
4. Same processed data files (verify MD5 or DVC hash)
5. `make install` to restore exact dependency versions from `pyproject.toml`
