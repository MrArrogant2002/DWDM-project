# Autonomous SQL Agent

An autonomous multi-agent SQL analysis pipeline for retail/e-commerce data warehouses,
powered by HuggingFace Transformers (`defog/sqlcoder-7b-2`).

## Quickstart

```bash
make install
cp .env.example .env   # edit DATABASE_URL
make train
make test
```

## Config Overrides (Hydra)

```bash
# Override seed
python scripts/train.py seed=123

# Run a named experiment
python scripts/train.py +experiment=baseline

# Override model
python scripts/train.py model.name=Qwen/Qwen2.5-3B-Instruct
```

## Experiment Tracking

Metrics are logged to CSV: `experiments/runs/<run_dir>/metrics.csv`.
Each run also emits a `config_snapshot.yaml` (via Hydra) and records the git SHA.

## Reproducing Experiments

See [docs/reproducibility.md](docs/reproducibility.md) for instructions on pinning seeds,
recovering past configs, and verifying dataset state.

## License

MIT
