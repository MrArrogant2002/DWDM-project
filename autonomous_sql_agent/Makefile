.PHONY: install test train lint audit fmt clean

install:
	pip install -e ".[dev]"
	pre-commit install

test:
	PYTHONPATH=src pytest tests/ -v

train:
	PYTHONPATH=src python scripts/train.py +experiment=baseline

lint:
	ruff check src/ tests/ scripts/
	black --check src/ tests/ scripts/
	mypy src/

audit:
	pip-audit
	detect-secrets scan --baseline .secrets.baseline

fmt:
	ruff check --fix src/ tests/ scripts/
	black src/ tests/ scripts/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf .pytest_cache dist build
