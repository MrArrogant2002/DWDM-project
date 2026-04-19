# --- builder stage ---
FROM python:3.12-slim AS builder

WORKDIR /build

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir ".[dev]" --target /app/deps

# --- runtime stage ---
FROM python:3.12-slim AS runtime

RUN groupadd -r app && useradd -r -g app -u 1000 app

WORKDIR /app

COPY --from=builder /app/deps /app/deps
COPY src/ src/
COPY configs/ configs/
COPY scripts/ scripts/

ENV PYTHONPATH=/app/deps:/app/src
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

USER app

CMD ["python", "scripts/train.py"]
