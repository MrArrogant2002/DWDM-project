from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def _to_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(slots=True)
class AppConfig:
    database_url: str
    hf_model_id: str
    hf_token: str | None
    hf_inference_model: str
    device: str
    statement_timeout_ms: int
    export_dir: Path
    preview_row_limit: int
    export_row_limit: int
    max_generation_retries: int
    default_order_count: int
    use_fallback_only: bool
    project_root: Path
    data_dir: Path
    docs_dir: Path

    @classmethod
    def from_env(cls) -> "AppConfig":
        project_root = Path(__file__).resolve().parents[2]
        if load_dotenv is not None:
            load_dotenv(project_root / ".env")

        export_dir = project_root / os.getenv("EXPORT_DIR", "exports")
        data_dir = project_root / "data"
        docs_dir = project_root / "docs"

        hf_token = os.getenv("HF_TOKEN") or None

        return cls(
            database_url=os.getenv(
                "DATABASE_URL",
                "sqlite:///data/warehouse.db",
            ),
            hf_model_id=os.getenv("HF_MODEL_ID", "defog/sqlcoder-7b-2"),
            hf_token=hf_token,
            hf_inference_model=os.getenv(
                "HF_INFERENCE_MODEL", "Qwen/Qwen2.5-Coder-7B-Instruct"
            ),
            device=os.getenv("DEVICE", "auto"),
            statement_timeout_ms=_to_int("STATEMENT_TIMEOUT_MS", 10000),
            export_dir=export_dir,
            preview_row_limit=_to_int("PREVIEW_ROW_LIMIT", 200),
            export_row_limit=_to_int("EXPORT_ROW_LIMIT", 50000),
            max_generation_retries=_to_int("MAX_GENERATION_RETRIES", 2),
            default_order_count=_to_int("DEFAULT_ORDER_COUNT", 10000),
            use_fallback_only=os.getenv("USE_FALLBACK_ONLY", "false").lower()
            in ("1", "true", "yes"),
            project_root=project_root,
            data_dir=data_dir,
            docs_dir=docs_dir,
        )
