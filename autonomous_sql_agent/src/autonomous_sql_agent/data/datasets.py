"""Dataset and DataLoader factories for warehouse analytics tasks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import structlog
from omegaconf import DictConfig
from torch.utils.data import DataLoader, Dataset

logger: structlog.BoundLogger = structlog.get_logger(__name__)


class WarehouseDataset(Dataset[dict[str, Any]]):
    """Wraps a pandas DataFrame as a PyTorch Dataset."""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df.reset_index(drop=True)

    def __len__(self) -> int:
        return len(self._df)

    def __getitem__(self, idx: int) -> dict[str, Any]:
        return self._df.iloc[idx].to_dict()  # type: ignore[return-value]


def make_dataloader(cfg: DictConfig, split: str = "train") -> DataLoader[dict[str, Any]]:
    """Create a DataLoader for the given split using config."""
    data_path = Path(cfg.data.root) / cfg.data[split]
    logger.info("loading_split", split=split, path=str(data_path))

    df = pd.read_parquet(data_path) if data_path.suffix == ".parquet" else pd.read_csv(data_path)
    dataset = WarehouseDataset(df)

    return DataLoader(
        dataset,
        batch_size=cfg.data.batch_size,
        shuffle=(split == "train"),
        num_workers=cfg.num_workers,
        pin_memory=False,  # CPU-only — pin_memory has no benefit here
    )
