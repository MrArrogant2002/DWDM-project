"""HuggingFace Transformer model skeleton for NL-to-SQL generation."""

from __future__ import annotations

import structlog
from omegaconf import DictConfig
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    PreTrainedModel,
    PreTrainedTokenizerBase,
)

logger: structlog.BoundLogger = structlog.get_logger(__name__)


class SQLGeneratorModel:
    """Wraps a HuggingFace causal LM for SQL generation."""

    def __init__(self, cfg: DictConfig) -> None:
        self._cfg = cfg
        self.model: PreTrainedModel | None = None
        self.tokenizer: PreTrainedTokenizerBase | None = None

    def load(self) -> None:
        """Load model and tokenizer; falls back to cfg.model.fallback_name on error."""
        model_name: str = self._cfg.model.name
        try:
            logger.info("loading_model", model=model_name)
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForCausalLM.from_pretrained(
                model_name,
                device_map="cpu",
                torch_dtype="auto",
                # 4-bit quantization requires CUDA — disabled for CPU-only
            )
            logger.info("model_loaded", model=model_name)
        except Exception as exc:
            fallback: str = self._cfg.model.fallback_name
            logger.warning("model_load_failed_using_fallback", error=str(exc), fallback=fallback)
            self.tokenizer = AutoTokenizer.from_pretrained(fallback)
            self.model = AutoModelForCausalLM.from_pretrained(
                fallback,
                device_map="cpu",
                torch_dtype="auto",
            )

    def generate(self, prompt: str) -> str:
        """Generate SQL from a natural-language prompt."""
        if self.model is None or self.tokenizer is None:
            raise RuntimeError("Model not loaded — call load() first.")
        inputs = self.tokenizer(prompt, return_tensors="pt")
        output_ids = self.model.generate(
            **inputs,
            max_new_tokens=self._cfg.model.max_new_tokens,
            temperature=self._cfg.model.temperature,
            top_p=self._cfg.model.top_p,
            do_sample=True,
        )
        return self.tokenizer.decode(output_ids[0], skip_special_tokens=True)
