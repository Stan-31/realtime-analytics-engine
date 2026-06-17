"""Wire-format models. A `ValidationError` here means the message goes to DLQ."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Tick(BaseModel):
    model_config = {"extra": "ignore"}

    symbol: str = Field(min_length=1, max_length=16)
    price: float = Field(gt=0)
    ts: float = Field(gt=0)
