"""Core data models for the carve-out monitor."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class DealStage(str, Enum):
    SIGNING = "signing"
    CLOSING = "closing"


class DealType(str, Enum):
    CORPORATE_CARVEOUT = "corporate_carveout"       # PE buys division from corporate
    PORTCO_CARVEOUT = "portco_carveout"              # PE buys division from PE portco


class Firm(BaseModel):
    """A target PE firm or law firm to monitor."""

    name: str
    domain: str = ""
    feed_url: str | None = None
    press_url: str | None = None
    hq: str = ""
    sectors: list[str] = Field(default_factory=list)
    source_category: str = "pe_firm"  # "pe_firm" or "law_firm"


class Article(BaseModel):
    """A raw article fetched from a PE firm's website."""

    title: str
    url: str
    summary: str = ""
    published: datetime | None = None
    firm_name: str = ""


class DealAlert(BaseModel):
    """A classified separation deal — the pipeline output."""

    article: Article
    is_carveout: bool = False
    deal_type: DealType | None = None
    stage: DealStage | None = None
    target_company: str = ""
    seller: str = ""
    buyer: str = ""  # Acquiring entity (PE firm or corporate)
    confidence: int = Field(default=0, ge=0, le=100)
    reasoning: str = ""


class QualifiedAlert(DealAlert):
    """A carve-out deal scored against Larkhill's buyer profile."""

    larkhill_fit: int = Field(default=0, ge=0, le=100)
    pe_buyer_score: int = 0
    separation_complexity_score: int = 0
    deal_size_score: int = 0
    geography_score: int = 0
    timing_score: int = 0
    pe_firm: str = ""
    recommended_action: str = "discard"  # pursue | monitor | discard


def load_firms(path: str | Path = "targets.yml") -> list[Firm]:
    """Load target firms from YAML file."""
    with open(path) as f:
        data = yaml.safe_load(f)
    return [Firm(**firm) for firm in data.get("firms", [])]
