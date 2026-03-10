"""Haiku-based classification of articles for carve-out deals."""

from __future__ import annotations

import json
import logging
import os

import anthropic

from .models import Article, DealAlert, DealStage

logger = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_BATCH_SIZE = 20
_MAX_RETRIES = 3

_SYSTEM_PROMPT = """You classify PE (private equity) deal announcements. For each article, determine if it describes a PE firm **signing or closing a carve-out deal**.

A CARVE-OUT is when a PE firm buys a **division, subsidiary, or business unit** from a **parent company** (corporate or conglomerate). The parent company continues to exist after selling off the unit.

INCLUDE (these ARE carve-outs):
- PE firm agrees to acquire [division/unit/business] from [parent company]
- PE firm completes purchase of [subsidiary] from [corporate parent]
- PE firm to buy [brand/segment] being divested by [parent]

EXCLUDE (these are NOT carve-outs):
- Secondary buyout: PE firm buys a company from ANOTHER PE firm (PE-to-PE)
- Take-private: PE firm acquires an entire publicly-listed company
- Platform acquisition: PE portfolio company acquires another company (bolt-on)
- Fund news: PE firm raises a new fund, closes a fund, hires staff
- IPO: portfolio company goes public
- Any deal where the seller is another PE/financial sponsor

For each article, respond with a JSON object:
{
  "is_carveout": true/false,
  "stage": "signing" or "closing" or null,
  "target_company": "name of the division/unit being acquired" or "",
  "seller": "name of the parent company selling" or "",
  "confidence": 0-100,
  "reasoning": "brief explanation"
}

"signing" = deal announced/agreed but not yet completed
"closing" = deal has been completed/finalized

Respond with a JSON array of objects, one per article, in the same order as the input."""

_FEW_SHOT_USER = """Classify these articles:

1. "Blackstone agrees to acquire Safety Products division from 3M for $2.5B"
2. "KKR completes acquisition of Unilever's tea business"
3. "Apollo to buy Shutterfly from Bain Capital in $2.7B deal"
4. "Thoma Bravo agrees to take Proofpoint private in $12.3B deal"
5. "Blackstone raises $30.4 billion for largest-ever buyout fund"
"""

_FEW_SHOT_ASSISTANT = """[
  {"is_carveout": true, "stage": "signing", "target_company": "Safety Products division", "seller": "3M", "confidence": 95, "reasoning": "PE firm acquiring a division from a corporate parent — classic carve-out signing."},
  {"is_carveout": true, "stage": "closing", "target_company": "tea business", "seller": "Unilever", "confidence": 92, "reasoning": "PE firm completed acquisition of a business unit from a corporate parent — carve-out closing."},
  {"is_carveout": false, "stage": null, "target_company": "", "seller": "", "confidence": 90, "reasoning": "Seller is Bain Capital (another PE firm) — this is a secondary buyout, not a carve-out."},
  {"is_carveout": false, "stage": null, "target_company": "", "seller": "", "confidence": 95, "reasoning": "Taking a whole public company private — this is a take-private, not a carve-out."},
  {"is_carveout": false, "stage": null, "target_company": "", "seller": "", "confidence": 98, "reasoning": "Fund news / fundraising — not a deal announcement at all."}
]"""


def classify_batch(articles: list[Article]) -> list[DealAlert]:
    """Classify a batch of articles using Claude Haiku."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return [DealAlert(article=a) for a in articles]

    client = anthropic.Anthropic(api_key=api_key)

    numbered = "\n".join(
        f'{i+1}. "{a.title}"' + (f" — {a.summary[:150]}" if a.summary else "")
        for i, a in enumerate(articles)
    )
    user_msg = f"Classify these articles:\n\n{numbered}"

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model=_MODEL,
                max_tokens=4096,
                system=_SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": _FEW_SHOT_USER},
                    {"role": "assistant", "content": _FEW_SHOT_ASSISTANT},
                    {"role": "user", "content": user_msg},
                ],
            )

            text = response.content[0].text.strip()
            # Extract JSON array from response
            start = text.index("[")
            end = text.rindex("]") + 1
            results = json.loads(text[start:end])

            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            logger.debug("LLM call: %d input tokens, %d output tokens", input_tokens, output_tokens)

            alerts = []
            for i, article in enumerate(articles):
                if i < len(results):
                    r = results[i]
                    stage = None
                    if r.get("stage") == "signing":
                        stage = DealStage.SIGNING
                    elif r.get("stage") == "closing":
                        stage = DealStage.CLOSING

                    alerts.append(DealAlert(
                        article=article,
                        is_carveout=r.get("is_carveout", False),
                        stage=stage,
                        target_company=r.get("target_company", ""),
                        seller=r.get("seller", ""),
                        confidence=r.get("confidence", 0),
                        reasoning=r.get("reasoning", ""),
                    ))
                else:
                    alerts.append(DealAlert(article=article))

            return alerts

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("LLM response parse error (attempt %d/%d): %s", attempt, _MAX_RETRIES, e)
        except anthropic.APIError as e:
            logger.warning("Anthropic API error (attempt %d/%d): %s", attempt, _MAX_RETRIES, e)

    logger.error("All %d LLM attempts failed for batch of %d articles", _MAX_RETRIES, len(articles))
    return [DealAlert(article=a) for a in articles]


def classify_articles(articles: list[Article]) -> list[DealAlert]:
    """Classify all articles in batches. Returns list of DealAlerts."""
    if not articles:
        return []

    logger.info("Classifying %d articles in batches of %d", len(articles), _BATCH_SIZE)
    all_alerts: list[DealAlert] = []

    for i in range(0, len(articles), _BATCH_SIZE):
        batch = articles[i:i + _BATCH_SIZE]
        logger.info("  Classifying batch %d/%d (%d articles)",
                     i // _BATCH_SIZE + 1,
                     (len(articles) + _BATCH_SIZE - 1) // _BATCH_SIZE,
                     len(batch))
        alerts = classify_batch(batch)
        all_alerts.extend(alerts)

    carveouts = [a for a in all_alerts if a.is_carveout]
    logger.info("Classification complete: %d carve-outs found out of %d articles",
                len(carveouts), len(articles))
    return all_alerts
