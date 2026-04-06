"""Opus-based Larkhill qualification of carve-out alerts."""

from __future__ import annotations

import json
import logging
import os

import anthropic

from .models import DealAlert, DealType, QualifiedAlert

logger = logging.getLogger(__name__)

_MODEL = "claude-opus-4-6"
_BATCH_SIZE = 5
_MAX_RETRIES = 3

# Module-level token counters for cost reporting
token_usage = {"input": 0, "output": 0}

_SYSTEM_PROMPT = """You are qualifying separation deals for Larkhill & Company, a separation execution firm. Larkhill runs the Separation Management Office (SMO) for carve-outs and divestitures.

Each deal has a deal_type:
- "corporate_carveout" — PE firm buys a division from a corporate parent
- "portco_carveout" — PE firm buys a division from another PE firm's portfolio company
- "corporate_divestiture" — Non-PE buyer (corporate/trade) buys a division from a corporate parent

For each deal, score against these criteria (0-100 each):

1. PE_BUYER: Is a PE firm on the BUY side?
   - For corporate_carveout and portco_carveout:
     - 100 = Named PE firm acquiring the division
     - 50 = PE involvement likely but not confirmed
     - 0 = No PE buyer identified
   - For corporate_divestiture: SKIP this criterion. The buyer is a corporate/trade entity. Larkhill targets BOTH sides (buyer needs integration help, seller needs separation help). Score 60 as a baseline — separation opportunity exists regardless of PE involvement.

2. SEPARATION_COMPLEXITY: Does this require functional separation?
   - 100 = Integrated operating business embedded in parent (shared IT, finance, HR, legal, operations, supply chain)
   - 70 = Likely separation needed (division/business unit, but details unclear)
   - 30 = Minimal separation (self-contained subsidiary)
   - 0 = No separation needed (asset transfer, standalone company, sports franchise)

3. DEAL_SIZE: Is this mid-market or larger?
   - 100 = $500M+ deal value
   - 70 = $100M-$500M
   - 40 = Under $100M or size unknown
   - 0 = Clearly small/micro deal

4. GEOGRAPHY: Is target or buyer in Larkhill's focus markets?
   - 100 = US or UK/Europe
   - 50 = Mixed or unclear
   - 0 = Asia, Africa, LatAm only

5. TIMING: Is this at a stage where SMO services are relevant?
   - 100 = Pre-close or just signed (maximum engagement window)
   - 70 = Recently closed (still in separation)
   - 30 = Closed 6+ months ago
   - 0 = Exploration/rumour only

Respond with a JSON array of objects, one per deal, in the same order as the input:
{
  "larkhill_fit": 0-100 (weighted average: PE_BUYER 30%, SEPARATION_COMPLEXITY 30%, DEAL_SIZE 15%, GEOGRAPHY 15%, TIMING 10%),
  "pe_buyer_score": 0-100,
  "separation_complexity_score": 0-100,
  "deal_size_score": 0-100,
  "geography_score": 0-100,
  "timing_score": 0-100,
  "pe_firm": "For carve-outs: ALL PE firms on the buy side, comma-separated. If multiple firms are bidding or co-investing, list every one. If 'private equity' or 'PE firms' mentioned without names, write 'Unnamed PE'. For corporate_divestiture: name of the BUYER (even though not PE). Empty string ONLY if buyer is completely unknown.",
  "reasoning": "brief explanation",
  "recommended_action": "pursue" or "monitor" or "discard"
}

"pursue" = larkhill_fit >= 65 — high-quality separation opportunity for Larkhill
"monitor" = larkhill_fit 40-64 — worth tracking but not actionable yet
"discard" = larkhill_fit < 40 — not relevant to Larkhill"""


def _qualify_batch(alerts: list[DealAlert], client: anthropic.Anthropic | None = None) -> list[QualifiedAlert]:
    """Qualify a batch of classifier-positive alerts using Opus."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key and not client:
        logger.error("ANTHROPIC_API_KEY not set")
        return [QualifiedAlert(**a.model_dump()) for a in alerts]

    if client is None:
        client = anthropic.Anthropic(api_key=api_key)

    numbered = "\n".join(
        f'{i+1}. "{a.article.title}"'
        + (f" — {a.article.summary[:200]}" if a.article.summary else "")
        + f"\n   Deal type: {a.deal_type.value if a.deal_type else 'unknown'}"
        + f", Target: {a.target_company}, Seller: {a.seller}"
        + (f", Buyer: {a.buyer}" if a.buyer else "")
        + f", Stage: {a.stage.value if a.stage else 'unknown'}"
        + f", Classifier confidence: {a.confidence}"
        + (f"\n   {a.reasoning}" if a.reasoning and "[Also:" in a.reasoning else "")
        for i, a in enumerate(alerts)
    )
    user_msg = f"Qualify these separation deals for Larkhill:\n\n{numbered}"

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model=_MODEL,
                max_tokens=4096,
                system=[{
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_msg}],
            )

            text = response.content[0].text.strip()
            start = text.index("[")
            end = text.rindex("]") + 1
            results = json.loads(text[start:end])

            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            token_usage["input"] += input_tokens
            token_usage["output"] += output_tokens
            logger.debug("Qualifier call: %d input tokens, %d output tokens",
                         input_tokens, output_tokens)

            qualified = []
            for i, alert in enumerate(alerts):
                if i < len(results):
                    r = results[i]
                    larkhill_fit = r.get("larkhill_fit", 0)
                    pe_buyer_score = r.get("pe_buyer_score", 0)
                    sep_score = r.get("separation_complexity_score", 0)
                    recommended = r.get("recommended_action", "discard")

                    # Hard cap: if PE_BUYER = 0 and this is a carve-out
                    # (not a corporate divestiture), there's no PE buyer
                    # to sell to — discard.
                    # Corporate divestitures skip this cap: Larkhill targets
                    # both buyer (integration) and seller (separation).
                    is_corporate_divestiture = (
                        alert.deal_type == DealType.CORPORATE_DIVESTITURE
                    )
                    if pe_buyer_score == 0 and not is_corporate_divestiture:
                        larkhill_fit = min(larkhill_fit, 30)
                        recommended = "discard"
                        logger.info("  PE_BUYER=0 cap: %s → larkhill_fit=%d, discard",
                                    alert.target_company, larkhill_fit)

                    # Hard cap: if SEPARATION_COMPLEXITY = 0, no SMO
                    # services are needed — discard.
                    if sep_score == 0:
                        larkhill_fit = min(larkhill_fit, 25)
                        recommended = "discard"
                        logger.info("  SEP_COMPLEXITY=0 cap: %s → larkhill_fit=%d, discard",
                                    alert.target_company, larkhill_fit)

                    qualified.append(QualifiedAlert(
                        **alert.model_dump(),
                        larkhill_fit=larkhill_fit,
                        pe_buyer_score=pe_buyer_score,
                        separation_complexity_score=sep_score,
                        deal_size_score=r.get("deal_size_score", 0),
                        geography_score=r.get("geography_score", 0),
                        timing_score=r.get("timing_score", 0),
                        pe_firm=r.get("pe_firm", ""),
                        recommended_action=recommended,
                    ))
                else:
                    qualified.append(QualifiedAlert(**alert.model_dump()))

            return qualified

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("Qualifier response parse error (attempt %d/%d): %s",
                           attempt, _MAX_RETRIES, e)
        except anthropic.APIError as e:
            logger.warning("Anthropic API error in qualifier (attempt %d/%d): %s",
                           attempt, _MAX_RETRIES, e)

    logger.error("All %d qualifier attempts failed for batch of %d alerts",
                 _MAX_RETRIES, len(alerts))
    return [QualifiedAlert(**a.model_dump()) for a in alerts]


def qualify_alerts(alerts: list[DealAlert]) -> list[QualifiedAlert]:
    """Qualify all classifier-positive alerts using Opus. Returns list of QualifiedAlerts."""
    if not alerts:
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = anthropic.Anthropic(api_key=api_key) if api_key else None

    logger.info("Qualifying %d alerts with Opus in batches of %d", len(alerts), _BATCH_SIZE)
    all_qualified: list[QualifiedAlert] = []

    for i in range(0, len(alerts), _BATCH_SIZE):
        batch = alerts[i:i + _BATCH_SIZE]
        logger.info("  Qualifying batch %d/%d (%d alerts)",
                     i // _BATCH_SIZE + 1,
                     (len(alerts) + _BATCH_SIZE - 1) // _BATCH_SIZE,
                     len(batch))
        qualified = _qualify_batch(batch, client=client)
        all_qualified.extend(qualified)

    pursue = sum(1 for a in all_qualified if a.recommended_action == "pursue")
    monitor = sum(1 for a in all_qualified if a.recommended_action == "monitor")
    discard = sum(1 for a in all_qualified if a.recommended_action == "discard")
    logger.info("Qualification complete: %d pursue, %d monitor, %d discard",
                pursue, monitor, discard)
    return all_qualified
