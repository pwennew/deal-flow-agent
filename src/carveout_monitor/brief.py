"""Deal brief auto-generation for qualified carve-out alerts."""

from __future__ import annotations

import json
import logging
import os

import anthropic

from .models import QualifiedAlert

logger = logging.getLogger(__name__)

_MODEL = "claude-opus-4-6"
_EXTRACTION_MODEL = "claude-opus-4-6"  # Tier 1 fields — go into PE Partner emails
_MAX_RETRIES = 2

_SYSTEM_PROMPT = """You are a senior business development analyst at Larkhill & Company, a separation execution firm that runs Separation Management Offices (SMOs) for PE-backed carve-outs.

Generate a structured deal brief for the carve-out opportunity described below. The brief should be actionable for a business development lead preparing outreach to the PE buyer's deal team.

Structure your response with these sections:

## Deal Basics
- Target company/division
- Seller (corporate parent)
- Buyer (PE firm, if identified)
- Deal value (if known)
- Deal stage (signing/closing/exploration)
- Geography

## Business Overview
Based on the article context, describe what the target business does. Include industry, approximate scale, and any known details about the business.

## Buyer Thesis
Why would the PE firm want this asset? What value creation levers are likely in play?

## Separation Complexity Estimate
Assess which of these 8 Carve-out Delivery System (CDS) domains are likely involved in the separation:
1. **IT & Digital** — ERP, CRM, data centres, cybersecurity, application portfolio
2. **Finance** — GL, AP/AR, treasury, tax, reporting, intercompany elimination
3. **HR & People** — Payroll, benefits, HRIS, org design, retention
4. **Legal & Compliance** — IP, contracts, regulatory licences, data privacy
5. **Operations & Supply Chain** — Manufacturing, logistics, procurement, vendor contracts
6. **Commercial** — Sales, marketing, customer contracts, brand licensing
7. **Real Estate & Facilities** — Shared offices, labs, warehouses, lease assignments
8. **Programme Management** — TSA negotiation, Day 1 readiness, PMO, governance

For each relevant domain, provide a one-line assessment of likely separation work.

## Larkhill Positioning
- How would Larkhill pitch SMO services for this deal?
- Who is the likely entry point (Operating Partner, Deal Lead, Portfolio Ops)?
- What is the urgency/timing?

Keep the brief concise but substantive — roughly 500-800 words total."""


def generate_deal_brief(alert: QualifiedAlert) -> str:
    """Generate a deal brief for a qualified carve-out alert.

    Returns the brief text, or an empty string on failure.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set — cannot generate deal brief")
        return ""

    client = anthropic.Anthropic(api_key=api_key)

    a = alert.article
    user_msg = (
        f"Generate a deal brief for this carve-out opportunity:\n\n"
        f"Article title: {a.title}\n"
        f"Article summary: {a.summary[:500] if a.summary else 'N/A'}\n"
        f"Source URL: {a.url}\n"
        f"Published: {a.published.strftime('%Y-%m-%d') if a.published else 'N/A'}\n\n"
        f"Classifier output:\n"
        f"- Target: {alert.target_company}\n"
        f"- Seller: {alert.seller}\n"
        f"- PE Firm (buyer): {alert.pe_firm or 'Not identified'}\n"
        f"- Deal stage: {alert.stage.value if alert.stage else 'Unknown'}\n"
        f"- Larkhill Fit score: {alert.larkhill_fit}/100\n"
        f"- PE Buyer score: {alert.pe_buyer_score}/100\n"
        f"- Separation Complexity score: {alert.separation_complexity_score}/100\n"
        f"- Reasoning: {alert.reasoning}\n"
    )

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model=_MODEL,
                max_tokens=4096,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_msg}],
            )

            brief_text = response.content[0].text.strip()
            logger.info("Generated deal brief for %s (%d chars)",
                        alert.target_company, len(brief_text))
            return brief_text

        except anthropic.APIError as e:
            logger.warning("Deal brief generation error (attempt %d/%d): %s",
                           attempt, _MAX_RETRIES, e)

    logger.error("Failed to generate deal brief for %s after %d attempts",
                 alert.target_company, _MAX_RETRIES)
    return ""


_EXTRACTION_PROMPT = """Extract two fields from this deal brief for use in an automated email sequence. These go directly into a PE Partner's inbox — they must be 100% factually accurate and conversational.

DEAL BRIEF:
{brief_text}

RULES:
1. pain_line: One conversational sentence about the #1 separation complexity. Max 120 characters. Must read like something you'd say on a call, not a database label. Name the specific operational fact that makes separation hard.
   - GOOD: "9 years inside Lonza, 15 sites across 12 countries — deep systems entanglement"
   - GOOD: "5 years on IAC's platform stack — the tech separation is the deal"
   - BAD: "Complex IT separation expected" (generic)

2. buyer_track_record: One natural line naming the buyer's specific prior carve-out deals. This goes into: "I know {{company}} has deep carve-out experience from {{buyer_track_record}}."
   - GOOD: "Kidde from Carrier ($3B), ERIKS, Milacron ($3.8B)"
   - BAD: "Experienced in carve-outs" (no specific deal)
   - If the buyer is unknown or it's an auction, return "Auction — buyer TBC"

Return ONLY valid JSON with these two keys: pain_line, buyer_track_record"""


def extract_hubspot_fields(
    brief_text: str, alert: QualifiedAlert
) -> dict[str, str]:
    """Extract HubSpot email sequence fields from a generated deal brief.

    Returns dict with keys: pain_line, buyer_track_record, outbound_trigger,
    seller, enterprise_value_m. Returns empty dict on failure.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key or not brief_text:
        return {}

    client = anthropic.Anthropic(api_key=api_key)

    # LLM extraction for pain_line and buyer_track_record
    try:
        response = client.messages.create(
            model=_EXTRACTION_MODEL,
            max_tokens=512,
            messages=[{
                "role": "user",
                "content": _EXTRACTION_PROMPT.format(brief_text=brief_text),
            }],
        )
        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        fields = json.loads(raw)
    except (anthropic.APIError, json.JSONDecodeError, IndexError) as e:
        logger.warning("HubSpot field extraction failed: %s", e)
        fields = {}

    # Map deal stage to outbound_trigger and sequence enum values
    trigger_map = {"signing": "Signing", "closing": "Closing"}
    sequence_map = {
        "signing": "Signing-Triggered Outbound",
        "closing": "Close-Triggered Outbound",
    }
    stage_val = alert.stage.value if alert.stage else ""

    result = {
        "pain_line": fields.get("pain_line", ""),
        "buyer_track_record": fields.get("buyer_track_record", ""),
    }
    if stage_val in trigger_map:
        result["outbound_trigger"] = trigger_map[stage_val]
        result["sequence"] = sequence_map[stage_val]

    logger.info("Extracted HubSpot fields for %s: pain_line=%r",
                alert.target_company, result.get("pain_line", "")[:60])
    return result
