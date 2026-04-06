"""Haiku-based classification of articles for carve-out deals."""

from __future__ import annotations

import json
import logging
import os

import anthropic

from .models import Article, DealAlert, DealStage, DealType

logger = logging.getLogger(__name__)

_MODEL = "claude-sonnet-4-6"
_BATCH_SIZE = 20
_MAX_RETRIES = 3

# Module-level token counters for cost reporting
token_usage = {"input": 0, "output": 0}

_SYSTEM_PROMPT = """You classify deal announcements. For each article, determine if it describes a deal involving the **separation of a division, subsidiary, or business unit** from a parent company.

DEAL TYPES (classify as one of these, or "none"):

1. "corporate_carveout" — A PE firm acquires a division/unit from a CORPORATE parent. The parent continues to exist. The PE firm is the buyer.
   Example: "Apollo acquires Safety Products division from 3M"

2. "portco_carveout" — A PE firm acquires a division/unit from another PE firm's PORTFOLIO COMPANY (not the whole portco). The portco is the corporate parent. Look for "carve-out from [PE]'s portco" or "divesting [unit] from [portfolio company]" language.
   Example: "Inflexion acquires Marioff in carve-out from Sentinel Capital Partners' portco"

3. "corporate_divestiture" — A NON-PE buyer (corporate, trade buyer, consortium, sovereign wealth fund) acquires a division/unit from a corporate parent. Same separation complexity as a carve-out, but the buyer is not a PE firm.
   Example: "Henkel acquires industrial adhesives division from BASF"

"none" — Not a separation deal. See EXCLUDE rules below.

INCLUDE (these ARE separation deals — classify by buyer type):
- Any entity acquires a [division/unit/business/subsidiary/segment] from a [parent company]
- Any entity completes purchase of a [business unit] from a [corporate parent]
- Any entity buys a [brand/segment] being divested by a [parent]
- PE firm acquires a division/unit being carved out of another PE firm's portfolio company (portco_carveout)
- Corporate/trade buyer acquires a division from another corporate (corporate_divestiture)
- Division being sold as part of a corporate restructuring or strategic review

EXCLUDE (these are NOT separation deals — classify as "none"):
- Secondary buyout: PE firm buys an entire company from ANOTHER PE firm (PE-to-PE, whole company)
- Take-private: PE firm acquires an entire publicly-listed company
- Platform acquisition: PE portfolio company acquires another company (bolt-on)
- Portfolio exit: PE firm SELLS or divests an entire portfolio company (the PE firm is the seller)
- Minority investment: Any entity takes a minority stake or makes a growth investment
- Whole-company acquisition: Any entity acquires an entire standalone company (not a division carved out of a parent). This includes companies being sold by their owners/shareholders even if the word "sale" or "divestiture" appears — if the ENTIRE company is being sold, it is NOT a separation deal.
- Whole-company sale by current owners: A company's shareholders (PE, family, founders) sell the ENTIRE company. The company already operates independently with its own IT, finance, HR, brand. No separation from a parent is required. Example: "Consortium agrees to acquire AA plc" — AA is a standalone company, not a division of a larger parent.
- Fund news: PE firm raises a new fund, closes a fund, hires staff
- IPO: portfolio company goes public
- Asset/claim transfer: Company acquires exploration rights, mineral claims, patent portfolios, or other assets that do not constitute an operating business with employees, systems, and infrastructure
- Standalone public company acquisition: Any entity acquires an entire company that already operates independently (has its own public listing, management team, IT, finance, HR). The key test: does the target need to be SEPARATED from a parent's shared infrastructure? If it already operates independently, it is not a separation deal.
- Self-contained subsidiary with no separation complexity: Parent sells a subsidiary that operates entirely independently (e.g., a sports franchise, a standalone brand) with no shared IT, finance, HR, or operational infrastructure to untangle
- PE firm selling a whole portfolio company: If the target is the ENTIRE portco (the whole company), this is a secondary buyout or PE exit — NOT a separation deal. The portco already runs independently. BUT if the target is a division/unit being carved out FROM the portco (not the whole portco), it IS a separation deal. Ask: is the target the WHOLE company or just a PART of it?

KEY TEST: Does the target need to be **SEPARATED** from a larger entity's shared infrastructure (IT, finance, HR, legal, operations)? If yes → classify by buyer type. If the target already operates independently → "none".

CRITICAL: You must be able to identify a SPECIFIC, NAMED seller/parent company from the article text. If you cannot determine who is divesting — if you would need to write "Unknown", "Unknown corporate parent", "Unknown (divesting parent)", or similar — classify as "none". A PE firm acquiring a standalone company where you can't identify the divesting parent is almost certainly a regular buyout, not a carve-out.

For each article, respond with a JSON object:
{
  "deal_type": "corporate_carveout" or "portco_carveout" or "corporate_divestiture" or "none",
  "stage": "signing" or "closing" or null,
  "target_company": "name of the division/unit being acquired" or "",
  "seller": "name of the parent company selling" or "",
  "buyer": "name of the acquiring entity (PE firm or corporate)" or "",
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
6. "Nautic Partners Announces Sale of Property Management Company AKAM"
7. "Eurazeo Invests in Groupe Pierre Schmidt Through a Sponsorless Transaction"
8. "GovCIO, a Welsh Carson portfolio company, to Acquire SoldierPoint Digital Health"
9. "SAGA Metals Acquires Strategic Titanium Assets from Rio Tinto in Quebec"
10. "Germany's Henkel in $1.4 billion deal to acquire hair care brand Olaplex"
11. "Diageo Sells Cricket Team to Blackstone-Backed Consortium for $1.8 Billion"
12. "Bidders circle as EDF's renewable assets in the US go on sale"
13. "Inflexion acquires Marioff in carve-out from Sentinel Capital Partners' portco"
14. "Henkel to acquire BASF's industrial adhesives division for €1.2 billion"
15. "Johnson Controls completes sale of Global Products business to Bosch"
16. "Siemens sells logistics unit to DSV in €1.15 billion deal"
17. "Peak Rock Capital affiliate acquires Hasa, Inc."
18. "Aurora Capital Partners Acquires GenServe, a Leading Infrastructure Services Provider"
19. "Consortium including Fortress and TowerBrook agrees to acquire AA plc for £7 billion"
20. "Nordic Capital completes sale of Geomatikk to Riksmätningen"
"""

_FEW_SHOT_ASSISTANT = """[
  {"deal_type": "corporate_carveout", "stage": "signing", "target_company": "Safety Products division", "seller": "3M", "buyer": "Blackstone", "confidence": 95, "reasoning": "PE firm acquiring a division from a corporate parent — classic corporate carve-out signing."},
  {"deal_type": "corporate_carveout", "stage": "closing", "target_company": "tea business", "seller": "Unilever", "buyer": "KKR", "confidence": 92, "reasoning": "PE firm completed acquisition of a business unit from a corporate parent — corporate carve-out closing."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 90, "reasoning": "Seller is Bain Capital (another PE firm) and Shutterfly is the whole company — this is a secondary buyout, not a separation deal."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 95, "reasoning": "Taking a whole public company private — not a separation deal."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 98, "reasoning": "Fund news / fundraising — not a deal announcement at all."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 92, "reasoning": "Nautic Partners is the SELLER (a PE firm exiting a portfolio company) — this is a portfolio exit, not a separation deal."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 88, "reasoning": "Eurazeo is making a minority/growth investment, not acquiring a division — not a separation deal."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 85, "reasoning": "A PE portfolio company (GovCIO) acquiring a standalone company — bolt-on acquisition, not a separation from a parent."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 90, "reasoning": "Asset/claim transfer — mineral exploration claims, not an operating business. No separation complexity."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 92, "reasoning": "Olaplex is a standalone Nasdaq-listed company. Already operates independently — no separation from a corporate parent required."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 88, "reasoning": "Self-contained sports franchise — operates independently with no shared infrastructure requiring separation."},
  {"deal_type": "corporate_carveout", "stage": "signing", "target_company": "EDF Power Solutions North America", "seller": "EDF", "buyer": "Unknown (bidders circling)", "confidence": 82, "reasoning": "EDF Power Solutions North America is an integrated operating platform within state-owned EDF. Separation from EDF's corporate structure would require IT, finance, HR, legal, commercial, and operational separation."},
  {"deal_type": "portco_carveout", "stage": "closing", "target_company": "Marioff", "seller": "Sentinel Capital Partners' portfolio company", "buyer": "Inflexion", "confidence": 85, "reasoning": "Marioff is a division being carved out of a Sentinel Capital Partners portfolio company — not the whole portco. The portco is the corporate parent. Target needs to be separated from the portco's shared infrastructure."},
  {"deal_type": "corporate_divestiture", "stage": "signing", "target_company": "industrial adhesives division", "seller": "BASF", "buyer": "Henkel", "confidence": 93, "reasoning": "Corporate buyer (Henkel) acquiring a division from a corporate parent (BASF). Division embedded in BASF's operations needs separation — corporate divestiture."},
  {"deal_type": "corporate_divestiture", "stage": "closing", "target_company": "Global Products business", "seller": "Johnson Controls", "buyer": "Bosch", "confidence": 91, "reasoning": "Corporate buyer (Bosch) completed acquisition of a business unit from Johnson Controls. Division requires separation from parent's shared infrastructure — corporate divestiture."},
  {"deal_type": "corporate_divestiture", "stage": "signing", "target_company": "logistics unit", "seller": "Siemens", "buyer": "DSV", "confidence": 90, "reasoning": "Corporate buyer (DSV) acquiring a division from a corporate parent (Siemens). Logistics unit embedded in Siemens infrastructure needs separation — corporate divestiture."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 85, "reasoning": "PE firm acquiring a standalone company. No specific corporate parent identified as divesting — cannot confirm this is a carve-out vs a regular buyout."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 82, "reasoning": "PE firm acquiring an infrastructure services company. No named parent company selling a division — this appears to be a standard PE acquisition of a standalone business."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 95, "reasoning": "AA plc is a standalone public company being acquired in its entirety. It already operates independently with its own IT, finance, HR, and brand. No separation from a parent company's shared infrastructure is needed — this is a take-private, not a carve-out."},
  {"deal_type": "none", "stage": null, "target_company": "", "seller": "", "buyer": "", "confidence": 92, "reasoning": "Nordic Capital is selling the ENTIRE company Geomatikk (a portfolio company). This is a PE exit / secondary sale, not a carve-out. Geomatikk is the whole portco being sold, not a division being carved out from a larger portco. No separation from a parent's shared infrastructure is required."}
]"""


def classify_batch(articles: list[Article], client: anthropic.Anthropic | None = None) -> list[DealAlert]:
    """Classify a batch of articles using Claude Sonnet."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key and not client:
        logger.error("ANTHROPIC_API_KEY not set")
        return [DealAlert(article=a) for a in articles]

    if client is None:
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
                system=[{
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[
                    {"role": "user", "content": _FEW_SHOT_USER},
                    {"role": "assistant", "content": [{
                        "type": "text",
                        "text": _FEW_SHOT_ASSISTANT,
                        "cache_control": {"type": "ephemeral"},
                    }]},
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
            cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
            cache_create = getattr(response.usage, "cache_creation_input_tokens", 0) or 0
            token_usage["input"] += input_tokens
            token_usage["output"] += output_tokens
            logger.debug("LLM call: %d input, %d output, %d cache_read, %d cache_create",
                         input_tokens, output_tokens, cache_read, cache_create)

            alerts = []
            for i, article in enumerate(articles):
                if i < len(results):
                    r = results[i]
                    stage = None
                    if r.get("stage") == "signing":
                        stage = DealStage.SIGNING
                    elif r.get("stage") == "closing":
                        stage = DealStage.CLOSING

                    # Map deal_type string to DealType enum
                    deal_type_str = r.get("deal_type", "none")
                    is_carveout = deal_type_str != "none"
                    deal_type = None
                    if is_carveout:
                        try:
                            deal_type = DealType(deal_type_str)
                        except ValueError:
                            logger.warning("Unknown deal_type '%s' — treating as none", deal_type_str)
                            is_carveout = False

                    alerts.append(DealAlert(
                        article=article,
                        is_carveout=is_carveout,
                        deal_type=deal_type,
                        stage=stage,
                        target_company=r.get("target_company", ""),
                        seller=r.get("seller", ""),
                        buyer=r.get("buyer", ""),
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

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = anthropic.Anthropic(api_key=api_key) if api_key else None

    logger.info("Classifying %d articles in batches of %d", len(articles), _BATCH_SIZE)
    all_alerts: list[DealAlert] = []

    for i in range(0, len(articles), _BATCH_SIZE):
        batch = articles[i:i + _BATCH_SIZE]
        logger.info("  Classifying batch %d/%d (%d articles)",
                     i // _BATCH_SIZE + 1,
                     (len(articles) + _BATCH_SIZE - 1) // _BATCH_SIZE,
                     len(batch))
        alerts = classify_batch(batch, client=client)
        all_alerts.extend(alerts)

    deals = [a for a in all_alerts if a.deal_type is not None]
    logger.info("Classification complete: %d separation deals found out of %d articles",
                len(deals), len(articles))
    return all_alerts
