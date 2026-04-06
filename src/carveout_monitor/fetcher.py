"""Fetch full article body text for improved classification accuracy."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from .models import Article

logger = logging.getLogger(__name__)

_MAX_WORKERS = 10
_TIMEOUT = 15
_MAX_CHARS = 6000
_MAX_WORDS = 1500

_STRIP_TAGS = {"nav", "footer", "header", "aside", "script", "style", "noscript", "form"}
_STRIP_ROLES = {"navigation", "banner", "contentinfo", "complementary"}
_STRIP_CLASSES = {"sidebar", "nav", "footer", "header", "menu", "ad", "ads", "advertisement",
                  "cookie", "popup", "modal", "social", "share", "comment", "comments"}


def _should_strip(tag) -> bool:
    """Check if a tag is nav/footer/sidebar/ads that should be removed."""
    if tag.name in _STRIP_TAGS:
        return True
    role = (tag.get("role") or "").lower()
    if role in _STRIP_ROLES:
        return True
    classes = " ".join(tag.get("class", [])).lower()
    if any(c in classes for c in _STRIP_CLASSES):
        return True
    return False


def _extract_body(html: str) -> str:
    """Extract main article body text from HTML, stripping boilerplate."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove boilerplate elements
    for tag in soup.find_all(lambda t: _should_strip(t)):
        tag.decompose()

    # Strategy 1: <article> tag
    article = soup.find("article")
    if article:
        text = article.get_text(separator=" ", strip=True)
        if len(text.split()) >= 50:
            return _cap_text(text)

    # Strategy 2: largest <div> with <p> children
    best_div = None
    best_p_count = 0
    for div in soup.find_all("div"):
        p_children = div.find_all("p", recursive=False)
        if len(p_children) > best_p_count:
            best_p_count = len(p_children)
            best_div = div
    if best_div and best_p_count >= 2:
        text = best_div.get_text(separator=" ", strip=True)
        if len(text.split()) >= 50:
            return _cap_text(text)

    # Strategy 3: all <p> tags
    paragraphs = soup.find_all("p")
    if paragraphs:
        text = " ".join(p.get_text(strip=True) for p in paragraphs)
        if text.strip():
            return _cap_text(text)

    return ""


def _cap_text(text: str) -> str:
    """Cap text to _MAX_WORDS words and _MAX_CHARS characters."""
    words = text.split()
    if len(words) > _MAX_WORDS:
        text = " ".join(words[:_MAX_WORDS])
    return text[:_MAX_CHARS]


def _fetch_one(article: Article) -> Article:
    """Fetch body text for a single article. Returns article with updated summary on success."""
    try:
        resp = requests.get(article.url, timeout=_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (compatible; CarveoutMonitor/1.0)",
        })
        resp.raise_for_status()
        body = _extract_body(resp.text)
        if body:
            article.summary = body
    except Exception as e:
        logger.debug("Failed to fetch %s: %s", article.url, e)
    return article


def fetch_article_bodies(articles: list[Article]) -> list[Article]:
    """Fetch full body text for articles in parallel. Modifies articles in-place.

    On failure, articles keep their existing summary — nothing is dropped.
    """
    if not articles:
        return articles

    logger.info("Fetching body text for %d articles (max %d workers)", len(articles), _MAX_WORKERS)
    fetched = 0

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, a): a for a in articles}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result.summary:
                    fetched += 1
            except Exception:
                pass

    logger.info("Fetched body text for %d/%d articles", fetched, len(articles))
    return articles
