"""
Market News Engine — Multi-Source Fuel & Energy News with Sentiment Analysis
Aggregates news from Google News, Reuters Energy, and OilPrice.com RSS feeds,
applying weighted keyword sentiment scoring with impact vector classification.
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import re
import html
import logging

logger = logging.getLogger(__name__)


# ========================================================================== #
#  Sentiment Analysis Configuration
# ========================================================================== #

# Bullish keywords → push prices UP (negative sentiment for consumers)
BULLISH_KEYWORDS = {
    "surge": 0.8,
    "spike": 0.9,
    "rally": 0.7,
    "soar": 0.8,
    "jump": 0.6,
    "hike": 0.7,
    "record high": 1.0,
    "opec cut": 0.9,
    "production cut": 0.85,
    "shortage": 0.9,
    "sanctions": 0.8,
    "war": 0.9,
    "conflict": 0.75,
    "supply disruption": 1.0,
    "supply crunch": 0.95,
    "crisis": 0.8,
    "embargo": 0.9,
    "outage": 0.7,
    "refinery fire": 0.85,
    "pipeline attack": 0.9,
    "hurricane": 0.6,
    "geopolitical": 0.5,
    "inflation": 0.4,
    "expensive": 0.5,
    "high demand": 0.6,
    "tight supply": 0.8,
    "climb": 0.5,
}

# Bearish keywords → push prices DOWN (positive sentiment for consumers)
BEARISH_KEYWORDS = {
    "crash": 0.9,
    "slump": 0.8,
    "plunge": 0.85,
    "plummet": 0.9,
    "collapse": 0.9,
    "oversupply": 0.8,
    "surplus": 0.7,
    "glut": 0.8,
    "ceasefire": 0.7,
    "peace": 0.6,
    "peace deal": 0.75,
    "production increase": 0.8,
    "output boost": 0.75,
    "inventory build": 0.6,
    "stockpile": 0.5,
    "recession": 0.7,
    "demand weakness": 0.7,
    "weak demand": 0.65,
    "drop": 0.5,
    "fall": 0.45,
    "slide": 0.5,
    "dip": 0.3,
    "ease": 0.4,
    "cheap": 0.5,
    "low": 0.3,
    "relief": 0.5,
    "opec increase": 0.8,
}

# Impact vector keywords
IMPACT_VECTORS = {
    "supply": [
        "supply", "production", "output", "refinery", "pipeline",
        "stockpile", "inventory", "shortage", "surplus", "opec",
        "barrel", "export", "import",
    ],
    "demand": [
        "demand", "consumption", "driving", "travel", "economic",
        "growth", "recession", "gdp", "recovery",
    ],
    "policy": [
        "tax", "excise", "regulation", "policy", "subsidy",
        "government", "legislation", "mandate", "carbon",
    ],
    "geopolitical": [
        "war", "conflict", "sanctions", "embargo", "attack",
        "military", "geopolitical", "missile", "iran", "russia",
        "ukraine", "middle east", "houthi", "strait",
    ],
    "currency": [
        "dollar", "aud", "usd", "exchange", "forex", "currency",
        "fed", "rba", "interest rate", "inflation",
    ],
}

# ========================================================================== #
#  RSS Feed Configuration
# ========================================================================== #

RSS_SOURCES = {
    "google_news_global": {
        "url": "https://news.google.com/rss/search?q=Global+Oil+Price+Energy+Market&hl=en-AU&gl=AU&ceid=AU:en",
        "category": "global",
        "source_name": "Google News",
        "max_items": 8,
    },
    "google_news_domestic": {
        "url": "https://news.google.com/rss/search?q=Australia+Petrol+Price+Fuel+Market&hl=en-AU&gl=AU&ceid=AU:en",
        "category": "domestic",
        "source_name": "Google News",
        "max_items": 8,
    },
    "reuters_energy": {
        "url": "https://news.google.com/rss/search?q=Reuters+energy+oil+crude&hl=en-AU&gl=AU&ceid=AU:en",
        "category": "global",
        "source_name": "Reuters via Google",
        "max_items": 5,
    },
    "oilprice_rss": {
        "url": "https://oilprice.com/rss/main",
        "category": "global",
        "source_name": "OilPrice.com",
        "max_items": 5,
    },
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ========================================================================== #
#  Helper Functions
# ========================================================================== #


def _clean_text(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^<]+?>", "", text)
    return text.strip()


def _fetch_rss(url: str, max_items: int = 8, source_name: str = "") -> list[dict]:
    """
    Fetch and parse an RSS feed, returning raw article dicts.
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code != 200:
            logger.warning(
                "RSS feed returned %d for %s", resp.status_code, source_name
            )
            return []

        root = ET.fromstring(resp.content)
        items: list[dict] = []

        for item_el in root.findall("./channel/item"):
            title_el = item_el.find("title")
            link_el = item_el.find("link")
            pub_el = item_el.find("pubDate")
            source_el = item_el.find("source")

            title = _clean_text(title_el.text) if title_el is not None and title_el.text else "Unknown"
            link = link_el.text if link_el is not None and link_el.text else "#"
            published = pub_el.text if pub_el is not None and pub_el.text else ""
            publisher = (
                source_el.text
                if source_el is not None and source_el.text
                else source_name or "Unknown"
            )

            # Strip trailing " - Source" from Google News titles
            if " - " in title:
                title = title.rsplit(" - ", 1)[0]

            items.append({
                "title": title,
                "link": link,
                "published": published,
                "publisher": publisher,
            })

            if len(items) >= max_items:
                break

        return items

    except Exception as e:
        logger.error("RSS fetch error (%s): %s", source_name, e)
        return []


# ========================================================================== #
#  Sentiment & Impact Analysis
# ========================================================================== #


def _score_sentiment(title: str) -> tuple[float, str]:
    """
    Compute weighted sentiment score for a headline.

    Returns
    -------
    (sentiment, magnitude)
      sentiment: float from -1.0 (very bullish / price-up) to +1.0 (very bearish / price-down)
      magnitude: "minor" | "moderate" | "major"
    """
    text_lower = title.lower()

    bullish_score = 0.0
    bearish_score = 0.0
    match_count = 0

    for keyword, weight in BULLISH_KEYWORDS.items():
        if keyword in text_lower:
            bullish_score += weight
            match_count += 1

    for keyword, weight in BEARISH_KEYWORDS.items():
        if keyword in text_lower:
            bearish_score += weight
            match_count += 1

    if match_count == 0:
        return 0.0, "minor"

    # Net sentiment: negative = bullish (bad for consumers), positive = bearish (good)
    raw = bearish_score - bullish_score
    # Normalise to [-1, 1]
    max_possible = max(bullish_score + bearish_score, 1.0)
    sentiment = max(-1.0, min(1.0, raw / max_possible))

    # Magnitude
    total_weight = bullish_score + bearish_score
    if total_weight >= 1.5:
        magnitude = "major"
    elif total_weight >= 0.6:
        magnitude = "moderate"
    else:
        magnitude = "minor"

    return round(sentiment, 3), magnitude


def _classify_impact_vector(title: str) -> str:
    """Classify an article's impact vector based on keyword matching."""
    text_lower = title.lower()
    scores: dict[str, int] = {v: 0 for v in IMPACT_VECTORS}

    for vector, keywords in IMPACT_VECTORS.items():
        for kw in keywords:
            if kw in text_lower:
                scores[vector] += 1

    best = max(scores, key=lambda k: scores[k])
    return best if scores[best] > 0 else "supply"


def _generate_analysis(title: str, sentiment: float, vector: str) -> str:
    """Generate a brief analytical sentence about why this article matters."""
    text_lower = title.lower()

    if vector == "geopolitical":
        if sentiment < -0.3:
            return "Geopolitical tensions may disrupt supply chains, putting upward pressure on fuel costs."
        elif sentiment > 0.3:
            return "Easing geopolitical risks could stabilise supply routes and moderate prices."
        return "Geopolitical developments being monitored for potential supply-chain impact."

    if vector == "supply":
        if sentiment < -0.3:
            return "Supply-side tightening typically flows through to higher wholesale and retail prices within 7-14 days."
        elif sentiment > 0.3:
            return "Increased supply availability should ease wholesale prices, benefiting consumers with a 1-2 week lag."
        return "Supply conditions appear stable; monitoring for changes."

    if vector == "demand":
        if sentiment < -0.3:
            return "Rising demand may push prices higher as refiners compete for crude supply."
        elif sentiment > 0.3:
            return "Weakening demand could ease price pressure, especially for discretionary fuels."
        return "Demand signals are mixed; no clear directional pressure identified."

    if vector == "policy":
        if sentiment < -0.3:
            return "Policy changes could increase regulatory costs, feeding through to pump prices."
        elif sentiment > 0.3:
            return "Policy interventions may provide price relief through subsidies or tax adjustments."
        return "Policy developments noted; monitoring for direct impact on Australian fuel markets."

    if vector == "currency":
        if sentiment < -0.3:
            return "Currency weakness makes fuel imports more expensive in local terms."
        elif sentiment > 0.3:
            return "A stronger local currency reduces the effective cost of imported fuel."
        return "Currency movements within normal ranges; limited direct price impact."

    return "Market development noted; assessing potential impact on Australian fuel prices."


def _analyse_articles(items: list[dict]) -> list[dict]:
    """
    Enrich a list of raw article dicts with sentiment analysis.
    """
    analysed: list[dict] = []

    for item in items:
        title = item.get("title", "")
        sentiment, magnitude = _score_sentiment(title)
        vector = _classify_impact_vector(title)
        analysis = _generate_analysis(title, sentiment, vector)

        # Legacy tag for backward compatibility
        if sentiment < -0.2:
            sentiment_tag = "🔴 High Price Pressure"
        elif sentiment > 0.2:
            sentiment_tag = "🟢 Price Relief"
        else:
            sentiment_tag = "⚪ Neutral"

        analysed.append({
            "title": title,
            "link": item.get("link", "#"),
            "published": item.get("published", ""),
            "publisher": item.get("publisher", "Unknown"),
            "sentiment": sentiment,
            "sentiment_tag": sentiment_tag,
            "magnitude": magnitude,
            "impact_vector": vector,
            "analysis": analysis,
        })

    return analysed


def _compute_overall_sentiment(articles: list[dict]) -> float:
    """Weighted average sentiment across articles, weighted by magnitude."""
    if not articles:
        return 0.0

    magnitude_weights = {"major": 3.0, "moderate": 2.0, "minor": 1.0}
    total_weight = 0.0
    weighted_sum = 0.0

    for article in articles:
        w = magnitude_weights.get(article.get("magnitude", "minor"), 1.0)
        weighted_sum += article.get("sentiment", 0.0) * w
        total_weight += w

    if total_weight == 0:
        return 0.0

    return round(weighted_sum / total_weight, 3)


def _generate_summary(articles: list[dict], category: str) -> str:
    """Generate a brief summary sentence for a news category."""
    if not articles:
        return f"No {category} news articles available at this time."

    overall = _compute_overall_sentiment(articles)
    count = len(articles)
    major_count = sum(1 for a in articles if a.get("magnitude") == "major")

    if overall < -0.3:
        tone = "predominantly bearish for consumers (price-up pressure)"
    elif overall > 0.3:
        tone = "leaning bullish for consumers (price-down pressure)"
    elif overall < -0.1:
        tone = "slightly negative with mild upward price pressure"
    elif overall > 0.1:
        tone = "slightly positive with mild downward price pressure"
    else:
        tone = "broadly neutral with no strong directional signal"

    summary = f"Across {count} {category} articles, sentiment is {tone}."
    if major_count > 0:
        summary += f" {major_count} high-impact {'story' if major_count == 1 else 'stories'} detected."

    return summary


# ========================================================================== #
#  Cache
# ========================================================================== #

_market_news_cache: dict | None = None
_market_news_cache_time: datetime | None = None


# ========================================================================== #
#  Public API
# ========================================================================== #


def get_market_news() -> dict:
    """
    Fetch and analyse market news from multiple RSS sources.

    Returns
    -------
    dict with keys:
        'global': {
            'articles': list[dict],  # Analysed article dicts
            'overall_sentiment': float,
            'summary': str,
        },
        'domestic': {
            'articles': list[dict],
            'overall_sentiment': float,
            'summary': str,
        }
    """
    global _market_news_cache, _market_news_cache_time
    now = datetime.now()

    if _market_news_cache is not None and _market_news_cache_time is not None:
        if (now - _market_news_cache_time).total_seconds() < 3600:  # 1-hour cache
            return _market_news_cache.copy()

    global_raw: list[dict] = []
    domestic_raw: list[dict] = []

    # Fetch from all configured RSS sources
    for feed_key, feed_cfg in RSS_SOURCES.items():
        raw_items = _fetch_rss(
            url=feed_cfg["url"],
            max_items=feed_cfg["max_items"],
            source_name=feed_cfg["source_name"],
        )

        if feed_cfg["category"] == "global":
            global_raw.extend(raw_items)
        else:
            domestic_raw.extend(raw_items)

    # De-duplicate by title (across sources)
    global_raw = _deduplicate(global_raw)
    domestic_raw = _deduplicate(domestic_raw)

    # Analyse
    global_articles = _analyse_articles(global_raw)
    domestic_articles = _analyse_articles(domestic_raw)

    result = {
        "global": {
            "articles": global_articles,
            "overall_sentiment": _compute_overall_sentiment(global_articles),
            "summary": _generate_summary(global_articles, "global energy"),
        },
        "domestic": {
            "articles": domestic_articles,
            "overall_sentiment": _compute_overall_sentiment(domestic_articles),
            "summary": _generate_summary(domestic_articles, "Australian fuel"),
        },
    }

    if global_articles or domestic_articles:
        _market_news_cache = result
        _market_news_cache_time = now

    return result


def _deduplicate(items: list[dict]) -> list[dict]:
    """Remove duplicate articles based on normalised title."""
    seen: set[str] = set()
    unique: list[dict] = []
    for item in items:
        key = item.get("title", "").lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    return unique
