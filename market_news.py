import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import re
import html

def clean_text(text):
    text = re.sub('<[^<]+?>', '', text)
    return text.strip()

def fetch_rss(query):
    url = f"https://news.google.com/rss/search?q={query}&hl=en-AU&gl=AU&ceid=AU:en"
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code != 200: return []
        
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall('./channel/item'):
            title = item.find('title').text if item.find('title') is not None else "Unknown"
            link = item.find('link').text if item.find('link') is not None else "#"
            pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ""
            source = item.find('source').text if item.find('source') is not None else "Google News"
            
            title = clean_text(title)
            if " - " in title: title = title.rsplit(" - ", 1)[0]
                
            items.append({'title': title, 'link': link, 'published': pub_date, 'publisher': source})
            if len(items) >= 8: break # Limit per feed
        return items
    except Exception as e:
        print(f"RSS Error ({query}): {e}")
        return []

def analyze_articles(items):
    # Keywords
    bullish = ["surge", "jump", "hike", "record", "crisis", "conflict", "war", "cut", "opec", "climb", "rally", "high", "expensive", "soar", "spike", "inflation"]
    bearish = ["drop", "fall", "plunge", "slide", "crash", "surplus", "glut", "low", "cheap", "recession", "weak", "inventory", "build", "dip", "ease"]

    analyzed = []
    for item in items:
        text = item['title'].lower()
        score = 0
        if any(k in text for k in bullish): score -= 1
        if any(k in text for k in bearish): score += 1
        
        if score < 0: tag = "🔴 High Price Pressure"
        elif score > 0: tag = "🟢 Price Relief"
        else: tag = "⚪ Neutral"
        
        item['sentiment'] = tag
        analyzed.append(item)
    return analyzed

# --- Cache ---
_market_news_cache = None
_market_news_cache_time = None

def get_market_news():
    """
    Fetches Global and Domestic news separately.
    """
    global _market_news_cache, _market_news_cache_time
    now = datetime.now()
    if _market_news_cache is not None and _market_news_cache_time is not None:
        if (now - _market_news_cache_time).total_seconds() < 3600: # 1 hour cache
            return _market_news_cache.copy()

    # 1. Global
    global_raw = fetch_rss("Global+Oil+Price+Energy+Market")
    global_news = analyze_articles(global_raw)
    
    # 2. Domestic
    domestic_raw = fetch_rss("Australia+Petrol+Price+Fuel+Market")
    domestic_news = analyze_articles(domestic_raw)
    
    res = {
        "global": global_news,
        "domestic": domestic_news
    }
    
    if global_news or domestic_news:
        _market_news_cache = res
        _market_news_cache_time = now
        
    return res
