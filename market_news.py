import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import re
import html

def clean_text(text):
    # Remove HTML tags if any
    text = re.sub('<[^<]+?>', '', text)
    return text.strip()

def fetch_google_news_rss():
    """
    Fetches energy news from Google News RSS (Australia Edition).
    Returns list of dicts: {'title', 'link', 'published', 'source'}
    """
    # Query: Crude Oil + Fuel Price + Energy Market
    url = "https://news.google.com/rss/search?q=Crude+Oil+Fuel+Price+Australia&hl=en-AU&gl=AU&ceid=AU:en"
    
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return []
            
        root = ET.fromstring(resp.content)
        items = []
        
        # Iterate over channel/item
        for item in root.findall('./channel/item'):
            title = item.find('title').text if item.find('title') is not None else "Unknown"
            link = item.find('link').text if item.find('link') is not None else "#"
            pub_date = item.find('pubDate').text if item.find('pubDate') is not None else ""
            source = item.find('source').text if item.find('source') is not None else "Google News"
            
            # Basic cleaning
            title = clean_text(title)
            # Remove the " - Source" often at the end of Google News titles
            if " - " in title:
                parts = title.rsplit(" - ", 1)
                title = parts[0]
                
            items.append({
                'title': title,
                'link': link,
                'published': pub_date,
                'publisher': source
            })
            
            if len(items) >= 15: break
            
        return items
    except Exception as e:
        print(f"RSS Error: {e}")
        return []

def get_market_sentiment():
    """
    Analyzes sentiment based on RSS headlines.
    """
    news_items = fetch_google_news_rss()
    
    if not news_items:
        return {
            "score": 0, 
            "mood": "Data Unavailable", 
            "color": "#64748b", # Slate 500
            "articles": []
        }

    # Sentiment Logic
    sentiment_score = 0
    
    # Keywords that imply consumers paying MORE (Bad/Bullish for Oil)
    bullish_keywords = [
        "surge", "jump", "hike", "record", "crisis", "conflict", "war", "cut", "opec",
        "climb", "rally", "high", "expensive", "soar", "spike", "inflation", "risk", "turmoil"
    ]
    
    # Keywords that imply consumers paying LESS (Good/Bearish for Oil)
    bearish_keywords = [
        "drop", "fall", "plunge", "slide", "crash", "surplus", "glut", "low", "cheap",
        "recession", "weak", "inventory", "build", "dip", "ease", "stable", "relief"
    ]

    analyzed_news = []
    
    for item in news_items:
        text = item['title'].lower()
        score = 0
        
        # Simple scoring
        if any(k in text for k in bullish_keywords): score -= 1 # Negative for consumer
        if any(k in text for k in bearish_keywords): score += 1 # Positive for consumer
        
        sentiment_score += score
        
        # Tagging
        if score < 0: tag = "🔴 High Price Pressure"
        elif score > 0: tag = "🟢 Price Relief"
        else: tag = "⚪ Neutral"
        
        item['sentiment'] = tag
        analyzed_news.append(item)

    # Normalize Mood
    # Range is roughly -5 to 5
    if sentiment_score <= -3:
        mood = "Market Stress (Prices Rising)"
        color = "#ef4444" # Red
    elif sentiment_score >= 3:
        mood = "Consumer Relief (Prices Falling)"
        color = "#10b981" # Green
    elif sentiment_score < 0:
        mood = "Slightly Inflationary"
        color = "#f59e0b" # Orange
    else:
        mood = "Stable / Mixed"
        color = "#3b82f6" # Blue

    return {
        "score": sentiment_score,
        "mood": mood,
        "color": color,
        "articles": analyzed_news
    }

if __name__ == "__main__":
    # Test
    res = get_market_sentiment()
    print(f"Score: {res['score']} | Mood: {res['mood']}")
    for a in res['articles'][:3]:
        print(f"- {a['title']}")