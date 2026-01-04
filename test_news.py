import market_news
import asyncio

try:
    print("Testing News Fetch...")
    news = market_news.get_market_news()
    print(f"Global Items: {len(news['global'])}")
    print(f"Domestic Items: {len(news['domestic'])}")
    if news['domestic']:
        print(f"Sample: {news['domestic'][0]['title']}")
except Exception as e:
    print(f"Error: {e}")
