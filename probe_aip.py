import requests
from bs4 import BeautifulSoup
import re

def probe_aip():
    url = "https://www.aip.com.au/pricing/market-watch"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        # Look for "Singapore"
        text = soup.get_text()
        print(text[:2000]) # Print snippet
        
        # Try to find a table value
        print("\n--- Tables ---")
        for table in soup.find_all('table'):
            print(table.get_text()[:500])
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    probe_aip()