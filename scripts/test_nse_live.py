"""Quick diagnostic: test NSE API connectivity step by step."""

import time

import httpx

BASE = "https://www.nseindia.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

print("Step 1: NSE homepage")
with httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True) as client:
    r1 = client.get(BASE, timeout=12)
    print(f"  HTTP {r1.status_code}  cookies: {list(client.cookies.keys())}")
    time.sleep(0.6)

    print("Step 2: Market-data page (cookie warm-up)")
    r2 = client.get(f"{BASE}/market-data/bulk-block-deals", timeout=12)
    print(f"  HTTP {r2.status_code}")
    time.sleep(0.4)

    print("Step 3: Delivery % API (RELIANCE, last 90 days)")
    url_delivery = (
        "https://www.nseindia.com/api/historical/cm/equity"
        "?symbol=RELIANCE&series=[%22EQ%22]&from=25-01-2026&to=25-04-2026&csv=true"
    )
    r3 = client.get(url_delivery, timeout=20)
    ct = r3.headers.get("content-type", "?")
    print(f"  HTTP {r3.status_code}  content-type: {ct}")
    print(f"  body (first 500 chars):\n{r3.text[:500]}")

    print("\nStep 4: Bulk deals API (last 30 days)")
    url_bulk = (
        "https://www.nseindia.com/api/historical/bulk-deals"
        "?from=25-03-2026&to=25-04-2026"
    )
    r4 = client.get(url_bulk, timeout=20)
    ct4 = r4.headers.get("content-type", "?")
    print(f"  HTTP {r4.status_code}  content-type: {ct4}")
    print(f"  body (first 500 chars):\n{r4.text[:500]}")
