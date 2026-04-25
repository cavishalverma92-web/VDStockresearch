"""Test alternative data sources: NSE archives CDN and BSE bhavcopy."""

import httpx

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

tests = [
    # NSE archives CDN (different subdomain from main site)
    ("NSE archive - today bhav", "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_25042026.csv"),
    ("NSE archive - yesterday bhav", "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_24042026.csv"),
    # BSE bhavcopy (publicly downloadable)
    ("BSE equity bhav today", "https://www.bseindia.com/download/BhavCopy/Equity/EQ250426_CSV.ZIP"),
    ("BSE equity bhav (txt format)", "https://archives.nseindia.com/products/content/sec_bhavdata_full_25042026.csv"),
    # NSE direct delivery CSV (older endpoint)
    ("NSE MTO file", "https://nsearchives.nseindia.com/archives/equities/mto/MTO_25042026.DAT"),
]

with httpx.Client(headers=HEADERS, timeout=20, follow_redirects=True) as client:
    for name, url in tests:
        try:
            r = client.get(url, timeout=15)
            ct = r.headers.get("content-type", "?")
            size = len(r.content)
            preview = r.text[:200].replace("\n", " ") if r.status_code == 200 else r.text[:100]
            print(f"[{r.status_code}] {name}")
            print(f"       {ct}  {size} bytes")
            if r.status_code == 200:
                print(f"       PREVIEW: {preview[:150]}")
        except Exception as exc:
            print(f"[ERR]  {name}: {exc}")
        print()
