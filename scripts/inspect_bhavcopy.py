"""Inspect the NSE bhavcopy CSV format and extract a sample stock row."""

from io import StringIO

import httpx
import pandas as pd

URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_24042026.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}

with httpx.Client(headers=HEADERS, timeout=20, follow_redirects=True) as client:
    resp = client.get(URL)

print(f"HTTP {resp.status_code}  size: {len(resp.content):,} bytes")
df = pd.read_csv(StringIO(resp.text))
df.columns = [c.strip() for c in df.columns]

print(f"\nAll columns ({len(df.columns)}):")
for col in df.columns:
    print(f"  {col!r}")

print(f"\nTotal rows: {len(df)}")
print("\nReliance row:")
row = df[df["SYMBOL"].str.strip() == "RELIANCE"]
if not row.empty:
    print(row.to_string())
else:
    print("  RELIANCE not found — first 3 rows instead:")
    print(df.head(3).to_string())
