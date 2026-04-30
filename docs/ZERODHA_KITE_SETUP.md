# Zerodha Kite Connect Setup

This project uses Zerodha Kite Connect as the preferred **market-data and instrument-metadata** provider when it is configured.

yfinance remains the fallback provider.

## What Kite Is Used For

- Instrument master
- Instrument token mapping
- Historical candles
- LTP
- OHLC quotes
- Full quotes where useful
- Future WebSocket live market-data readiness

## What Kite Is Not Used For

These are intentionally disabled:

- holdings
- positions
- funds
- margins
- orders
- trades
- place_order
- modify_order
- cancel_order
- automated trading
- profile display
- fundamentals
- insider trades
- mutual fund holdings
- concall data

## Create a Kite Connect App

1. Go to the Zerodha Kite Connect developer portal.
2. Create a Kite Connect app.
3. Use this local redirect URL while developing:

```text
http://localhost:8501
```

4. Copy the app API key and API secret.

Kite Connect is a paid developer API. Historical and live market data may require an active subscription.

## Configure `.env`

Open local `.env` in the project root and set:

```text
KITE_API_KEY=your_api_key_here
KITE_API_SECRET=your_api_secret_here
KITE_ACCESS_TOKEN=
MARKET_DATA_PROVIDER=kite
ENABLE_KITE_MARKET_DATA=true
ENABLE_KITE_TRADING=false
ENABLE_KITE_PORTFOLIO=false
```

Never commit `.env`. It is ignored by Git.

## Provider Modes

```text
MARKET_DATA_PROVIDER=kite
```

Prefer Kite. If Kite is missing, expired, unmapped, fails, or fails validation, automatically use yfinance fallback.

```text
MARKET_DATA_PROVIDER=yfinance
```

Use yfinance only.

```text
MARKET_DATA_PROVIDER=auto
```

Use Kite only when API key, API secret, access token, and market-data flag are present. Otherwise use yfinance.

## Generate Login URL

1. Start the app:

```powershell
streamlit run src\stock_platform\ui\streamlit_app.py
```

2. Open **Operations & Alerts**.
3. Open **Zerodha API Setup**.
4. Click **Generate Zerodha Login URL**.
5. Open the link and complete Zerodha login.

## Copy `request_token`

After login, Zerodha redirects your browser back to something like:

```text
http://localhost:8501/?request_token=...&action=login&status=success
```

Copy only the value after:

```text
request_token=
```

Do not copy the whole URL.

## Generate `access_token`

1. Paste the temporary `request_token` into the app.
2. Click **Generate Access Token**.
3. The app does not display the full token.
4. Click **Save generated token to local .env**.
5. Restart Streamlit so settings reload.

The access token is sensitive. Treat it like a password.

## Why The Access Token May Need Refresh

Kite access tokens are session tokens and can expire. Expect to regenerate them regularly, often daily.

## Test The Setup

In **Zerodha API Setup**:

1. Generate login URL.
2. Generate access token.
3. Test Kite market-data connection.
4. Test RELIANCE LTP from Kite.
5. Test RELIANCE historical candles from Kite.
6. Confirm the main chart displays **Data source: Zerodha Kite** when Kite works.
7. Temporarily remove `KITE_ACCESS_TOKEN` from `.env`, restart Streamlit, and confirm the main chart falls back to yfinance.

## How Fallback Works

The main chart asks the market-data router for OHLCV data.

The router:

1. Tries Kite first when `MARKET_DATA_PROVIDER=kite`.
2. Validates the Kite OHLCV data.
3. Falls back to yfinance if Kite is unavailable, expired, fails, has no instrument token, or returns invalid data.
4. Marks the resulting data source in the UI.

Fundamentals, events, ownership, insider trades, mutual fund holdings, and concalls remain separate providers. They do not use Kite.

## Instrument Master Cache

After the access token works, sync instruments locally:

```powershell
.\.venv\Scripts\python.exe scripts\sync_kite_instruments.py
```

Output files:

```text
data/raw/kite/
data/processed/kite/nse_instruments.csv
data/cache/kite/nse_instruments_latest.csv
```

The script fetches only instrument metadata. It does not fetch portfolio/trading data.

## Security Warnings

- Never commit `.env`.
- Never share API secret.
- Never expose access token.
- Never expose request token.
- Do not deploy publicly with local secrets.
- No trading APIs are enabled.
- No portfolio APIs are enabled.
- Review Zerodha and exchange data terms before redistribution or public deployment.

## Troubleshooting

### Login URL fails

Check `KITE_API_KEY` and restart Streamlit.

### Access-token generation fails

Most common causes:

- request_token expired
- request_token already used
- API secret copied incorrectly
- redirect URL mismatch
- Kite Connect subscription inactive

### Market-data connection fails

Regenerate the access token and test RELIANCE LTP.

### Historical candles fail

Check:

- access token is fresh
- instrument token exists
- date range is valid
- Kite Connect subscription supports historical data

### Main chart uses yfinance

This is expected when Kite is not usable. Check the visible warning in the app. It should say Kite data was unavailable and yfinance fallback was used.
