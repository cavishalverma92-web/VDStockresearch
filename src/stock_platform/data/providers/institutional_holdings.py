"""Institutional and promoter holdings provider backed by yfinance.

yfinance exposes two holdings datasets for most stocks:
  - major_holders  : high-level breakdown (% insiders, institutions, float)
  - institutional_holders : top institutional holders with shares and % held

For Indian .NS stocks the coverage varies; this module returns empty DataFrames
gracefully when yfinance has no data, so callers never need to guard against
None returns.
"""

from __future__ import annotations

import pandas as pd
import yfinance as yf

from stock_platform.utils.logging import get_logger

log = get_logger(__name__)

_MAJOR_COLUMNS = ["category", "value"]
_INST_COLUMNS = ["holder", "shares", "date_reported", "pct_held", "value"]


def get_major_holders(symbol: str) -> pd.DataFrame:
    """Return high-level ownership breakdown for *symbol*.

    Columns: category, value
    Typical categories (may vary by stock / yfinance version):
      - % of Shares Held by All Insider
      - % of Shares Held by Institutions
      - % of Float Held by Institutions
      - Number of Institutions Holding Shares
    Returns an empty DataFrame if yfinance has no data.
    """
    try:
        ticker = yf.Ticker(symbol)
        raw = ticker.major_holders
        if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
            log.info("yfinance major_holders: no data for %s", symbol)
            return pd.DataFrame(columns=_MAJOR_COLUMNS)

        frame = raw.copy().reset_index(drop=True)
        # yfinance >= 0.2 returns columns [0, 1]; rename to category/value
        if list(frame.columns) == [0, 1]:
            frame.columns = pd.Index(["value", "category"])
            frame = frame[["category", "value"]]
        elif "Value" in frame.columns and "Breakdown" in frame.columns:
            frame = frame.rename(columns={"Value": "value", "Breakdown": "category"})
            frame = frame[["category", "value"]]

        log.info("yfinance major_holders: %d rows for %s", len(frame), symbol)
        return frame

    except Exception as exc:
        log.warning("yfinance major_holders failed for %s: %s", symbol, exc)
        return pd.DataFrame(columns=_MAJOR_COLUMNS)


def get_institutional_holders(symbol: str) -> pd.DataFrame:
    """Return top institutional holders for *symbol*.

    Columns: holder, shares, date_reported, pct_held, value
    Returns an empty DataFrame if yfinance has no data.
    """
    try:
        ticker = yf.Ticker(symbol)
        raw = ticker.institutional_holders
        if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
            log.info("yfinance institutional_holders: no data for %s", symbol)
            return pd.DataFrame(columns=_INST_COLUMNS)

        frame = raw.copy()
        # Normalize column names across yfinance versions
        frame.columns = [str(c).strip() for c in frame.columns]
        rename_map = {
            "Holder": "holder",
            "Shares": "shares",
            "Date Reported": "date_reported",
            "% Out": "pct_held",
            "Value": "value",
        }
        frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns})

        for col in _INST_COLUMNS:
            if col not in frame.columns:
                frame[col] = None

        frame = frame[_INST_COLUMNS].reset_index(drop=True)
        log.info("yfinance institutional_holders: %d holders for %s", len(frame), symbol)
        return frame

    except Exception as exc:
        log.warning("yfinance institutional_holders failed for %s: %s", symbol, exc)
        return pd.DataFrame(columns=_INST_COLUMNS)


def get_mutualfund_holders(symbol: str) -> pd.DataFrame:
    """Return top mutual-fund holders for *symbol* (where available).

    Columns: holder, shares, date_reported, pct_held, value
    Note: for Indian .NS stocks yfinance may return limited MF data.
    """
    try:
        ticker = yf.Ticker(symbol)
        raw = ticker.mutualfund_holders
        if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
            log.info("yfinance mutualfund_holders: no data for %s", symbol)
            return pd.DataFrame(columns=_INST_COLUMNS)

        frame = raw.copy()
        frame.columns = [str(c).strip() for c in frame.columns]
        rename_map = {
            "Holder": "holder",
            "Shares": "shares",
            "Date Reported": "date_reported",
            "% Out": "pct_held",
            "Value": "value",
        }
        frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns})

        for col in _INST_COLUMNS:
            if col not in frame.columns:
                frame[col] = None

        frame = frame[_INST_COLUMNS].reset_index(drop=True)
        log.info("yfinance mutualfund_holders: %d holders for %s", len(frame), symbol)
        return frame

    except Exception as exc:
        log.warning("yfinance mutualfund_holders failed for %s: %s", symbol, exc)
        return pd.DataFrame(columns=_INST_COLUMNS)


def holdings_summary(symbol: str) -> dict[str, object]:
    """Return a concise holdings summary dict for the UI.

    Keys: insider_pct, institution_pct, float_pct, top_holder, top_holder_pct,
          data_available, source
    """
    major = get_major_holders(symbol)
    inst = get_institutional_holders(symbol)

    result: dict[str, object] = {
        "insider_pct": None,
        "institution_pct": None,
        "float_pct": None,
        "top_holder": None,
        "top_holder_pct": None,
        "data_available": False,
        "source": "yfinance",
    }

    if not major.empty and "category" in major.columns and "value" in major.columns:
        for _, row in major.iterrows():
            cat = str(row["category"]).lower()
            val = row["value"]
            if "insider" in cat:
                result["insider_pct"] = _safe_pct(val)
            elif "institution" in cat and "float" not in cat:
                result["institution_pct"] = _safe_pct(val)
            elif "float" in cat and "institution" in cat:
                result["float_pct"] = _safe_pct(val)
        result["data_available"] = True

    if not inst.empty and "holder" in inst.columns:
        top = inst.iloc[0]
        result["top_holder"] = str(top.get("holder", ""))
        result["top_holder_pct"] = _safe_pct(top.get("pct_held"))

    return result


def _safe_pct(value: object) -> float | None:
    if value is None:
        return None
    try:
        v = float(str(value).replace("%", "").strip())
        # yfinance sometimes returns fractions (0.45) or percentages (45.0)
        return v * 100 if v < 1.0 else v
    except (ValueError, TypeError):
        return None
