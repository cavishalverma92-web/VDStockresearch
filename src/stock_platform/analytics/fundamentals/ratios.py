"""Basic fundamentals ratios for Phase 1."""

from __future__ import annotations

from stock_platform.analytics.fundamentals.schema import FundamentalSnapshot


def safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    """Return `numerator / denominator`, or `None` when the ratio is not usable."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def equity(snapshot: FundamentalSnapshot) -> float | None:
    """Return book equity from assets and liabilities."""
    if snapshot.total_assets is None or snapshot.total_liabilities is None:
        return None
    return snapshot.total_assets - snapshot.total_liabilities


def working_capital(snapshot: FundamentalSnapshot) -> float | None:
    """Return current assets minus current liabilities."""
    if snapshot.current_assets is None or snapshot.current_liabilities is None:
        return None
    return snapshot.current_assets - snapshot.current_liabilities


def asset_turnover(snapshot: FundamentalSnapshot) -> float | None:
    """Return revenue divided by total assets."""
    return safe_divide(snapshot.revenue, snapshot.total_assets)


def calculate_basic_ratios(snapshot: FundamentalSnapshot) -> dict[str, float | None]:
    """Calculate the first set of annual ratios needed by Phase 1."""
    book_equity = equity(snapshot)
    free_cash_flow = _free_cash_flow(snapshot)
    market_cap = snapshot.market_cap
    enterprise_value = snapshot.enterprise_value
    return {
        "return_on_assets": safe_divide(snapshot.net_income, snapshot.total_assets),
        "return_on_equity": safe_divide(snapshot.net_income, book_equity),
        "return_on_capital_employed": safe_divide(snapshot.ebit, _capital_employed(snapshot)),
        "debt_to_equity": safe_divide(snapshot.total_liabilities, book_equity),
        "net_debt_to_ebitda": safe_divide(snapshot.net_debt, snapshot.ebitda),
        "current_ratio": safe_divide(snapshot.current_assets, snapshot.current_liabilities),
        "gross_margin": safe_divide(snapshot.gross_profit, snapshot.revenue),
        "ebitda_margin": safe_divide(snapshot.ebitda, snapshot.revenue),
        "pat_margin": safe_divide(snapshot.net_income, snapshot.revenue),
        "operating_cash_flow_to_net_income": safe_divide(
            snapshot.operating_cash_flow,
            snapshot.net_income,
        ),
        "free_cash_flow": free_cash_flow,
        "free_cash_flow_yield": safe_divide(free_cash_flow, market_cap),
        "eps": snapshot.eps
        if snapshot.eps is not None
        else safe_divide(snapshot.net_income, snapshot.shares_outstanding),
        "book_value_per_share": safe_divide(book_equity, snapshot.shares_outstanding),
        "price_to_book": safe_divide(market_cap, book_equity),
        "price_to_earnings": safe_divide(market_cap, snapshot.net_income),
        "ev_to_ebitda": safe_divide(enterprise_value, snapshot.ebitda),
        "ev_to_sales": safe_divide(enterprise_value, snapshot.revenue),
        "asset_turnover": asset_turnover(snapshot),
    }


def calculate_growth(
    current: FundamentalSnapshot,
    previous: FundamentalSnapshot,
) -> dict[str, float | None]:
    """Calculate simple year-over-year growth metrics."""
    return {
        "revenue_growth": safe_divide(
            None
            if current.revenue is None or previous.revenue is None
            else current.revenue - previous.revenue,
            previous.revenue,
        ),
        "net_income_growth": safe_divide(
            None
            if current.net_income is None or previous.net_income is None
            else current.net_income - previous.net_income,
            previous.net_income,
        ),
        "eps_growth": safe_divide(
            None if current.eps is None or previous.eps is None else current.eps - previous.eps,
            previous.eps,
        ),
        "operating_cash_flow_growth": safe_divide(
            None
            if current.operating_cash_flow is None or previous.operating_cash_flow is None
            else current.operating_cash_flow - previous.operating_cash_flow,
            previous.operating_cash_flow,
        ),
        "free_cash_flow_growth": safe_divide(
            None
            if _free_cash_flow(current) is None or _free_cash_flow(previous) is None
            else _free_cash_flow(current) - _free_cash_flow(previous),
            _free_cash_flow(previous),
        ),
        "book_value_growth": safe_divide(
            None
            if current.book_value is None or previous.book_value is None
            else current.book_value - previous.book_value,
            previous.book_value,
        ),
    }


def _capital_employed(snapshot: FundamentalSnapshot) -> float | None:
    if snapshot.total_assets is None or snapshot.current_liabilities is None:
        return None
    return snapshot.total_assets - snapshot.current_liabilities


def _free_cash_flow(snapshot: FundamentalSnapshot) -> float | None:
    if snapshot.free_cash_flow is not None:
        return snapshot.free_cash_flow
    if snapshot.operating_cash_flow is None or snapshot.capital_expenditure is None:
        return None
    return snapshot.operating_cash_flow - abs(snapshot.capital_expenditure)
