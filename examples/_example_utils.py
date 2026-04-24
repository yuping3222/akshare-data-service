"""Examples shared helper utilities."""

from __future__ import annotations

from datetime import datetime, timedelta
import time
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from akshare_data.core.symbols import normalize_symbol


def normalize_symbol_input(symbol: str) -> str:
    """Normalize symbol input."""
    return normalize_symbol(symbol)


def is_empty_result(value: Any) -> bool:
    """Check whether value should be treated as empty."""
    if value is None:
        return True
    if hasattr(value, "empty"):
        return bool(value.empty)
    if isinstance(value, (dict, list, tuple, set)):
        return len(value) == 0
    return False


def fetch_with_retry(
    fetcher: Callable[[], Any],
    retries: int = 2,
    sleep_seconds: float = 0.6,
) -> Any:
    """Retry on empty value or exception, return last value."""
    last_value: Any = None
    last_error: Optional[Exception] = None
    for idx in range(retries + 1):
        try:
            value = fetcher()
            last_value = value
            if not is_empty_result(value):
                return value
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        if idx < retries:
            time.sleep(sleep_seconds)
    if last_error is not None and is_empty_result(last_value):
        raise last_error
    return last_value


def recent_dates(base_date: str, fallback_days: int = 7) -> list[str]:
    """Generate fallback date list from a base date."""
    dt = datetime.strptime(base_date, "%Y-%m-%d")
    return [(dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(fallback_days + 1)]


def fetch_with_date_fallback(
    fetcher: Callable[[str], Any],
    base_date: str,
    fallback_days: int = 7,
    retries_per_date: int = 1,
) -> tuple[str | None, Any]:
    """Try date fallback and return hit date with value."""
    for date in recent_dates(base_date, fallback_days=fallback_days):
        try:
            value = fetch_with_retry(lambda: fetcher(date), retries=retries_per_date)
            if not is_empty_result(value):
                return date, value
        except Exception:  # noqa: BLE001
            continue
    return None, pd.DataFrame()


def stable_df(df: pd.DataFrame) -> pd.DataFrame:
    """Stable sort output DataFrame to reduce fluctuations."""
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    sort_keys: list[str] = []
    for key in ("date", "日期", "trade_date", "symbol", "代码", "code"):
        if key in work.columns:
            sort_keys.append(key)
    if sort_keys:
        work = work.sort_values(sort_keys, kind="stable")
    return work.reset_index(drop=True)


def print_df_brief(df: pd.DataFrame, rows: int = 10) -> None:
    """Print compact DataFrame summary."""
    stable = stable_df(df)
    print(f"数据形状: {stable.shape}")
    print(f"字段列表: {list(stable.columns)}")
    if not stable.empty:
        print(stable.head(rows).to_string(index=False))


def recent_trade_days(service, max_backtrack: int = 10) -> list[str]:
    """Get latest trade-day candidates in YYYY-MM-DD format."""
    today = datetime.now().date()
    start = (today - timedelta(days=45)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    try:
        calendar = service.get_trade_calendar(start_date=start, end_date=end)
        if isinstance(calendar, list) and calendar:
            return list(reversed(calendar[-max_backtrack:]))
    except Exception:
        pass
    # Fallback to business-day candidates when calendar fetch is unavailable.
    bdays = pd.bdate_range(end=today, periods=max_backtrack)
    return [d.strftime("%Y-%m-%d") for d in reversed(bdays)]


def call_with_date_range_fallback(
    service,
    fetch_fn: Callable[..., pd.DataFrame],
    *,
    symbol: str | None = None,
    max_backtrack: int = 10,
    window_days: int = 365,
) -> tuple[pd.DataFrame, str | None]:
    """Try T, T-1 ... T-10 by date range and return first non-empty result."""
    for end_date in recent_trade_days(service, max_backtrack=max_backtrack):
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        start_date = (end_dt - timedelta(days=window_days)).strftime("%Y-%m-%d")
        kwargs = {"start_date": start_date, "end_date": end_date}
        if symbol is not None:
            kwargs["symbol"] = symbol
        try:
            df = fetch_fn(**kwargs)
            if df is not None and not df.empty:
                return df, end_date
        except Exception:
            continue
    return pd.DataFrame(), None


def first_non_empty_by_symbol(
    fetch_fn: Callable[..., pd.DataFrame], symbols: Iterable[str]
) -> tuple[pd.DataFrame, str | None]:
    """Try symbols one by one and return first non-empty result."""
    for symbol in symbols:
        try:
            df = fetch_fn(symbol=symbol)
            if df is not None and not df.empty:
                return df, symbol
        except Exception:
            continue
    return pd.DataFrame(), None
