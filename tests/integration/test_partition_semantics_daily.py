from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from akshare_data.api import DataService as APIDataService
from akshare_data.service.data_service import DataService as ServedDataService
from akshare_data.service.reader import ServedReader
from akshare_data.store.manager import CacheConfig, CacheManager, reset_cache_manager


@pytest.mark.integration
def test_served_reader_partition_contract_warns_and_fallbacks(caplog):
    cache = MagicMock()
    cache.read.return_value = pd.DataFrame({"symbol": ["sh600000"], "date": ["2024-01-02"]})

    reader = ServedReader(cache_manager=cache)
    with caplog.at_level("WARNING"):
        df = reader.read(
            "stock_daily",
            where={"symbol": "sh600000"},
            partition_by="symbol",
            partition_value="sh600000",
        )

    assert not df.empty
    assert "partition_by mismatch" in caplog.text
    assert cache.read.call_args.kwargs["partition_by"] == "date"


@pytest.fixture
def isolated_service(tmp_path):
    reset_cache_manager()
    cache = CacheManager(config=CacheConfig(base_dir=str(tmp_path)))
    yield ServedDataService(cache_manager=cache), APIDataService(cache_manager=cache), cache
    reset_cache_manager()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("table", "query_fn", "symbol"),
    [
        ("stock_daily", lambda served, _api, s: served.query_daily("stock_daily", s, "2024-01-01", "2024-01-31").data, "sh600000"),
        ("index_daily", lambda _served, api, s: api.cn.index.quote.daily(s, "2024-01-01", "2024-01-31"), "sh000300"),
        ("etf_daily", lambda _served, api, s: api.cn.fund.quote.daily(s, "2024-01-01", "2024-01-31"), "sh510300"),
    ],
)
def test_single_symbol_query_no_cross_symbol_pollution(isolated_service, table, query_fn, symbol):
    served, api, cache = isolated_service
    other_symbol = "sh688001"
    raw = pd.DataFrame(
        [
            {"symbol": symbol, "date": "2024-01-02", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.8, "volume": 1000.0, "amount": 10800.0},
            {"symbol": other_symbol, "date": "2024-01-02", "open": 20.0, "high": 21.0, "low": 19.5, "close": 20.8, "volume": 2000.0, "amount": 41600.0},
            {"symbol": symbol, "date": "2024-01-03", "open": 10.5, "high": 11.2, "low": 10.1, "close": 11.0, "volume": 1100.0, "amount": 12100.0},
        ]
    )
    if table == "stock_daily":
        raw["adjust"] = "qfq"

    for date in sorted(raw["date"].unique()):
        part = raw[raw["date"] == date].copy()
        cache.write(
            table,
            part,
            storage_layer="daily",
            partition_by="date",
            partition_value=date,
        )

    result = query_fn(served, api, symbol)
    assert not result.empty
    assert set(result["symbol"]) == {symbol}

