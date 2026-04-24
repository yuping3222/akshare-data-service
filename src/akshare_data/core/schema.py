"""Table schema registry for akshare_data_service cache.

Defines all cache table schemas including column types, primary keys,
partitioning strategy, TTL, priority, and storage layer.

Sources:
- jk2bt/cache/registry.py: CacheTable dataclass and 38+ table definitions
- akshare_one/cache/schema.py: TableSchema, TableInfo, TableRegistry
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class CacheTable:
    """Immutable schema definition for a cache table.

    Attributes:
        name: Unique table identifier.
        partition_by: Column name for parquet partitioning, or None.
        ttl_hours: Time-to-live in hours. 0 means no expiration.
        schema: Mapping of column names to parquet data types.
        primary_key: Columns that uniquely identify a row.
        aggregation_enabled: Whether aggregated storage is enabled.
        compaction_threshold: File count before triggering compaction.
        priority: Priority tier (P0-P3).
        storage_layer: Logical storage layer (daily, meta, snapshot, minute).
    """

    name: str
    partition_by: str | None
    ttl_hours: int
    schema: dict[str, str]
    primary_key: list[str]
    aggregation_enabled: bool = True
    compaction_threshold: int = 20
    priority: str = "P0"
    storage_layer: str = "daily"


@dataclass
class TableInfo:
    """Runtime metadata about a cache table's physical state.

    Attributes:
        name: Table name.
        file_count: Number of parquet files on disk.
        total_size_bytes: Total size of all parquet files.
        last_updated: Timestamp of the most recent write.
        partition_count: Number of distinct partition values.
        priority: Priority tier.
    """

    name: str
    file_count: int
    total_size_bytes: int
    last_updated: datetime | None
    partition_count: int
    priority: str


class TableRegistry:
    """Registry for table schemas.

    Provides lookup, listing, and filtering operations over registered
    CacheTable instances.
    """

    def __init__(self) -> None:
        self._tables: dict[str, CacheTable] = {}

    def register(self, table: CacheTable) -> None:
        """Register a table schema.

        Args:
            table: CacheTable instance to register.
        """
        self._tables[table.name] = table

    def get(self, name: str) -> CacheTable:
        """Get a table schema by name.

        Args:
            name: Table name.

        Returns:
            CacheTable for the given name.

        Raises:
            KeyError: If the table is not registered.
        """
        return self._tables[name]

    def get_or_none(self, name: str) -> CacheTable | None:
        """Get a table schema by name, or None if not found.

        Args:
            name: Table name.

        Returns:
            CacheTable or None.
        """
        return self._tables.get(name)

    def list_all(self) -> dict[str, CacheTable]:
        """Return a copy of all registered schemas.

        Returns:
            Dictionary mapping table names to CacheTable.
        """
        return dict(self._tables)

    def list_by_priority(self, priority: str) -> list[CacheTable]:
        """List tables filtered by priority tier.

        Args:
            priority: Priority string (e.g. "P0").

        Returns:
            List of CacheTable matching the priority.
        """
        return [t for t in self._tables.values() if t.priority == priority]

    def list_by_layer(self, layer: str) -> list[CacheTable]:
        """List tables filtered by storage layer.

        Args:
            layer: Storage layer name (e.g. "daily", "meta").

        Returns:
            List of CacheTable matching the layer.
        """
        return [t for t in self._tables.values() if t.storage_layer == layer]

    def has(self, name: str) -> bool:
        """Check if a table is registered.

        Args:
            name: Table name.

        Returns:
            True if the table exists in the registry.
        """
        return name in self._tables


STOCK_DAILY = CacheTable(
    name="stock_daily",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
        "adjust": "string",
    },
    primary_key=["symbol", "date", "adjust"],
    storage_layer="daily",
)

ETF_DAILY = CacheTable(
    name="etf_daily",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
)

INDEX_DAILY = CacheTable(
    name="index_daily",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
)

# 首批统一语义审计（stock_daily / etf_daily / index_daily）
# 当前定义 vs 实际查询方式（变更后）
#
# | table       | schema.partition_by | 实际查询过滤（where）                          | 备注 |
# |-------------|---------------------|-----------------------------------------------|------|
# | stock_daily | date                | symbol + date range（DataService.query_daily） | 业务键统一进 where，分区键保持 date |
# | etf_daily   | date                | symbol + date range（CNETFQuoteAPI.daily）     | 禁止按 symbol 作为 partition_by |
# | index_daily | date                | symbol + date range（CNIndexQuoteAPI.daily）   | 禁止按 symbol 作为 partition_by |

FUTURES_DAILY = CacheTable(
    name="futures_daily",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "open_interest": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
)

FUTURES_SPOT = CacheTable(
    name="futures_spot",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "price": "float64",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "prev_close": "float64",
        "volume": "float64",
        "open_interest": "float64",
        "change": "float64",
        "pct_change": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
    priority="P1",
)

FUTURES_MAIN_CONTRACTS = CacheTable(
    name="futures_main_contracts",
    partition_by=None,
    ttl_hours=24,
    schema={
        "symbol": "string",
        "name": "string",
        "exchange": "string",
        "variety": "string",
    },
    primary_key=["symbol"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="snapshot",
    priority="P1",
)

CONVERSION_BOND_DAILY = CacheTable(
    name="conversion_bond_daily",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
)

INDEX_COMPONENTS = CacheTable(
    name="index_components",
    partition_by="date",
    ttl_hours=720,
    schema={
        "index_code": "string",
        "date": "date",
        "symbol": "string",
        "weight": "float64",
    },
    primary_key=["index_code", "date", "symbol"],
    storage_layer="daily",
)

INDEX_WEIGHTS = CacheTable(
    name="index_weights",
    partition_by=None,
    ttl_hours=720,
    schema={
        "index_code": "string",
        "stock_code": "string",
        "weight": "float64",
        "update_date": "date",
        "update_time": "timestamp",
    },
    primary_key=["index_code", "stock_code", "update_date"],
    storage_layer="meta",
)

FINANCE_INDICATOR = CacheTable(
    name="finance_indicator",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "report_date": "date",
        "pe": "float64",
        "pb": "float64",
        "ps": "float64",
        "roe": "float64",
        "net_profit": "float64",
        "revenue": "float64",
    },
    primary_key=["symbol", "report_date"],
    compaction_threshold=5,
    storage_layer="daily",
)

MONEY_FLOW = CacheTable(
    name="money_flow",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "main_net_inflow": "float64",
        "super_large_net_inflow": "float64",
        "large_net_inflow": "float64",
        "medium_net_inflow": "float64",
        "small_net_inflow": "float64",
    },
    primary_key=["symbol", "date"],
    priority="P1",
    storage_layer="daily",
)

NORTH_FLOW = CacheTable(
    name="north_flow",
    partition_by="date",
    ttl_hours=0,
    schema={
        "date": "date",
        "net_flow": "float64",
        "buy_amount": "float64",
        "sell_amount": "float64",
    },
    primary_key=["date"],
    priority="P1",
    storage_layer="daily",
)

INDUSTRY_COMPONENTS = CacheTable(
    name="industry_components",
    partition_by="date",
    ttl_hours=720,
    schema={
        "industry_code": "string",
        "date": "date",
        "symbol": "string",
        "industry_name": "string",
    },
    primary_key=["industry_code", "date", "symbol"],
    priority="P1",
    storage_layer="daily",
)

HOLDER = CacheTable(
    name="holder",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "report_date": "date",
        "holder_name": "string",
        "hold_count": "float64",
        "hold_ratio": "float64",
        "holder_type": "string",
    },
    primary_key=["symbol", "report_date", "holder_name"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

DIVIDEND = CacheTable(
    name="dividend",
    partition_by="announce_date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "announce_date": "date",
        "dividend_cash": "float64",
        "dividend_stock": "float64",
        "record_date": "date",
        "ex_date": "date",
    },
    primary_key=["symbol", "announce_date"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

VALUATION = CacheTable(
    name="valuation",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "pe": "float64",
        "pb": "float64",
        "ps": "float64",
        "market_cap": "float64",
        "circulating_cap": "float64",
    },
    primary_key=["symbol", "date"],
    priority="P1",
    storage_layer="daily",
)

INDEX_VALUATION = CacheTable(
    name="index_valuation",
    partition_by="date",
    ttl_hours=0,
    schema={
        "index_code": "string",
        "date": "date",
        "pe": "float64",
        "pb": "float64",
        "dividend_yield": "float64",
    },
    primary_key=["index_code", "date"],
    storage_layer="daily",
    priority="P1",
)

UNLOCK = CacheTable(
    name="unlock",
    partition_by="announce_date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "announce_date": "date",
        "unlock_date": "date",
        "unlock_count": "float64",
        "unlock_ratio": "float64",
        "unlock_type": "string",
    },
    primary_key=["symbol", "announce_date"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

SPOT_SNAPSHOT = CacheTable(
    name="spot_snapshot",
    partition_by="date",
    ttl_hours=168,
    schema={
        "symbol": "string",
        "date": "date",
        "price": "float64",
        "change_pct": "float64",
        "volume": "float64",
        "amount": "float64",
        "turnover_rate": "float64",
        "pe": "float64",
        "pb": "float64",
        "market_cap": "float64",
    },
    primary_key=["symbol", "date"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P0",
)

SECTOR_FLOW_SNAPSHOT = CacheTable(
    name="sector_flow_snapshot",
    partition_by="date",
    ttl_hours=168,
    schema={
        "date": "date",
        "sector_name": "string",
        "sector_type": "string",
        "change_pct": "float64",
        "net_inflow": "float64",
        "stock_count": "int64",
    },
    primary_key=["date", "sector_name", "sector_type"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P0",
)

HSGT_HOLD_SNAPSHOT = CacheTable(
    name="hsgt_hold_snapshot",
    partition_by="date",
    ttl_hours=168,
    schema={
        "symbol": "string",
        "date": "date",
        "hold_count": "float64",
        "hold_ratio": "float64",
        "change_count": "float64",
    },
    primary_key=["symbol", "date"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P0",
)

STOCK_MINUTE = CacheTable(
    name="stock_minute",
    partition_by="week",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "datetime": "timestamp",
        "period": "string",
        "adjust": "string",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["symbol", "datetime", "period", "adjust"],
    compaction_threshold=50,
    storage_layer="minute",
    priority="P2",
)

ETF_MINUTE = CacheTable(
    name="etf_minute",
    partition_by="week",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "datetime": "timestamp",
        "period": "string",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["symbol", "datetime", "period"],
    compaction_threshold=50,
    storage_layer="minute",
    priority="P2",
)

SECURITIES = CacheTable(
    name="securities",
    partition_by=None,
    ttl_hours=0,
    schema={
        "symbol": "string",
        "name": "string",
        "type": "string",
        "list_date": "date",
        "delist_date": "date",
        "exchange": "string",
    },
    primary_key=["symbol"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

TRADE_CALENDAR = CacheTable(
    name="trade_calendar",
    partition_by=None,
    ttl_hours=0,
    schema={
        "date": "date",
        "is_trading_day": "bool",
    },
    primary_key=["date"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

INDUSTRY_LIST = CacheTable(
    name="industry_list",
    partition_by=None,
    ttl_hours=720,
    schema={
        "industry_code": "string",
        "industry_name": "string",
        "source": "string",
    },
    primary_key=["industry_code"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

CONCEPT_LIST = CacheTable(
    name="concept_list",
    partition_by=None,
    ttl_hours=720,
    schema={
        "concept_code": "string",
        "concept_name": "string",
        "source": "string",
    },
    primary_key=["concept_code"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

COMPANY_INFO = CacheTable(
    name="company_info",
    partition_by=None,
    ttl_hours=720,
    schema={
        "symbol": "string",
        "name": "string",
        "industry": "string",
        "area": "string",
        "list_date": "date",
        "market": "string",
    },
    primary_key=["symbol"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

FACTOR_CACHE = CacheTable(
    name="factor_cache",
    partition_by="factor_name",
    ttl_hours=0,
    schema={
        "factor_name": "string",
        "symbol": "string",
        "date": "date",
        "value": "float64",
    },
    primary_key=["factor_name", "symbol", "date"],
    compaction_threshold=10,
    storage_layer="daily",
    priority="P1",
)

FINANCIAL_REPORT = CacheTable(
    name="financial_report",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "report_date": "date",
        "report_type": "string",
        "item_name": "string",
        "item_value": "float64",
    },
    primary_key=["symbol", "report_date", "report_type", "item_name"],
    compaction_threshold=5,
    storage_layer="daily",
    priority="P0",
)

FINANCIAL_BENEFIT = CacheTable(
    name="financial_benefit",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "report_date": "date",
        "indicator": "string",
        "value": "float64",
    },
    primary_key=["symbol", "report_date", "indicator"],
    compaction_threshold=5,
    storage_layer="daily",
    priority="P0",
)

INDUSTRY_MAPPING = CacheTable(
    name="industry_mapping",
    partition_by=None,
    ttl_hours=720,
    schema={
        "symbol": "string",
        "industry_code": "string",
        "industry_name": "string",
        "level": "int64",
    },
    primary_key=["symbol"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P1",
)

CONCEPT_COMPONENTS = CacheTable(
    name="concept_components",
    partition_by="date",
    ttl_hours=720,
    schema={
        "concept_code": "string",
        "concept_name": "string",
        "date": "date",
        "symbol": "string",
    },
    primary_key=["concept_code", "date", "symbol"],
    storage_layer="daily",
    priority="P1",
)

FUND_PORTFOLIO = CacheTable(
    name="fund_portfolio",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "fund_code": "string",
        "report_date": "date",
        "symbol": "string",
        "hold_count": "float64",
        "hold_ratio": "float64",
        "market_value": "float64",
    },
    primary_key=["fund_code", "report_date", "symbol"],
    compaction_threshold=5,
    storage_layer="daily",
    priority="P1",
)

SHARE_CHANGE = CacheTable(
    name="share_change",
    partition_by="announce_date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "announce_date": "date",
        "total_shares": "float64",
        "circulating_shares": "float64",
        "change_type": "string",
    },
    primary_key=["symbol", "announce_date"],
    compaction_threshold=5,
    storage_layer="daily",
    priority="P2",
)

HOLDING_CHANGE = CacheTable(
    name="holding_change",
    partition_by="announce_date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "announce_date": "date",
        "holder_name": "string",
        "change_count": "float64",
        "change_ratio": "float64",
        "change_type": "string",
    },
    primary_key=["symbol", "announce_date", "holder_name"],
    compaction_threshold=5,
    storage_layer="daily",
    priority="P2",
)

MACRO_DATA = CacheTable(
    name="macro_data",
    partition_by=None,
    ttl_hours=720,
    schema={
        "indicator": "string",
        "date": "date",
        "value": "float64",
        "change_pct": "float64",
    },
    primary_key=["indicator", "date"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

MARGIN_DETAIL = CacheTable(
    name="margin_detail",
    partition_by="date",
    ttl_hours=720,
    schema={
        "market": "string",
        "date": "date",
        "symbol": "string",
        "margin_balance": "float64",
        "short_balance": "float64",
    },
    primary_key=["market", "date", "symbol"],
    storage_layer="daily",
    priority="P2",
)

MARGIN_UNDERLYING = CacheTable(
    name="margin_underlying",
    partition_by="date",
    ttl_hours=720,
    schema={
        "market": "string",
        "date": "date",
        "symbol": "string",
        "stock_name": "string",
    },
    primary_key=["market", "date", "symbol"],
    storage_layer="daily",
    priority="P2",
)

OPTION_DAILY = CacheTable(
    name="option_daily",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "open_interest": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
    priority="P3",
)

STATUS_CHANGE = CacheTable(
    name="status_change",
    partition_by=None,
    ttl_hours=720,
    schema={
        "symbol": "string",
        "status_date": "date",
        "status_type": "string",
        "reason": "string",
    },
    primary_key=["symbol", "status_date"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

CALL_AUCTION = CacheTable(
    name="call_auction",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "datetime": "timestamp",
        "price": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["symbol", "datetime"],
    compaction_threshold=50,
    storage_layer="daily",
    priority="P3",
)

EQUITY_PLEDGE = CacheTable(
    name="equity_pledge",
    partition_by="pledge_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "pledge_date": "date",
        "shareholder_name": "string",
        "pledge_shares": "float64",
        "pledge_ratio": "float64",
        "pledgee": "string",
        "start_date": "date",
        "end_date": "date",
    },
    primary_key=["symbol", "pledge_date", "shareholder_name"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

RESTRICTED_RELEASE = CacheTable(
    name="restricted_release",
    partition_by="release_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "release_date": "date",
        "release_shares": "float64",
        "release_value": "float64",
        "release_type": "string",
        "shareholder_name": "string",
    },
    primary_key=["symbol", "release_date", "shareholder_name"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

GOODWILL = CacheTable(
    name="goodwill",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "report_date": "date",
        "goodwill_balance": "float64",
        "goodwill_impairment": "float64",
        "net_assets": "float64",
        "goodwill_ratio": "float64",
    },
    primary_key=["symbol", "report_date"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

REPURCHASE = CacheTable(
    name="repurchase",
    partition_by="announcement_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "announcement_date": "date",
        "progress": "string",
        "amount": "float64",
        "quantity": "float64",
        "price_range": "string",
    },
    primary_key=["symbol", "announcement_date"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

INSIDER_TRADE = CacheTable(
    name="insider_trade",
    partition_by="transaction_date",
    ttl_hours=720,
    schema={
        "symbol": "string",
        "transaction_date": "date",
        "name": "string",
        "title": "string",
        "transaction_shares": "float64",
        "transaction_price": "float64",
        "transaction_value": "float64",
        "relationship": "string",
    },
    primary_key=["symbol", "transaction_date", "name"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

ESG_RATING = CacheTable(
    name="esg_rating",
    partition_by="rating_date",
    ttl_hours=720,
    schema={
        "symbol": "string",
        "rating_date": "date",
        "esg_score": "float64",
        "e_score": "float64",
        "s_score": "float64",
        "g_score": "float64",
        "rating_agency": "string",
    },
    primary_key=["symbol", "rating_date", "rating_agency"],
    compaction_threshold=5,
    priority="P2",
    storage_layer="daily",
)

PERFORMANCE_FORECAST = CacheTable(
    name="performance_forecast",
    partition_by="report_date",
    ttl_hours=2160,
    schema={
        "symbol": "string",
        "report_date": "date",
        "forecast_type": "string",
        "net_profit_min": "float64",
        "net_profit_max": "float64",
        "change_pct_min": "float64",
        "change_pct_max": "float64",
    },
    primary_key=["symbol", "report_date", "forecast_type"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

ANALYST_RANK = CacheTable(
    name="analyst_rank",
    partition_by="date",
    ttl_hours=720,
    schema={
        "analyst_name": "string",
        "broker_name": "string",
        "date": "date",
        "rank": "int64",
        "score": "float64",
    },
    primary_key=["analyst_name", "date"],
    compaction_threshold=5,
    priority="P2",
    storage_layer="daily",
)

RESEARCH_REPORT = CacheTable(
    name="research_report",
    partition_by="report_date",
    ttl_hours=720,
    schema={
        "symbol": "string",
        "report_date": "date",
        "title": "string",
        "analyst": "string",
        "broker": "string",
        "rating": "string",
        "target_price": "float64",
    },
    primary_key=["symbol", "report_date", "title"],
    compaction_threshold=5,
    priority="P2",
    storage_layer="daily",
)

CHIP_DISTRIBUTION = CacheTable(
    name="chip_distribution",
    partition_by="date",
    ttl_hours=720,
    schema={
        "symbol": "string",
        "date": "date",
        "price": "float64",
        "volume": "float64",
        "ratio": "float64",
    },
    primary_key=["symbol", "date", "price"],
    compaction_threshold=5,
    priority="P2",
    storage_layer="daily",
)

STOCK_BONUS = CacheTable(
    name="stock_bonus",
    partition_by="announce_date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "announce_date": "date",
        "cash_dividend": "float64",
        "stock_dividend": "float64",
        "capitalization_reserve": "float64",
        "record_date": "date",
        "ex_date": "date",
        "pay_date": "date",
    },
    primary_key=["symbol", "announce_date"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

RIGHTS_ISSUE = CacheTable(
    name="rights_issue",
    partition_by="announce_date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "announce_date": "date",
        "rights_price": "float64",
        "rights_ratio": "float64",
        "actual_raise": "float64",
    },
    primary_key=["symbol", "announce_date"],
    compaction_threshold=5,
    priority="P1",
    storage_layer="daily",
)

COMPANY_MANAGEMENT = CacheTable(
    name="company_management",
    partition_by=None,
    ttl_hours=720,
    schema={
        "symbol": "string",
        "name": "string",
        "title": "string",
        "age": "int64",
        "education": "string",
        "hold_shares": "float64",
    },
    primary_key=["symbol", "name"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

NAME_HISTORY = CacheTable(
    name="name_history",
    partition_by=None,
    ttl_hours=0,
    schema={
        "symbol": "string",
        "old_name": "string",
        "new_name": "string",
        "change_date": "date",
    },
    primary_key=["symbol", "change_date"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

SHIBOR_RATE = CacheTable(
    name="shibor_rate",
    partition_by="date",
    ttl_hours=0,
    schema={
        "date": "date",
        "on": "float64",
        "1w": "float64",
        "2w": "float64",
        "1m": "float64",
        "3m": "float64",
        "6m": "float64",
        "9m": "float64",
        "1y": "float64",
    },
    primary_key=["date"],
    priority="P2",
    storage_layer="daily",
)

SOCIAL_FINANCING = CacheTable(
    name="social_financing",
    partition_by="date",
    ttl_hours=0,
    schema={
        "date": "date",
        "total_amount": "float64",
        "yoy_change": "float64",
    },
    primary_key=["date"],
    priority="P2",
    storage_layer="daily",
)

MACRO_GDP = CacheTable(
    name="macro_gdp",
    partition_by="date",
    ttl_hours=0,
    schema={
        "date": "date",
        "gdp": "float64",
        "gdp_yoy": "float64",
        "primary_industry": "float64",
        "secondary_industry": "float64",
        "tertiary_industry": "float64",
    },
    primary_key=["date"],
    priority="P2",
    storage_layer="daily",
)

MACRO_EXCHANGE_RATE = CacheTable(
    name="macro_exchange_rate",
    partition_by="date",
    ttl_hours=0,
    schema={
        "date": "date",
        "currency_pair": "string",
        "rate": "float64",
    },
    primary_key=["date", "currency_pair"],
    priority="P2",
    storage_layer="daily",
)

FOF_FUND = CacheTable(
    name="fof_fund",
    partition_by=None,
    ttl_hours=720,
    schema={
        "fund_code": "string",
        "fund_name": "string",
        "fund_type": "string",
        "nav_date": "date",
        "nav": "float64",
    },
    primary_key=["fund_code"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

LOF_FUND = CacheTable(
    name="lof_fund",
    partition_by=None,
    ttl_hours=720,
    schema={
        "fund_code": "string",
        "fund_name": "string",
        "nav_date": "date",
        "nav": "float64",
        "market_price": "float64",
        "premium_rate": "float64",
    },
    primary_key=["fund_code"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

CONVERT_BOND_PREMIUM = CacheTable(
    name="convert_bond_premium",
    partition_by="date",
    ttl_hours=168,
    schema={
        "bond_code": "string",
        "bond_name": "string",
        "stock_code": "string",
        "bond_price": "float64",
        "conversion_value": "float64",
        "premium_rate": "float64",
        "date": "date",
    },
    primary_key=["bond_code", "date"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P1",
)

INDUSTRY_PERFORMANCE = CacheTable(
    name="industry_performance",
    partition_by="date",
    ttl_hours=168,
    schema={
        "industry_code": "string",
        "industry_name": "string",
        "date": "date",
        "change_pct": "float64",
        "turnover": "float64",
        "stock_count": "int64",
    },
    primary_key=["industry_code", "date"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P1",
)

CONCEPT_PERFORMANCE = CacheTable(
    name="concept_performance",
    partition_by="date",
    ttl_hours=168,
    schema={
        "concept_code": "string",
        "concept_name": "string",
        "date": "date",
        "change_pct": "float64",
        "turnover": "float64",
        "stock_count": "int64",
    },
    primary_key=["concept_code", "date"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P1",
)

HOT_RANK = CacheTable(
    name="hot_rank",
    partition_by="date",
    ttl_hours=168,
    schema={
        "rank": "int64",
        "symbol": "string",
        "name": "string",
        "price": "float64",
        "pct_change": "float64",
        "date": "date",
    },
    primary_key=["rank", "date"],
    compaction_threshold=1,
    storage_layer="snapshot",
    priority="P0",
)

NORTHBOUND_HOLDINGS = CacheTable(
    name="northbound_holdings",
    partition_by="date",
    ttl_hours=0,
    schema={
        "symbol": "string",
        "date": "date",
        "hold_count": "float64",
        "hold_ratio": "float64",
        "net_buy": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
    priority="P1",
)

BLOCK_DEAL = CacheTable(
    name="block_deal",
    partition_by="date",
    ttl_hours=720,
    schema={
        "symbol": "string",
        "date": "date",
        "deal_price": "float64",
        "deal_volume": "float64",
        "deal_amount": "float64",
        "buyer": "string",
        "seller": "string",
        "premium_ratio": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
    priority="P2",
)

DRAGON_TIGER_LIST = CacheTable(
    name="dragon_tiger_list",
    partition_by="date",
    ttl_hours=168,
    schema={
        "symbol": "string",
        "name": "string",
        "date": "date",
        "change_pct": "float64",
        "turnover": "float64",
        "reason": "string",
        "net_buy": "float64",
        "buy_amount": "float64",
        "sell_amount": "float64",
    },
    primary_key=["symbol", "date"],
    storage_layer="daily",
    priority="P1",
)

SUSPENDED_STOCKS = CacheTable(
    name="suspended_stocks",
    partition_by=None,
    ttl_hours=24,
    schema={
        "symbol": "string",
        "name": "string",
        "suspend_date": "date",
        "reason": "string",
    },
    primary_key=["symbol"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

ST_STOCKS = CacheTable(
    name="st_stocks",
    partition_by=None,
    ttl_hours=168,
    schema={
        "symbol": "string",
        "name": "string",
        "st_type": "string",
        "st_date": "date",
    },
    primary_key=["symbol"],
    aggregation_enabled=False,
    compaction_threshold=0,
    storage_layer="meta",
    priority="P2",
)

SW_INDUSTRY_DAILY = CacheTable(
    name="sw_industry_daily",
    partition_by="index_code",
    ttl_hours=0,
    schema={
        "index_code": "string",
        "date": "date",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "amount": "float64",
    },
    primary_key=["index_code", "date"],
    storage_layer="daily",
    priority="P2",
)

SCHEMA_REGISTRY = TableRegistry()

_DEFAULT_TABLES = (
    STOCK_DAILY,
    ETF_DAILY,
    INDEX_DAILY,
    FUTURES_DAILY,
    FUTURES_SPOT,
    FUTURES_MAIN_CONTRACTS,
    CONVERSION_BOND_DAILY,
    INDEX_COMPONENTS,
    INDEX_WEIGHTS,
    FINANCE_INDICATOR,
    MONEY_FLOW,
    NORTH_FLOW,
    INDUSTRY_COMPONENTS,
    HOLDER,
    DIVIDEND,
    VALUATION,
    INDEX_VALUATION,
    UNLOCK,
    SPOT_SNAPSHOT,
    SECTOR_FLOW_SNAPSHOT,
    HSGT_HOLD_SNAPSHOT,
    STOCK_MINUTE,
    ETF_MINUTE,
    SECURITIES,
    TRADE_CALENDAR,
    INDUSTRY_LIST,
    CONCEPT_LIST,
    COMPANY_INFO,
    FACTOR_CACHE,
    FINANCIAL_REPORT,
    FINANCIAL_BENEFIT,
    INDUSTRY_MAPPING,
    CONCEPT_COMPONENTS,
    FUND_PORTFOLIO,
    SHARE_CHANGE,
    HOLDING_CHANGE,
    MACRO_DATA,
    MARGIN_DETAIL,
    MARGIN_UNDERLYING,
    STATUS_CHANGE,
    OPTION_DAILY,
    CALL_AUCTION,
    EQUITY_PLEDGE,
    RESTRICTED_RELEASE,
    GOODWILL,
    REPURCHASE,
    INSIDER_TRADE,
    ESG_RATING,
    PERFORMANCE_FORECAST,
    ANALYST_RANK,
    RESEARCH_REPORT,
    CHIP_DISTRIBUTION,
    STOCK_BONUS,
    RIGHTS_ISSUE,
    COMPANY_MANAGEMENT,
    NAME_HISTORY,
    SHIBOR_RATE,
    SOCIAL_FINANCING,
    MACRO_GDP,
    MACRO_EXCHANGE_RATE,
    FOF_FUND,
    LOF_FUND,
    CONVERT_BOND_PREMIUM,
    INDUSTRY_PERFORMANCE,
    CONCEPT_PERFORMANCE,
    HOT_RANK,
    NORTHBOUND_HOLDINGS,
    BLOCK_DEAL,
    DRAGON_TIGER_LIST,
    SUSPENDED_STOCKS,
    ST_STOCKS,
    SW_INDUSTRY_DAILY,
)


def get_table_schema(name: str) -> CacheTable | None:
    """Look up a table schema by name.

    Args:
        name: Table name.

    Returns:
        CacheTable if found, None otherwise.
    """
    return SCHEMA_REGISTRY.get_or_none(name)


def list_tables() -> list[str]:
    """Return a sorted list of all registered table names.

    Returns:
        List of table name strings.
    """
    return sorted(SCHEMA_REGISTRY.list_all().keys())


def init_schemas() -> None:
    """Register all default table schemas into the global registry.

    Idempotent: safe to call multiple times.
    """
    for table in _DEFAULT_TABLES:
        SCHEMA_REGISTRY.register(table)


init_schemas()
