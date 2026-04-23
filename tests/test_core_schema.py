"""Tests for akshare_data.core.schema module.

Covers:
- CacheTable frozen dataclass
- TableInfo dataclass
- TableRegistry class and methods
- All predefined table schemas
- Schema registry functions
"""

import pytest
from datetime import datetime
from akshare_data.core.schema import (
    TableInfo,
    TableRegistry,
    STOCK_DAILY,
    ETF_DAILY,
    INDEX_DAILY,
    FUTURES_DAILY,
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
    OPTION_DAILY,
    STATUS_CHANGE,
    CALL_AUCTION,
    SCHEMA_REGISTRY,
    get_table_schema,
    list_tables,
    init_schemas,
)


class TestCacheTable:
    """Test CacheTable frozen dataclass."""

    def test_is_frozen(self):
        """Should be frozen (immutable)."""
        with pytest.raises(Exception):  # frozen dataclass raises FrozenInstanceError
            STOCK_DAILY.name = "new_name"

    def test_has_required_attributes(self):
        """Should have all required attributes."""
        assert hasattr(STOCK_DAILY, "name")
        assert hasattr(STOCK_DAILY, "partition_by")
        assert hasattr(STOCK_DAILY, "ttl_hours")
        assert hasattr(STOCK_DAILY, "schema")
        assert hasattr(STOCK_DAILY, "primary_key")
        assert hasattr(STOCK_DAILY, "aggregation_enabled")
        assert hasattr(STOCK_DAILY, "compaction_threshold")
        assert hasattr(STOCK_DAILY, "priority")
        assert hasattr(STOCK_DAILY, "storage_layer")

    def test_schema_is_dict(self):
        """Schema should be a dictionary."""
        assert isinstance(STOCK_DAILY.schema, dict)

    def test_primary_key_is_list(self):
        """Primary key should be a list."""
        assert isinstance(STOCK_DAILY.primary_key, list)


class TestTableInfo:
    """Test TableInfo dataclass."""

    def test_creation(self):
        """Should create with required fields."""
        info = TableInfo(
            name="test_table",
            file_count=10,
            total_size_bytes=1000000,
            last_updated=datetime.now(),
            partition_count=5,
            priority="P0",
        )
        assert info.name == "test_table"
        assert info.file_count == 10
        assert info.total_size_bytes == 1000000
        assert info.partition_count == 5

    def test_last_updated_can_be_none(self):
        """last_updated can be None."""
        info = TableInfo(
            name="test_table",
            file_count=0,
            total_size_bytes=0,
            last_updated=None,
            partition_count=0,
            priority="P0",
        )
        assert info.last_updated is None


class TestTableRegistry:
    """Test TableRegistry class."""

    def setup_method(self):
        """Create fresh registry for each test."""
        self.registry = TableRegistry()

    def test_register_and_get(self):
        """Should register and retrieve table."""
        self.registry.register(STOCK_DAILY)
        retrieved = self.registry.get("stock_daily")
        assert retrieved.name == "stock_daily"

    def test_get_nonexistent_raises_key_error(self):
        """Getting nonexistent table should raise KeyError."""
        with pytest.raises(KeyError):
            self.registry.get("nonexistent")

    def test_get_or_none_returns_none_for_missing(self):
        """get_or_none should return None for missing tables."""
        result = self.registry.get_or_none("nonexistent")
        assert result is None

    def test_get_or_none_returns_table_when_exists(self):
        """get_or_none should return table when it exists."""
        self.registry.register(STOCK_DAILY)
        result = self.registry.get_or_none("stock_daily")
        assert result is not None
        assert result.name == "stock_daily"

    def test_list_all_returns_dict(self):
        """list_all should return dictionary."""
        self.registry.register(STOCK_DAILY)
        tables = self.registry.list_all()
        assert isinstance(tables, dict)
        assert "stock_daily" in tables

    def test_list_by_priority(self):
        """Should filter tables by priority."""
        self.registry.register(SPOT_SNAPSHOT)  # P0
        p0_tables = self.registry.list_by_priority("P0")
        assert len(p0_tables) >= 1
        assert all(t.priority == "P0" for t in p0_tables)

    def test_list_by_layer(self):
        """Should filter tables by storage layer."""
        self.registry.register(STOCK_DAILY)
        daily_tables = self.registry.list_by_layer("daily")
        assert len(daily_tables) >= 1
        assert all(t.storage_layer == "daily" for t in daily_tables)

    def test_has_returns_true_for_existing(self):
        """has should return True for existing tables."""
        self.registry.register(STOCK_DAILY)
        assert self.registry.has("stock_daily") is True

    def test_has_returns_false_for_missing(self):
        """has should return False for missing tables."""
        assert self.registry.has("nonexistent") is False


class TestPredefinedTables:
    """Test all predefined table schemas."""

    def test_stock_daily(self):
        """STOCK_DAILY should have correct schema."""
        assert STOCK_DAILY.name == "stock_daily"
        assert "symbol" in STOCK_DAILY.schema
        assert "open" in STOCK_DAILY.schema
        assert STOCK_DAILY.storage_layer == "daily"

    def test_etf_daily(self):
        """ETF_DAILY should have correct schema."""
        assert ETF_DAILY.name == "etf_daily"
        assert ETF_DAILY.storage_layer == "daily"

    def test_index_daily(self):
        """INDEX_DAILY should have correct schema."""
        assert INDEX_DAILY.name == "index_daily"
        assert INDEX_DAILY.storage_layer == "daily"

    def test_futures_daily(self):
        """FUTURES_DAILY should have correct schema."""
        assert FUTURES_DAILY.name == "futures_daily"
        assert "open_interest" in FUTURES_DAILY.schema

    def test_conversion_bond_daily(self):
        """CONVERSION_BOND_DAILY should exist."""
        assert CONVERSION_BOND_DAILY.name == "conversion_bond_daily"

    def test_index_components(self):
        """INDEX_COMPONENTS should have weight column."""
        assert INDEX_COMPONENTS.name == "index_components"
        assert "weight" in INDEX_COMPONENTS.schema

    def test_index_weights(self):
        """INDEX_WEIGHTS should have correct structure."""
        assert INDEX_WEIGHTS.name == "index_weights"
        assert INDEX_WEIGHTS.partition_by is None

    def test_finance_indicator(self):
        """FINANCE_INDICATOR should have financial fields."""
        assert FINANCE_INDICATOR.name == "finance_indicator"
        assert "pe" in FINANCE_INDICATOR.schema
        assert "roe" in FINANCE_INDICATOR.schema

    def test_money_flow(self):
        """MONEY_FLOW should have flow columns."""
        assert MONEY_FLOW.name == "money_flow"
        assert "main_net_inflow" in MONEY_FLOW.schema
        assert MONEY_FLOW.priority == "P1"

    def test_north_flow(self):
        """NORTH_FLOW should exist."""
        assert NORTH_FLOW.name == "north_flow"
        assert NORTH_FLOW.priority == "P1"

    def test_industry_components(self):
        """INDUSTRY_COMPONENTS should exist."""
        assert INDUSTRY_COMPONENTS.name == "industry_components"

    def test_holder(self):
        """HOLDER should have holder info."""
        assert HOLDER.name == "holder"
        assert "holder_name" in HOLDER.schema

    def test_dividend(self):
        """DIVIDEND should have dividend fields."""
        assert DIVIDEND.name == "dividend"
        assert "dividend_cash" in DIVIDEND.schema

    def test_valuation(self):
        """VALUATION should have valuation fields."""
        assert VALUATION.name == "valuation"
        assert "pe" in VALUATION.schema
        assert "pb" in VALUATION.schema

    def test_unlock(self):
        """UNLOCK should exist."""
        assert UNLOCK.name == "unlock"

    def test_spot_snapshot(self):
        """SPOT_SNAPSHOT should have snapshot fields."""
        assert SPOT_SNAPSHOT.name == "spot_snapshot"
        assert SPOT_SNAPSHOT.priority == "P0"
        assert SPOT_SNAPSHOT.storage_layer == "snapshot"

    def test_sector_flow_snapshot(self):
        """SECTOR_FLOW_SNAPSHOT should exist."""
        assert SECTOR_FLOW_SNAPSHOT.name == "sector_flow_snapshot"

    def test_hsgt_hold_snapshot(self):
        """HSGT_HOLD_SNAPSHOT should exist."""
        assert HSGT_HOLD_SNAPSHOT.name == "hsgt_hold_snapshot"

    def test_stock_minute(self):
        """STOCK_MINUTE should have minute fields."""
        assert STOCK_MINUTE.name == "stock_minute"
        assert "period" in STOCK_MINUTE.schema
        assert STOCK_MINUTE.storage_layer == "minute"
        assert STOCK_MINUTE.priority == "P2"

    def test_etf_minute(self):
        """ETF_MINUTE should exist."""
        assert ETF_MINUTE.name == "etf_minute"
        assert ETF_MINUTE.storage_layer == "minute"

    def test_securities(self):
        """SECURITIES should have security info."""
        assert SECURITIES.name == "securities"
        assert SECURITIES.aggregation_enabled is False

    def test_trade_calendar(self):
        """TRADE_CALENDAR should exist."""
        assert TRADE_CALENDAR.name == "trade_calendar"
        assert "is_trading_day" in TRADE_CALENDAR.schema

    def test_industry_list(self):
        """INDUSTRY_LIST should exist."""
        assert INDUSTRY_LIST.name == "industry_list"
        assert INDUSTRY_LIST.storage_layer == "meta"

    def test_concept_list(self):
        """CONCEPT_LIST should exist."""
        assert CONCEPT_LIST.name == "concept_list"

    def test_company_info(self):
        """COMPANY_INFO should exist."""
        assert COMPANY_INFO.name == "company_info"

    def test_factor_cache(self):
        """FACTOR_CACHE should exist."""
        assert FACTOR_CACHE.name == "factor_cache"
        assert FACTOR_CACHE.priority == "P1"

    def test_financial_report(self):
        """FINANCIAL_REPORT should exist."""
        assert FINANCIAL_REPORT.name == "financial_report"
        assert FINANCIAL_REPORT.priority == "P0"

    def test_financial_benefit(self):
        """FINANCIAL_BENEFIT should exist."""
        assert FINANCIAL_BENEFIT.name == "financial_benefit"

    def test_industry_mapping(self):
        """INDUSTRY_MAPPING should exist."""
        assert INDUSTRY_MAPPING.name == "industry_mapping"

    def test_concept_components(self):
        """CONCEPT_COMPONENTS should exist."""
        assert CONCEPT_COMPONENTS.name == "concept_components"

    def test_fund_portfolio(self):
        """FUND_PORTFOLIO should exist."""
        assert FUND_PORTFOLIO.name == "fund_portfolio"

    def test_share_change(self):
        """SHARE_CHANGE should exist."""
        assert SHARE_CHANGE.name == "share_change"

    def test_holding_change(self):
        """HOLDING_CHANGE should exist."""
        assert HOLDING_CHANGE.name == "holding_change"

    def test_macro_data(self):
        """MACRO_DATA should exist."""
        assert MACRO_DATA.name == "macro_data"
        assert MACRO_DATA.storage_layer == "meta"

    def test_margin_detail(self):
        """MARGIN_DETAIL should exist."""
        assert MARGIN_DETAIL.name == "margin_detail"

    def test_margin_underlying(self):
        """MARGIN_UNDERLYING should exist."""
        assert MARGIN_UNDERLYING.name == "margin_underlying"

    def test_option_daily(self):
        """OPTION_DAILY should exist."""
        assert OPTION_DAILY.name == "option_daily"
        assert OPTION_DAILY.priority == "P3"

    def test_status_change(self):
        """STATUS_CHANGE should exist."""
        assert STATUS_CHANGE.name == "status_change"

    def test_call_auction(self):
        """CALL_AUCTION should exist."""
        assert CALL_AUCTION.name == "call_auction"
        assert CALL_AUCTION.priority == "P3"


class TestSchemaRegistry:
    """Test global SCHEMA_REGISTRY and functions."""

    def test_schema_registry_exists(self):
        """SCHEMA_REGISTRY should exist."""
        assert SCHEMA_REGISTRY is not None
        assert isinstance(SCHEMA_REGISTRY, TableRegistry)

    def test_all_default_tables_registered(self):
        """All _DEFAULT_TABLES should be registered."""
        for table in [
            STOCK_DAILY,
            ETF_DAILY,
            INDEX_DAILY,
            FUTURES_DAILY,
            CONVERSION_BOND_DAILY,
            INDEX_COMPONENTS,
            INDEX_WEIGHTS,
        ]:
            assert SCHEMA_REGISTRY.has(table.name)

    def test_get_table_schema_returns_table(self):
        """get_table_schema should return CacheTable."""
        schema = get_table_schema("stock_daily")
        assert schema is not None
        assert schema.name == "stock_daily"

    def test_get_table_schema_returns_none_for_missing(self):
        """get_table_schema should return None for missing."""
        schema = get_table_schema("nonexistent_table")
        assert schema is None

    def test_list_tables_returns_sorted_list(self):
        """list_tables should return sorted list of table names."""
        tables = list_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0
        assert tables == sorted(tables)

    def test_init_schemas_is_idempotent(self):
        """init_schemas should be safe to call multiple times."""
        initial_tables = list_tables()
        init_schemas()
        after_tables = list_tables()
        assert initial_tables == after_tables
