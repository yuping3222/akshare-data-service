"""AkShare data source adapter - 配置驱动的薄分发器

通过 __getattr__ 动态路由所有 get_xxx() 调用到 fetcher.fetch()。
不再有 100+ 个重复方法。

Design:
- 方法名自动映射到接口名（get_daily_data -> equity_daily）
- 所有字段映射、参数转换、多源 fallback 都由 akshare_registry.yaml 驱动
- 计算逻辑（Greeks/BS）保留为显式方法
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Union

import pandas as pd

from akshare_data.core.base import DataSource
from akshare_data.core.errors import DataSourceError, SourceUnavailableError, ErrorCode
from akshare_data.core.symbols import (
    jq_code_to_ak as _jq_code_to_ak,
    ak_code_to_jq as _ak_code_to_jq,
)
from akshare_data.sources.akshare.fetcher import fetch

logger = logging.getLogger(__name__)


def find_date_column(df: pd.DataFrame) -> str:
    """查找 DataFrame 中的日期列"""
    if df.empty:
        return ""

    date_candidates = [
        "trade_date",
        "datetime",
        "date",
        "日期",
        "report_date",
        "update_date",
    ]
    for col in date_candidates:
        if col in df.columns:
            return col

    return df.columns[0] if len(df.columns) > 0 else ""


# 旧方法名 -> 新接口名映射
_METHOD_TO_INTERFACE = {
    "get_daily_data": "equity_daily",
    "get_minute_data": "equity_minute",
    "get_realtime_data": "equity_realtime",
    "get_index_daily": "index_daily",
    "get_etf_daily": "etf_daily",
    "get_futures_hist_data": "futures_daily",
    "get_futures_realtime_data": "futures_realtime",
    "get_futures_main_contracts": "futures_main_contracts",
    "get_options_chain": "options_chain",
    "get_options_realtime_data": "options_realtime",
    "get_options_expirations": "options_expirations",
    "get_options_hist_data": "options_hist",
    "get_convert_bond_premium": "convert_bond_premium",
    "get_convert_bond_spot": "convert_bond_spot",
    "get_convert_bond_list": "convert_bond_premium",
    "get_convert_bond_info": "convert_bond_spot",
    "get_lpr_rate": "macro_lpr",
    "get_pmi_index": "macro_pmi",
    "get_cpi_data": "macro_cpi",
    "get_ppi_data": "macro_ppi",
    "get_m2_supply": "macro_m2",
    "get_shibor_rate": "macro_shibor",
    "get_social_financing": "macro_social_financing",
    "get_macro_gdp": "macro_gdp",
    "get_macro_exchange_rate": "macro_exchange_rate",
    "get_finance_indicator": "finance_indicator",
    "get_balance_sheet": "balance_sheet",
    "get_income_statement": "income_statement",
    "get_cash_flow": "cash_flow",
    "get_financial_metrics": "financial_metrics",
    "get_basic_info": "basic_info",
    "get_money_flow": "money_flow",
    "get_north_money_flow": "north_money_flow",
    "get_northbound_holdings": "northbound_holdings",
    "get_northbound_top_stocks": "northbound_top_stocks",
    "get_dragon_tiger_list": "dragon_tiger_list",
    "get_dragon_tiger_summary": "dragon_tiger_summary",
    "get_limit_up_pool": "limit_up_pool",
    "get_limit_down_pool": "limit_down_pool",
    "get_block_deal": "block_deal",
    "get_block_deal_summary": "block_deal",
    "get_margin_data": "margin_data",
    "get_margin_summary": "margin_summary",
    "get_equity_pledge": "equity_pledge",
    "get_equity_pledge_rank": "equity_pledge_rank",
    "get_restricted_release": "restricted_release",
    "get_restricted_release_detail": "restricted_release_detail",
    "get_restricted_release_calendar": "restricted_release_calendar",
    "get_dividend_data": "dividend_data",
    "get_dividend_by_date": "dividend_by_date",
    "get_stock_bonus": "stock_bonus",
    "get_rights_issue": "rights_issue",
    "get_repurchase_data": "repurchase_data",
    "get_insider_trading": "insider_trading",
    "get_insider_trade": "insider_trading",
    "get_esg_rating": "esg_rating",
    "get_esg_rank": "esg_rank",
    "get_performance_forecast": "performance_forecast",
    "get_performance_express": "performance_express",
    "get_analyst_rank": "analyst_rank",
    "get_research_report": "research_report",
    "get_chip_distribution": "chip_distribution",
    "get_management_info": "management_info",
    "get_name_history": "name_history",
    "get_goodwill_data": "goodwill_data",
    "get_goodwill_impairment": "goodwill_impairment",
    "get_goodwill_by_industry": "goodwill_by_industry",
    "get_shareholder_changes": "shareholder_changes",
    "get_equity_freeze": "equity_freeze",
    "get_capital_change": "capital_change",
    "get_earnings_forecast": "earnings_forecast",
    "get_disclosure_news": "disclosure_news",
    "get_call_auction": "call_auction",
    "get_securities_list": "securities_list",
    "get_security_info": "security_info",
    "get_trading_days": "tool_trade_date_hist_sina",
    "get_st_stocks": "st_stocks",
    "get_suspended_stocks": "suspended_stocks",
    "get_index_stocks": "index_components",
    "get_index_components": "index_components",
    "get_index_constituents": "index_components",
    "get_index_list": "index_list",
    "get_index_weights": "index_weights",
    "get_index_weights_history": "index_weights_history",
    "get_index_valuation": "index_valuation",
    "get_etf_list": "etf_list",
    "get_lof_list": "lof_list",
    "get_fund_manager_info": "fund_manager_info",
    "get_fund_net_value": "fund_net_value",
    "get_fof_list": "fof_list",
    "get_fof_nav": "fof_nav",
    "get_lof_spot": "lof_spot",
    "get_lof_nav": "lof_nav",
    "get_fund_open_daily": "fund_open_daily",
    "get_fund_open_nav": "fund_open_nav",
    "get_fund_open_info": "fund_open_info",
    "get_sector_fund_flow": "sector_fund_flow",
    "get_main_fund_flow_rank": "main_fund_flow_rank",
    "get_industry_stocks": "industry_stocks",
    "get_industry_mapping": "industry_mapping",
    "get_industry_performance": "industry_performance",
    "get_concept_stocks": "concept_stocks",
    "get_concept_performance": "concept_performance",
    "get_stock_industry": "stock_industry",
    "get_hot_rank": "hot_rank",
    "get_sw_industry_list": "sw_industry_list",
    "get_sw_industry_daily": "sw_industry_daily",
    "get_news_data": "disclosure_news",
    # 可转债
    "get_conversion_bond_list": "bond_zh_cov",
    "get_conversion_bond_daily": "bond_zh_cov_daily",
    # 期权
    "get_option_list": "option_current_day_sse",
    "get_option_daily": "option_sse_daily_sina",
    "get_option_greeks": "option_sse_greeks_sina",
    # LOF基金
    "get_lof_hist": "fund_lof_hist_em",
    "get_lof_daily": "fund_lof_hist_em",
    # 期货
    "get_futures_daily": "futures_zh_daily_sina",
    "get_futures_spot": "futures_zh_spot",
    # 全市场实时行情
    "get_spot_em": "stock_zh_a_spot_em",
    # 概念板块
    "get_concept_components": "stock_board_concept_cons_em",
    "get_hk_stocks": "hk_stocks",
    "get_us_stocks": "us_stocks",
    "get_new_stocks": "new_stocks",
    "get_ipo_info": "ipo_info",
}


class AkShareAdapter(DataSource):
    """AkShare 数据源适配器 - 配置驱动薄分发器。

    所有 get_xxx() 调用通过 __getattr__ 动态路由到 fetcher.fetch()。
    接口定义、字段映射、多源 fallback 全部由 akshare_registry.yaml 驱动。
    """

    name = "akshare"
    source_type = "real"
    DEFAULT_DATA_SOURCES = ["sina", "east_money", "tushare", "baostock"]

    def __init__(
        self,
        use_cache: bool = True,
        cache_ttl_hours: int = 24,
        offline_mode: bool = False,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        data_sources: List[str] = None,
    ):
        self._use_cache = use_cache
        self._cache_ttl_hours = cache_ttl_hours
        self._offline_mode = offline_mode
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._data_sources = data_sources or [
            "sina",
            "east_money",
            "tushare",
            "baostock",
        ]

        self._akshare_available = False
        self._akshare = None
        try:
            import akshare

            self._akshare = akshare
            self._akshare_available = True
            logger.debug("akshare version: %s", akshare.__version__)
        except ImportError:
            logger.warning("akshare 未安装，数据源将不可用")

        self._scipy_available = False
        self._np_available = False
        try:
            from scipy.stats import norm
            from scipy.optimize import brentq
            import numpy as np

            self._norm = norm
            self._brentq = brentq
            self._np = np
            self._scipy_available = True
            self._np_available = True
        except ImportError:
            logger.warning("scipy/numpy 未安装，Greeks计算和隐含波动率功能将不可用")

        from akshare_data.core.stats import get_stats_collector

        self._stats = get_stats_collector()
        self._market_data_loaded = False
        self._get_call_auction = None

    def __getattr__(self, name: str):
        """动态路由：任何未定义的 get_xxx() 方法自动路由到 fetcher.fetch()"""
        if name.startswith("_"):
            raise AttributeError(name)

        interface_name = _METHOD_TO_INTERFACE.get(name)
        if interface_name is None:
            # 尝试 get_xxx -> xxx (去掉 get_ 前缀)
            if name.startswith("get_"):
                candidate = name[4:]
                interface_name = _METHOD_TO_INTERFACE.get(candidate)

        if interface_name is None:
            raise AttributeError(
                f"{self.__class__.__name__} 没有方法 '{name}'，"
                f"也未在 akshare_registry.yaml 中找到对应接口"
            )

        def dispatcher(*args, **kwargs):
            # Convert positional args to kwargs based on common patterns
            if args:
                param_names = ["symbol", "start_date", "end_date"]
                for i, arg in enumerate(args):
                    if i < len(param_names) and param_names[i] not in kwargs:
                        kwargs[param_names[i]] = arg

            # Special handling: convert single 'date' to 'start_date' + 'end_date'
            # for interfaces that require a date range but are called with a single date
            if interface_name == "dragon_tiger_list" and "date" in kwargs:
                date_val = kwargs.pop("date")
                if isinstance(date_val, (datetime, date)):
                    date_val = date_val.strftime("%Y-%m-%d")
                if "start_date" not in kwargs:
                    kwargs["start_date"] = date_val
                if "end_date" not in kwargs:
                    kwargs["end_date"] = date_val

            if self._offline_mode:
                raise SourceUnavailableError(
                    "离线模式下无缓存数据",
                    error_code=ErrorCode.SOURCE_UNAVAILABLE,
                    source=self.name,
                    symbol=kwargs.get("symbol", ""),
                )
            if not self._akshare_available:
                raise SourceUnavailableError(
                    "akshare 不可用",
                    source=self.name,
                    symbol=kwargs.get("symbol", ""),
                )
            return fetch(interface_name, akshare=self._akshare, **kwargs)

        return dispatcher

    # ── DataSource Abstract Methods Implementation ──────────────────

    def get_daily_data(self, symbol, start_date, end_date, adjust="qfq", **kwargs):
        return self.__getattr__("get_daily_data")(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            **kwargs,
        )

    def get_index_stocks(self, index_code, **kwargs):
        return self.__getattr__("get_index_stocks")(index_code=index_code, **kwargs)

    def get_index_components(self, index_code, include_weights=True, **kwargs):
        return self.__getattr__("get_index_components")(
            index_code=index_code, include_weights=include_weights, **kwargs
        )

    def get_trading_days(self, start_date=None, end_date=None, **kwargs):
        return self.__getattr__("get_trading_days")(
            start_date=start_date, end_date=end_date, **kwargs
        )

    def get_securities_list(self, security_type="stock", date=None, **kwargs):
        return self.__getattr__("get_securities_list")(
            security_type=security_type, date=date, **kwargs
        )

    def get_security_info(self, symbol, **kwargs):
        if not self._akshare_available:
            return {"code": symbol, "type": "unknown"}
        return self.__getattr__("get_security_info")(symbol=symbol, **kwargs)

    def get_minute_data(
        self, symbol, freq="1min", start_date=None, end_date=None, **kwargs
    ):
        kwargs["period"] = freq.replace("min", "")
        return self.__getattr__("get_minute_data")(
            symbol=symbol, start_date=start_date, end_date=end_date, **kwargs
        )

    def get_money_flow(self, symbol, start_date=None, end_date=None, **kwargs):
        return self.__getattr__("get_money_flow")(
            symbol=symbol, start_date=start_date, end_date=end_date, **kwargs
        )

    def get_north_money_flow(self, start_date=None, end_date=None, **kwargs):
        return self.__getattr__("get_north_money_flow")(
            start_date=start_date, end_date=end_date, **kwargs
        )

    def get_industry_stocks(self, industry_code, level=1, **kwargs):
        return self.__getattr__("get_industry_stocks")(
            industry_code=industry_code, level=level, **kwargs
        )

    def get_industry_mapping(self, symbol, level=1, **kwargs):
        if not self._akshare_available:
            return ""
        return self.__getattr__("get_industry_mapping")(
            symbol=symbol, level=level, **kwargs
        )

    def get_finance_indicator(
        self, symbol, fields=None, start_date=None, end_date=None, **kwargs
    ):
        return self.__getattr__("get_finance_indicator")(
            symbol=symbol,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    def get_call_auction(self, symbol, date=None, **kwargs):
        if self._market_data_loaded and self._get_call_auction is None:
            raise DataSourceError(
                "集合竞价数据不可用",
                source=self.name,
                symbol=symbol,
            )
        return self.__getattr__("get_call_auction")(symbol=symbol, date=date, **kwargs)

    def get_st_stocks(self, **kwargs):
        if not self._akshare_available:
            raise DataSourceError(
                "akshare 不可用",
                source=self.name,
            )
        return self.__getattr__("get_st_stocks")(**kwargs)

    def get_suspended_stocks(self, **kwargs):
        if not self._akshare_available:
            raise DataSourceError(
                "akshare 不可用",
                source=self.name,
            )
        return self.__getattr__("get_suspended_stocks")(**kwargs)

    def get_sector_fund_flow(self, sector_type: str = None, **kwargs):
        if sector_type and sector_type not in (
            "行业",
            "概念",
            "地域",
            "industry",
            "concept",
            "region",
        ):
            raise DataSourceError(
                f"无效的板块类型: {sector_type}",
                source=self.name,
            )
        return self.__getattr__("get_sector_fund_flow")(
            sector_type=sector_type, **kwargs
        )

    # ── 计算逻辑（不属于配置驱动，显式定义） ─────────────────────────

    def get_option_greeks(
        self, symbol: str, date: Union[str, date, datetime], **kwargs
    ) -> Dict[str, float]:
        """计算期权 Greeks"""
        from akshare_data.core.options import calculate_option_greeks

        if not self._scipy_available or not self._np_available:
            raise DataSourceError(
                "scipy/numpy 不可用，无法计算 Greeks",
                source=self.name,
                symbol=symbol,
            )


        float(kwargs.get("h", 0.01))
        r = float(kwargs.get("r", 0.03))
        S = float(kwargs.get("spot", 0))
        K = float(kwargs.get("strike", 0))
        sigma = float(kwargs.get("sigma", 0.2))
        option_type = kwargs.get("option_type", "call").lower()

        if S <= 0 or K <= 0 or sigma <= 0:
            hist_data = fetch(
                "options_hist",
                akshare=self._akshare,
                symbol=symbol,
                start_date=date,
                end_date=date,
                exchange=kwargs.get("exchange"),
            )
            if hist_data is not None and not hist_data.empty:
                if "close" in hist_data.columns:
                    S = float(kwargs.get("spot", hist_data["close"].iloc[-1]))
                if K <= 0 and "strike_price" in hist_data.columns:
                    K = float(kwargs.get("strike", hist_data["strike_price"].iloc[-1]))

        date_val = pd.to_datetime(date)
        expiration_date = kwargs.get("expiration_date", None)
        if expiration_date is not None:
            expiry = pd.to_datetime(expiration_date)
        else:
            expiry = date_val + pd.Timedelta(days=30)
        T = max((expiry - date_val).days / 365.0, 1e-10)

        if T <= 0:
            raise DataSourceError("到期时间必须为正", source=self.name, symbol=symbol)

        return calculate_option_greeks(S, K, T, r, sigma, option_type)

    def calculate_option_implied_vol(
        self,
        symbol: str,
        price: float,
        strike: float,
        expiry: Union[str, date, datetime],
        option_type: str,
        rate: float = 0.03,
        **kwargs,
    ) -> float:
        """计算隐含波动率"""
        if not self._scipy_available or not self._np_available:
            raise DataSourceError(
                "scipy/numpy 不可用，无法计算隐含波动率",
                source=self.name,
                symbol=symbol,
            )

        from akshare_data.core.options import black_scholes_price

        spot = float(kwargs.get("spot", 0))
        if spot <= 0:
            raise DataSourceError(
                "标的资产价格必须为正", source=self.name, symbol=symbol
            )

        today = pd.Timestamp.today()
        expiry_dt = pd.to_datetime(expiry)
        T = max((expiry_dt - today).days / 365.0, 1e-10)

        def objective(sigma):
            try:
                call = black_scholes_price(spot, strike, T, rate, sigma, "call")
                put = black_scholes_price(spot, strike, T, rate, sigma, "put")
                return call - price if option_type.lower() == "call" else put - price
            except Exception:
                return 1e10

        try:
            return self._brentq(objective, 1e-6, 5.0, xtol=1e-8)
        except Exception as e:
            raise DataSourceError(
                f"无法计算隐含波动率: {e}",
                source=self.name,
                symbol=symbol,
            )

    def black_scholes_price(
        self,
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str,
        **kwargs,
    ) -> float:
        """BS 期权定价"""
        from akshare_data.core.options import black_scholes_price as bs

        try:
            return bs(S, K, T, r, sigma, option_type)
        except ValueError as e:
            raise DataSourceError(str(e), source=self.name)

    def calculate_conversion_value(
        self, bond_price: float, conversion_ratio: float, stock_price: float, **kwargs
    ) -> Dict[str, Any]:
        """计算转债转换价值"""
        from akshare_data.core.options import calculate_conversion_value

        return calculate_conversion_value(bond_price, conversion_ratio, stock_price)

    # ── 信息/健康检查 ──────────────────────────────────────────────

    def get_source_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.source_type,
            "description": "AkShare 数据源适配器（配置驱动）",
            "akshare_available": self._akshare_available,
            "cache_enabled": self._use_cache,
            "offline_mode": self._offline_mode,
            "data_sources": self._data_sources,
        }

    def health_check(self) -> Dict[str, Any]:
        return {
            "status": "ok" if self._akshare_available else "degraded",
            "akshare_available": self._akshare_available,
            "cache_enabled": self._use_cache,
        }

    # ── Helper methods required by tests ────────────────────────────

    def _normalize_symbol(self, symbol: str) -> str:
        return _jq_code_to_ak(symbol)

    def _normalize_date(self, val) -> str:
        if isinstance(val, str):
            return val
        if isinstance(val, (datetime, date)):
            return val.strftime("%Y-%m-%d")
        return str(val)

    def _to_jq_format(self, code: str) -> str:
        return _ak_code_to_jq(code)

    def _record_request(
        self, source: str, latency: float, success: bool, error_type: str = None
    ):
        self._stats.record_request(source, latency, success, error_type)

    def _record_cache_hit(self, cache: str):
        self._stats.record_cache_hit(cache)

    def _record_cache_miss(self, cache: str):
        self._stats.record_cache_miss(cache)
