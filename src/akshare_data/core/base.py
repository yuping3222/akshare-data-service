"""
src/data_access/data_source.py
数据源抽象基类 - 定义统一的数据访问接口。

所有数据源实现必须继承此基类并实现所有抽象方法。
这确保了不同数据源（AkShare, Tushare, Mock 等）提供一致的 API。
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
import pandas as pd

from akshare_data.core.errors import DataSourceError


class FinanceMixin:
    """Mixin for financial data methods: indicators, reports, statements, dividends, and share changes."""

    def get_call_auction(
        self, symbol: str, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        """获取集合竞价数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_call_auction")

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """获取资产负债表数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_balance_sheet")

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """获取利润表数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_income_statement")

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        """获取现金流量表数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_cash_flow")

    def get_financial_report(self, symbol: str, report_type: str) -> pd.DataFrame:
        """获取财务报表。report_type: '现金流量表' | '资产负债表' | '利润表'"""
        raise NotImplementedError(f"{self.name} 不支持 get_financial_report")

    def get_financial_benefit(self, symbol: str, indicator: str) -> pd.DataFrame:
        """获取财务效益数据。替代 ak.stock_financial_benefit_ths()"""
        raise NotImplementedError(f"{self.name} 不支持 get_financial_benefit")

    def get_cashflow(self, symbol: str) -> pd.DataFrame:
        """获取现金流数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_cashflow")

    def get_dividend(self, symbol: str) -> pd.DataFrame:
        """获取分红数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_dividend")

    def get_share_change(self, symbol: str) -> pd.DataFrame:
        """获取股本变动数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_share_change")

    def get_unlock_schedule(self, symbol: str) -> pd.DataFrame:
        """获取解禁计划数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_unlock_schedule")


class ShareholderMixin:
    """Mixin for shareholder data: holders, changes, pledges, and unlock schedules."""

    def get_top10_holders(self, symbol: str) -> pd.DataFrame:
        """获取前十大股东数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_top10_holders")

    def get_top10_float_holders(self, symbol: str) -> pd.DataFrame:
        """获取前十大流通股东数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_top10_float_holders")

    def get_holder_count(self, symbol: str) -> pd.DataFrame:
        """获取股东人数数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_holder_count")

    def get_institutional_holders(self, symbol: str) -> pd.DataFrame:
        """获取机构持股数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_institutional_holders")


class IndustryMixin:
    """Mixin for industry and concept sector methods."""

    def get_industry_list(self, source: str = "em") -> pd.DataFrame:
        """获取行业列表。source: 'em' (东方财富) 等。"""
        raise NotImplementedError(f"{self.name} 不支持 get_industry_list")

    def get_industry_components(
        self, industry_name: str, source: str = "em"
    ) -> pd.DataFrame:
        """获取行业成分股。source: 'em' 等。"""
        raise NotImplementedError(f"{self.name} 不支持 get_industry_components")

    def get_concept_list(self) -> pd.DataFrame:
        """获取概念板块列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_concept_list")

    def get_concept_components(self, concept_name: str) -> pd.DataFrame:
        """获取概念板块成分股。"""
        raise NotImplementedError(f"{self.name} 不支持 get_concept_components")

    def get_sw_industry(self, level: str = "1") -> pd.DataFrame:
        """获取申万行业数据。level: '1' | '2' | '3'。"""
        raise NotImplementedError(f"{self.name} 不支持 get_sw_industry")


class FundMixin:
    """Mixin for fund-related methods: NAV, portfolio, dividends, and fund lists."""


class FuturesMixin:
    """Mixin for futures data methods."""

    def get_futures_daily(self, contract_code: str) -> pd.DataFrame:
        """获取期货日线数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_daily")


class OptionMixin:
    """Mixin for options data methods."""

    def get_option_list(self, **kwargs) -> pd.DataFrame:
        """获取期权列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_list")

    def get_option_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """获取期权日线数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_daily")


class BondMixin:
    """Mixin for bond and convertible bond data methods."""

    def get_bond_yield(self, symbol: str) -> pd.DataFrame:
        """获取债券收益率数据。替代 ak.bond_china_yield()。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_yield")

    def get_conversion_bond_list(self, **kwargs) -> pd.DataFrame:
        """获取可转债列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_conversion_bond_list")

    def get_conversion_bond_daily(self, symbol: str) -> pd.DataFrame:
        """获取可转债日线数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_conversion_bond_daily")


class HsgtMixin:
    """Mixin for Hong Kong Stock Connect / northbound fund flow methods."""


class QuoteExtensionMixin:
    """Mixin for extended quote methods: valuations, spot data, premarket, and raw market data."""

    def get_st_stocks(self) -> pd.DataFrame:
        """获取 ST 股票列表。返回 DataFrame with columns [代码, 名称]。"""
        raise NotImplementedError(f"{self.name} 不支持 get_st_stocks")

    def get_suspended_stocks(self) -> pd.DataFrame:
        """获取停牌股票列表。返回 DataFrame with columns [代码, 名称]。"""
        raise NotImplementedError(f"{self.name} 不支持 get_suspended_stocks")

    def get_index_valuation(self, index_code: str) -> pd.DataFrame:
        """获取指数估值历史数据。替代 ak.index_value_hist_fina()。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_valuation")

    def get_stock_valuation(self, symbol: str) -> pd.DataFrame:
        """获取个股估值数据。替代 ak.stock_a_lg_indicator()。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_valuation")

    def get_stock_pe_pb(self, symbol: str) -> pd.DataFrame:
        """获取个股 PE/PB 数据。替代 ak.stock_a_pe_and_pb()。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_pe_pb")

    def get_spot_em(self) -> pd.DataFrame:
        """获取全市场实时行情。替代 ak.stock_zh_a_spot_em()。"""
        raise NotImplementedError(f"{self.name} 不支持 get_spot_em")

    def get_securities_code_name(self) -> pd.DataFrame:
        """获取 A 股代码名称映射。替代 ak.stock_info_a_code_name()。"""
        raise NotImplementedError(f"{self.name} 不支持 get_securities_code_name")

    def get_stock_hist(
        self,
        symbol: str,
        period: str = "daily",
        start_date: str = "",
        end_date: str = "",
        adjust: str = "",
    ) -> pd.DataFrame:
        """获取股票历史行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_hist")

    def get_trade_dates(self) -> pd.DataFrame:
        """获取交易日历 DataFrame。"""
        raise NotImplementedError(f"{self.name} 不支持 get_trade_dates")


class CompanyInfoMixin:
    """Mixin for company info methods: announcements, management, index components, and corporate data."""

    def get_billboard_list(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """获取龙虎榜数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_billboard_list")

    def get_company_info(self, symbol: str) -> pd.DataFrame:
        """获取公司基本信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_company_info")

    def get_forecast(self, symbol: str) -> pd.DataFrame:
        """获取业绩预告数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_forecast")


class MoneyFlowMixin:
    """Mixin for sector and individual stock money flow methods."""


class MiscMixin:
    """Mixin for miscellaneous methods: holidays, ETF/index defaults, margin, macro, and misc data."""

    def get_etf_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """获取 ETF 日线数据。默认使用 get_daily_data 实现。"""
        return self.get_daily_data(
            symbol, start_date, end_date, adjust="none", **kwargs
        )

    def get_index_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """获取指数日线数据。默认使用 get_daily_data 实现。"""
        return self.get_daily_data(
            symbol, start_date, end_date, adjust="none", **kwargs
        )

    def get_lof_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """获取 LOF 日线数据。"""
        return self.get_daily_data(
            symbol, start_date, end_date, adjust="none", **kwargs
        )

    def get_lof_hist(self, symbol: str) -> pd.DataFrame:
        """获取 LOF 历史行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_lof_hist")

    def get_margin_detail(self, market: str, date: str) -> pd.DataFrame:
        """获取融资融券明细。market: 'sh' | 'sz'。"""
        raise NotImplementedError(f"{self.name} 不支持 get_margin_detail")

    def get_margin_underlying(self, market: str) -> pd.DataFrame:
        """获取融资融券标的信息。market: 'sh' | 'sz'。"""
        raise NotImplementedError(f"{self.name} 不支持 get_margin_underlying")

    def get_macro_raw(self, indicator: str) -> pd.DataFrame:
        """获取宏观数据原始 DataFrame。indicator: pmi/cpi/ppi/gdp/m2/interest_rate/exchange_rate/rmb。"""
        raise NotImplementedError(f"{self.name} 不支持 get_macro_raw")

    def get_etf_hist(self, symbol: str) -> pd.DataFrame:
        """获取 ETF 历史行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_etf_hist")


class DataSource(
    ABC,
    FinanceMixin,
    ShareholderMixin,
    IndustryMixin,
    FundMixin,
    FuturesMixin,
    OptionMixin,
    BondMixin,
    HsgtMixin,
    QuoteExtensionMixin,
    CompanyInfoMixin,
    MoneyFlowMixin,
    MiscMixin,
):
    """
    数据源抽象基类。

    定义了所有数据源必须实现的接口，包括:
    - 日线数据获取
    - 指数成分股查询
    - 交易日历
    - 股票列表
    - 分钟数据
    - 资金流向
    - 财务数据

    所有方法返回 pandas DataFrame 或标准 Python 类型。
    """

    # 数据源名称标识
    name: str = "abstract"

    # 数据源类型
    source_type: str = "abstract"

    @abstractmethod
    def get_daily_data(
        self,
        symbol: str,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取日线行情数据。

                Args:
                    symbol: 股票/ETF/指数代码，支持多种格式
                        - 'sh600000' / 'sz000001' (前缀格式)
                        - '600000.XSHG' / '000001.XSHE' (聚宽格式)
                        - '600000' / '000001' (纯代码)
                    start_date: 起始日期，支持 'YYYY-MM-DD' / datetime / date (可选)
                    end_date: 结束日期 (可选)
                    adjust: 复权类型
                        - 'qfq': 前复权 (默认)
                        - 'hfq': 后复权
                        - 'none': 不复权

                Returns:
                    DataFrame 标准化字段:
                        - datetime: 日期 (datetime)
                        - open: 开盘价 (float)
                        - high: 最高价 (float)
                        - low: 最低价 (float)
                        - close: 收盘价 (float)
                        - volume: 成交量 (int)
                        - amount: 成交额 (float, 可选)

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        """
        获取指数成分股详情（含权重）。

                Args:
                    index_code: 指数代码，支持:
                        - '000300.XSHG' (沪深300)
                        - '000905.XSHG' (中证500)
                        - '000016.XSHG' (上证50)
                        - '000852.XSHG' (中证1000)
                        - '399006.XSHE' (创业板指)
                    include_weights: 是否包含权重信息

                Returns:
                    DataFrame:
                        - index_code: 指数代码
                        - code: 成分股代码
                        - stock_name: 股票名称
                        - weight: 权重 (如果 include_weights=True)
                        - effective_date: 生效日期

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取交易日列表。

                Args:
                    start_date: 起始日期 (可选)
                    end_date: 结束日期 (可选)

                Returns:
                    List[str]: 交易日列表 ['2020-01-02', '2020-01-03', ...]

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取证券列表。

                Args:
                    security_type: 证券类型
                        - 'stock': A股股票
                        - 'etf': ETF基金
                        - 'index': 指数
                        - 'lof': LOF基金
                        - 'fund': 场内基金
                    date: 查询日期 (可选，默认最新)

                Returns:
                    DataFrame:
                        - code: 证券代码
                        - display_name: 证券名称
                        - type: 证券类型
                        - start_date: 上市日期

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_security_info(self, symbol: str, **kwargs) -> pd.DataFrame:
        """
        获取单个证券基本信息。

                Args:
                    symbol: 证券代码

                Returns:
                    Dict:
                        - code: 代码
                        - display_name: 名称
                        - type: 类型
                        - start_date: 上市日期
                        - end_date: 退市日期 (如有)
                        - industry: 行业 (如有)

                Raises:
                    DataSourceError: 信息获取失败
        """
        pass

    @abstractmethod
    def get_minute_data(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取分钟线数据。

                Args:
                    symbol: 证券代码
                    freq: 频率
                        - '1min': 1分钟
                        - '5min': 5分钟
                        - '15min': 15分钟
                        - '30min': 30分钟
                        - '60min': 60分钟
                    start_date: 起始日期 (可选)
                    end_date: 结束日期 (可选)

                Returns:
                    DataFrame:
                        - datetime: 时间戳
                        - open, high, low, close, volume

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取资金流向数据。

                Args:
                    symbol: 股票代码
                    start_date: 起始日期 (可选)
                    end_date: 结束日期 (可选)

                Returns:
                    DataFrame:
                        - date: 日期
                        - main_buy: 主力买入
                        - main_sell: 主力卖出
                        - main_net: 主力净额
                        - retail_net: 散户净额

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取北向资金流向。

                Args:
                    start_date: 起始日期 (可选)
                    end_date: 结束日期 (可选)

                Returns:
                    DataFrame:
                        - date: 日期
                        - north_buy: 北向买入
                        - north_sell: 北向卖出
                        - north_net: 北向净额

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_industry_stocks(
        self, industry_code: str, level: int = 1, **kwargs
    ) -> pd.DataFrame:
        """
        获取行业成分股。

                Args:
                    industry_code: 行业代码 (申万行业)
                        - '801010': 农林牧渔
                        - '801030': 基础化工
                        等
                    level: 行业级别 (1/2/3)

                Returns:
                    List[str]: 股票代码列表

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def get_industry_mapping(
        self, symbol: str, level: int = 1, **kwargs
    ) -> pd.DataFrame:
        """
        获取股票所属行业。

                Args:
                    symbol: 股票代码
                    level: 行业级别 (1/2/3)

                Returns:
                    str: 行业代码

                Raises:
                    DataSourceError: 数据获取失败
        """
        pass

    def health_check(self) -> Dict[str, Any]:
        """数据源健康检查。"""
        try:
            import time

            start = time.time()
            days = self.get_trading_days()
            latency = (time.time() - start) * 1000
            return {
                "status": "ok",
                "message": f"获取到 {len(days)} 个交易日",
                "latency_ms": round(latency, 2),
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "latency_ms": None}

    def get_source_info(self) -> Dict[str, Any]:
        """获取数据源信息。"""
        return {
            "name": self.name,
            "type": self.source_type,
            "description": f"数据源: {self.name}",
        }


__all__ = [
    "DataSource",
    "FinanceMixin",
    "ShareholderMixin",
    "IndustryMixin",
    "FundMixin",
    "FuturesMixin",
    "OptionMixin",
    "BondMixin",
    "HsgtMixin",
    "QuoteExtensionMixin",
    "CompanyInfoMixin",
    "MoneyFlowMixin",
    "MiscMixin",
    "DataSourceError",
]
