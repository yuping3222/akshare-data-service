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

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """获取财务指标数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_finance_indicator")

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

    def get_top10_holders_em(self, symbol: str) -> pd.DataFrame:
        """获取前十大股东数据（东方财富）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_top10_holders_em")

    def get_top10_float_holders_em(self, symbol: str) -> pd.DataFrame:
        """获取前十大流通股东数据（东方财富）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_top10_float_holders_em")

    def get_share_change_cninfo(
        self, symbol: str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """获取巨潮资讯股本变动数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_share_change_cninfo")

    def get_shareholder_change_ths(self, symbol: str) -> pd.DataFrame:
        """获取同花顺股东变动数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_shareholder_change_ths")

    def get_holding_change_em(
        self, symbol: str = None, date: str = None
    ) -> pd.DataFrame:
        """获取东方财富股东增减持数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_holding_change_em")

    def get_pledge_ratio_em(self, symbol: str) -> pd.DataFrame:
        """获取股权质押比例数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_pledge_ratio_em")

    def get_equity_mortgage_cninfo(self, symbol: str) -> pd.DataFrame:
        """获取巨潮资讯股权质押数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_equity_mortgage_cninfo")

    def get_unlock_queue_sina(self, symbol: str) -> pd.DataFrame:
        """获取新浪限售股解禁排队数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_unlock_queue_sina")

    def get_unlock_summary_em(self, symbol: str) -> pd.DataFrame:
        """获取东方财富限售股解禁汇总数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_unlock_summary_em")

    def get_unlock_detail_em(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取东方财富限售股解禁明细数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_unlock_detail_em")

    def get_unlock_summary(self) -> pd.DataFrame:
        """获取全市场限售股解禁汇总数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_unlock_summary")

    def get_top10_shareholders(self, symbol: str) -> pd.DataFrame:
        """获取前十大股东数据（多数据源 fallback）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_top10_shareholders")

    def get_shareholders(self, symbol: str) -> pd.DataFrame:
        """获取股东数据（多数据源 fallback）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_shareholders")

    def get_shareholder_changes(
        self, symbol: str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """获取股东增减持数据（多数据源 fallback）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_shareholder_changes")

    def get_fund_hold_stock(self, symbol: str) -> pd.DataFrame:
        """获取基金持股数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_hold_stock")

    def get_dividend_fhps(self, symbol: str = None, date: str = None) -> pd.DataFrame:
        """获取分红送股数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_dividend_fhps")

    def get_dividend_all(self) -> pd.DataFrame:
        """获取全市场分红数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_dividend_all")


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

    def get_company_industry_em(self, symbol: str) -> pd.DataFrame:
        """获取公司行业信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_company_industry_em")


class FundMixin:
    """Mixin for fund-related methods: NAV, portfolio, dividends, and fund lists."""

    def get_fund_name_list(self) -> pd.DataFrame:
        """获取基金名称列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_name_list")

    def get_fund_of_nav(self, symbol: str) -> pd.DataFrame:
        """获取场外基金历史净值。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_of_nav")

    def get_fund_open_daily(self) -> pd.DataFrame:
        """获取场外基金当日净值列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_open_daily")

    def get_fund_open_info(self, symbol: str) -> pd.DataFrame:
        """获取场外基金基本信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_open_info")

    def get_fund_net_value_hist(
        self, fund_code: str, indicator: str = "单位净值走势"
    ) -> pd.DataFrame:
        """获取基金历史净值。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_net_value_hist")

    def get_fund_portfolio(self, fund_code: str) -> pd.DataFrame:
        """获取基金持仓数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_portfolio")

    def get_fund_dividend(self, fund_code: str) -> pd.DataFrame:
        """获取基金分红拆分数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_dividend")

    def get_lof_spot(self) -> pd.DataFrame:
        """获取 LOF 实时行情列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_lof_spot")

    def get_lof_hist_min(
        self, symbol: str, start_date: str, end_date: str, period: str, adjust: str = ""
    ) -> pd.DataFrame:
        """获取 LOF 分钟行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_lof_hist_min")

    def get_fund_etf_spot_em(self) -> pd.DataFrame:
        """获取ETF实时行情（东方财富）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_etf_spot_em")

    def get_fund_etf_hist_sina(self, symbol: str) -> pd.DataFrame:
        """获取ETF历史行情（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_etf_hist_sina")

    def get_fund_index_fund_em(self) -> pd.DataFrame:
        """获取指数基金列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_index_fund_em")

    def get_fund_money_fund_info_em(self) -> pd.DataFrame:
        """获取货币基金列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_money_fund_info_em")

    def get_fund_portfolio_bond_hold_em(self, symbol: str) -> pd.DataFrame:
        """获取基金债券持仓。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_portfolio_bond_hold_em")

    def get_fund_announcement_dividend_em(self) -> pd.DataFrame:
        """获取基金分红公告。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_fund_announcement_dividend_em"
        )

    def get_fund_open_fund_daily_em(self) -> pd.DataFrame:
        """获取开放式基金每日净值。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_open_fund_daily_em")

    def get_fund_name_em(self) -> pd.DataFrame:
        """获取基金名称列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_fund_name_em")


class FuturesMixin:
    """Mixin for futures data methods."""

    def get_futures_daily(self, contract_code: str) -> pd.DataFrame:
        """获取期货日线数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_daily")

    def get_futures_main_sina(
        self, symbol: str, start_date: str = "19900101", end_date: str = "22220101"
    ) -> pd.DataFrame:
        """获取期货主力合约数据（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_main_sina")

    def get_futures_spot(self) -> pd.DataFrame:
        """获取期货实时行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_spot")

    def get_futures_main_em(self, symbol: str) -> pd.DataFrame:
        """获取期货主力合约行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_main_em")

    def get_futures_sina_main(self, symbol: str) -> pd.DataFrame:
        """获取新浪期货主力合约。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_sina_main")

    def get_futures_display_main(self, symbol: str) -> pd.DataFrame:
        """获取期货合约列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_display_main")

    def get_futures_settlement_price_sina(self, symbol: str) -> pd.DataFrame:
        """获取期货结算价（新浪）。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_futures_settlement_price_sina"
        )

    def get_futures_contract_info_shfe_dce_czce(self) -> pd.DataFrame:
        """获取期货合约信息。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_futures_contract_info_shfe_dce_czce"
        )

    def get_futures_comm_info(self, symbol: str) -> pd.DataFrame:
        """获取期货品种信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_comm_info")

    def get_futures_warehouse_receipt(self, symbol: str) -> pd.DataFrame:
        """获取期货仓单。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_warehouse_receipt")

    def get_futures_zh_tick_sina(self, symbol: str) -> pd.DataFrame:
        """获取期货tick数据（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_zh_tick_sina")

    def get_futures_zh_minute_sina(self, symbol: str, period: str) -> pd.DataFrame:
        """获取期货分钟数据（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_zh_minute_sina")

    def get_futures_global_hist_em(self) -> pd.DataFrame:
        """获取全球期货历史数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_global_hist_em")

    def get_futures_fees_info(self) -> pd.DataFrame:
        """获取期货手续费。"""
        raise NotImplementedError(f"{self.name} 不支持 get_futures_fees_info")


class OptionMixin:
    """Mixin for options data methods."""

    def get_option_current_day_sse(self) -> pd.DataFrame:
        """获取上交所期权当日行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_current_day_sse")

    def get_option_current_day_szse(self) -> pd.DataFrame:
        """获取深交所期权当日行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_current_day_szse")

    def get_option_cffex_hs300_spot(self) -> pd.DataFrame:
        """获取中金所沪深300期权行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_cffex_hs300_spot")

    def get_option_sse_greeks(self, symbol: str) -> pd.DataFrame:
        """获取上交所期权希腊字母。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_sse_greeks")

    def get_option_sse_daily(self, symbol: str) -> pd.DataFrame:
        """获取上交所期权日线数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_sse_daily")

    def get_option_sse_tick_sina(self, symbol: str) -> pd.DataFrame:
        """获取期权tick数据（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_sse_tick_sina")

    def get_option_finance_minute_sina(self, symbol: str) -> pd.DataFrame:
        """获取期权分钟数据（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_finance_minute_sina")

    def get_option_szse_daily_sina(self, symbol: str) -> pd.DataFrame:
        """获取深交所期权日线（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_option_szse_daily_sina")

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

    def get_bond_cb_jsl(self) -> pd.DataFrame:
        """获取可转债数据（集思录）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_cb_jsl")

    def get_bond_zh_hs_daily(self, symbol: str) -> pd.DataFrame:
        """获取可转债历史行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_zh_hs_daily")

    def get_bond_zh_hs_spot(self) -> pd.DataFrame:
        """获取沪深债券实时行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_zh_hs_spot")

    def get_bond_info_cm(self) -> pd.DataFrame:
        """获取债券基本信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_info_cm")

    def get_bond_cash_summary_sina(self) -> pd.DataFrame:
        """获取债券付息数据（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_cash_summary_sina")

    def get_bond_zh_repurchase_daily(self, symbol: str = "全部") -> pd.DataFrame:
        """获取国债逆回购日行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_zh_repurchase_daily")

    def get_bond_repo_zh_spot_sina(self) -> pd.DataFrame:
        """获取债券回购实时行情（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_repo_zh_spot_sina")

    def get_bond_zh_hs_repo(self) -> pd.DataFrame:
        """获取债券回购数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_zh_hs_repo")

    def get_bond_zh_cov_info_sina(self, symbol: str) -> pd.DataFrame:
        """获取可转债基本信息（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_bond_zh_cov_info_sina")

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

    def get_hsgt_north_net_flow(self, symbol: str = "北上") -> pd.DataFrame:
        """获取北向资金净流入。"""
        raise NotImplementedError(f"{self.name} 不支持 get_hsgt_north_net_flow")

    def get_hsgt_hold_stock(
        self, symbol: str = "北向", indicator: str = "今日"
    ) -> pd.DataFrame:
        """获取北向资金持股统计。"""
        raise NotImplementedError(f"{self.name} 不支持 get_hsgt_hold_stock")

    def get_hsgt_individual_stock_flow(
        self, stock: str, indicator: str = "北向资金"
    ) -> pd.DataFrame:
        """获取个股北向资金流入。"""
        raise NotImplementedError(f"{self.name} 不支持 get_hsgt_individual_stock_flow")

    def get_stock_hsgt_fund_flow_summary_em(self) -> pd.DataFrame:
        """获取沪深港通资金流向汇总。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_stock_hsgt_fund_flow_summary_em"
        )

    def get_stock_hsgt_hold_stock_em(self, market: str = "沪深港通") -> pd.DataFrame:
        """获取沪深港通持股股票。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_hsgt_hold_stock_em")

    def get_stock_hsgt_sh_hk_spot_em(self) -> pd.DataFrame:
        """获取沪港通实时行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_hsgt_sh_hk_spot_em")

    def get_ah_stock_list(self) -> pd.DataFrame:
        """获取AH股对照列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_ah_stock_list")

    def get_ah_stock_spot(self) -> pd.DataFrame:
        """获取AH股实时行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_ah_stock_spot")


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

    def get_index_daily_raw(self, symbol: str) -> pd.DataFrame:
        """直接获取指数日线数据（返回原始 DataFrame）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_daily_raw")

    def get_index_zh_a_hist(self, symbol: str, period: str = "daily") -> pd.DataFrame:
        """获取指数A股历史行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_zh_a_hist")

    def get_stock_minute_raw(
        self, symbol: str, period: str, start_date: str, end_date: str, adjust: str = ""
    ) -> pd.DataFrame:
        """获取股票分钟行情（原始）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_minute_raw")

    def get_etf_minute_raw(
        self,
        symbol: str,
        period: str,
        start_date: str,
        end_date: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """获取 ETF 分钟行情（原始）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_etf_minute_raw")

    def get_call_auction_raw(
        self, symbol: str, start_time: str = "09:15:00", end_time: str = "09:25:00"
    ) -> pd.DataFrame:
        """获取集合竞价原始数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_call_auction_raw")

    def get_stock_valuation_baidu(self, symbol: str, indicator: str) -> pd.DataFrame:
        """获取百度股票估值数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_valuation_baidu")

    def get_stock_individual_info(self, symbol: str) -> pd.DataFrame:
        """获取个股基本信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_individual_info")

    def get_financial_analysis_indicator(self, symbol: str) -> pd.DataFrame:
        """获取财务分析指标数据。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_financial_analysis_indicator"
        )

    def get_stock_zh_index_hist_min_em(
        self, symbol: str, period: str = "1", adjust: str = ""
    ) -> pd.DataFrame:
        """获取指数分钟历史数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_zh_index_hist_min_em")

    def get_stock_zh_a_premarket_em(self, symbol: str = "1") -> pd.DataFrame:
        """获取盘前竞价数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_zh_a_premarket_em")


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

    def get_forecast_ths(self, symbol: str, indicator: str) -> pd.DataFrame:
        """获取同花顺业绩预告数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_forecast_ths")

    def get_stock_info_index_name_sina(self) -> pd.DataFrame:
        """获取指数名称列表（新浪）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_info_index_name_sina")

    def get_stock_info_change_name(self, symbol: str) -> pd.DataFrame:
        """获取公司更名历史。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_info_change_name")

    def get_stock_management_change_ths(self, symbol: str) -> pd.DataFrame:
        """获取高管变动（同花顺）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_management_change_ths")

    def get_stock_employee_info_em(self) -> pd.DataFrame:
        """获取员工信息（东方财富）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_employee_info_em")

    def get_stock_management_info_em(self) -> pd.DataFrame:
        """获取管理人员信息（东方财富）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_management_info_em")

    def get_stock_cixinqr_cninfo(self, symbol: str) -> pd.DataFrame:
        """获取企业信息披露（巨潮）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_cixinqr_cninfo")

    def get_stock_cixinhhr_cninfo(self, symbol: str) -> pd.DataFrame:
        """获取企业回答调研（巨潮）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_cixinhhr_cninfo")

    def get_stock_em_performance_letters(self, symbol: str) -> pd.DataFrame:
        """获取业绩快报（东方财富）。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_stock_em_performance_letters"
        )

    def get_stock_report_disclosure(self, market: str = "沪深") -> pd.DataFrame:
        """获取财报披露计划。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_report_disclosure")

    def get_stock_yjkb_em(self, symbol: str) -> pd.DataFrame:
        """获取业绩快报（东方财富）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_yjkb_em")

    def get_suspension_em(self, date: str) -> pd.DataFrame:
        """获取停牌数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_suspension_em")

    def get_stock_info_sh_name_code(self, symbol: str = "sh") -> pd.DataFrame:
        """获取上交所股票代码名称。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_info_sh_name_code")

    def get_stock_info_sz_name_code(self, symbol: str = "sz") -> pd.DataFrame:
        """获取深交所股票代码名称。"""
        raise NotImplementedError(f"{self.name} 不支持 get_stock_info_sz_name_code")

    def get_index_stock_info(self) -> pd.DataFrame:
        """获取指数基本信息列表。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_stock_info")

    def get_index_component_sw(self, symbol: str) -> pd.DataFrame:
        """获取申万行业指数成分股。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_component_sw")

    def get_index_stock_cons(self, symbol: str) -> pd.DataFrame:
        """获取指数成分股。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_stock_cons")

    def get_index_stock_cons_weight_csindex(self, symbol: str) -> pd.DataFrame:
        """获取中证指数成分股及权重。"""
        raise NotImplementedError(
            f"{self.name} 不支持 get_index_stock_cons_weight_csindex"
        )

    def get_sw_index_info(self) -> pd.DataFrame:
        """获取申万行业指数信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_sw_index_info")

    def get_sw_index_cons(self, index_code: str) -> pd.DataFrame:
        """获取申万行业指数成分股。"""
        raise NotImplementedError(f"{self.name} 不支持 get_sw_index_cons")

    def get_sw_index_daily(self, index_code: str) -> pd.DataFrame:
        """获取申万行业指数日线行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_sw_index_daily")

    def get_sw_index_daily_spot(self) -> pd.DataFrame:
        """获取申万行业指数实时行情。"""
        raise NotImplementedError(f"{self.name} 不支持 get_sw_index_daily_spot")

    def get_index_stocks(self, index_code: str, **kwargs) -> pd.DataFrame:
        """获取指数成分股列表（仅代码）。"""
        raise NotImplementedError(f"{self.name} 不支持 get_index_stocks")


class MoneyFlowMixin:
    """Mixin for sector and individual stock money flow methods."""

    def get_sector_money_flow(
        self, sector_type: str = "industry", indicator: str = "今日"
    ) -> pd.DataFrame:
        """获取板块资金流向排名。sector_type: 'industry' | 'concept'。"""
        raise NotImplementedError(f"{self.name} 不支持 get_sector_money_flow")

    def get_individual_fund_flow(self, symbol: str, market: str = "sh") -> pd.DataFrame:
        """获取个股资金流向历史数据。"""
        raise NotImplementedError(f"{self.name} 不支持 get_individual_fund_flow")

    def get_individual_fund_flow_rank(self, indicator: str = "今日") -> pd.DataFrame:
        """获取个股资金流向排名。"""
        raise NotImplementedError(f"{self.name} 不支持 get_individual_fund_flow_rank")


class MiscMixin:
    """Mixin for miscellaneous methods: holidays, ETF/index defaults, margin, macro, and misc data."""

    def get_trade_holidays(self) -> pd.DataFrame:
        """获取节假日信息。"""
        raise NotImplementedError(f"{self.name} 不支持 get_trade_holidays")

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
