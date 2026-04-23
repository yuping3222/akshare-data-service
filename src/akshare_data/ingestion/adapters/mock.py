"""Mock adapter for the ingestion layer.

Generates synthetic data for testing and development.
"""
# ruff: noqa: F811

from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from akshare_data.ingestion.base import DataSource


class MockAdapter(DataSource):
    """Mock data source that generates synthetic data."""

    name = "mock"
    source_type = "mock"

    def get_daily_data(
        self,
        symbol: str,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        adjust: str = "qfq",
        **kwargs,
    ) -> pd.DataFrame:
        if start_date is None:
            start_date = "2024-01-01"
        if end_date is None:
            end_date = "2024-01-31"
        start = datetime.strptime(str(start_date), "%Y-%m-%d")
        end = datetime.strptime(str(end_date), "%Y-%m-%d")

        dates = []
        curr = start
        while curr <= end:
            if curr.weekday() < 5:
                dates.append(curr.strftime("%Y-%m-%d"))
            curr += timedelta(days=1)

        if not dates:
            return pd.DataFrame()

        count = len(dates)
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(dates),
                "open": np.random.uniform(10, 100, count),
                "high": np.random.uniform(10, 100, count),
                "low": np.random.uniform(10, 100, count),
                "close": np.random.uniform(10, 100, count),
                "volume": np.random.uniform(1000, 1000000, count),
                "symbol": symbol,
            }
        )
        df["high"] = df[["open", "close", "high"]].max(axis=1)
        df["low"] = df[["open", "close", "low"]].min(axis=1)
        return df

    def get_index_stocks(self, index_code: str, **kwargs) -> List[str]:
        return [f"{i:06d}" for i in range(1, 5001)]

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        stocks = self.get_index_stocks(index_code)
        if include_weights:
            weight = 100.0 / len(stocks)
            return pd.DataFrame(
                {
                    "index_code": [index_code] * len(stocks),
                    "code": stocks,
                    "stock_name": [f"stock_{s}" for s in stocks],
                    "weight": [weight] * len(stocks),
                }
            )
        return pd.DataFrame({"index_code": [index_code] * len(stocks), "code": stocks})

    def get_trading_days(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> List[str]:
        if start_date is None:
            start_date = "2020-01-01"
        if end_date is None:
            end_date = "2020-01-31"
        start = datetime.strptime(str(start_date), "%Y-%m-%d")
        end = datetime.strptime(str(end_date), "%Y-%m-%d")
        return [
            (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5
        ]

    def get_securities_list(
        self,
        security_type: str = "stock",
        date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": [f"{i:06d}" for i in range(1, 101)],
                "display_name": [f"股票{i}" for i in range(1, 101)],
                "type": [security_type] * 100,
                "start_date": ["2020-01-01"] * 100,
            }
        )

    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        return {
            "code": symbol,
            "display_name": f"股票{symbol}",
            "type": "stock",
            "start_date": "2020-01-01",
            "end_date": None,
            "industry": "制造业",
        }

    def get_minute_data(
        self,
        symbol: str,
        freq: str = "1min",
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if start_date is None:
            start_date = "2024-01-01"
        if end_date is None:
            end_date = "2024-01-05"
        return self.get_daily_data(symbol, str(start_date), str(end_date))

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "main_buy": [1000000.0, 1200000.0],
                "main_sell": [800000.0, 900000.0],
                "main_net": [200000.0, 300000.0],
                "retail_net": [-200000.0, -300000.0],
            }
        )

    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "north_buy": [1000000.0, 1200000.0],
                "north_sell": [800000.0, 900000.0],
                "north_net": [200000.0, 300000.0],
            }
        )

    def get_industry_stocks(
        self, industry_code: str, level: int = 1, **kwargs
    ) -> List[str]:
        return [f"{i:06d}" for i in range(1, 101)]

    def get_industry_mapping(self, symbol: str, level: int = 1, **kwargs) -> str:
        return "801010"

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "symbol": [symbol],
                "pe_ttm": [10.0],
                "pb": [1.5],
            }
        )

    def get_call_auction(
        self, symbol: str, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "time": ["09:25"],
                "open": [100.0],
                "high": [100.0],
                "low": [100.0],
                "close": [100.0],
                "volume": [1000],
            }
        )

    # --- Stub methods for backward compatibility with tests ---

    def get_equity_pledge(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "pledged_shares": [1000000], "date": ["2024-01-01"]}
        )

    def get_equity_pledge_rank(
        self, date: Optional[Union[str, date]] = None, top_n: int = 50, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [f"{i:06d}" for i in range(top_n)], "rank": list(range(1, top_n + 1))}
        )

    def get_goodwill_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "goodwill": [1e8], "date": ["2024-01-01"]}
        )

    def get_goodwill_impairment(
        self, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "impairment": [1e6], "date": ["2024-01-01"]}
        )

    def get_goodwill_by_industry(
        self, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"industry": ["制造业"], "avg_goodwill": [1e7], "date": ["2024-01-01"]}
        )

    def get_repurchase_data(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "repurchased_shares": [1e5], "date": ["2024-01-01"]}
        )

    def get_esg_rating(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "esg_score": [80.0], "date": ["2024-01-01"]}
        )

    def get_esg_rank(
        self, date: Optional[Union[str, date]] = None, top_n: int = 50, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [f"{i:06d}" for i in range(top_n)], "rank": list(range(1, top_n + 1))}
        )

    def get_performance_forecast(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "forecast_eps": [1.0], "date": ["2024-01-01"]}
        )

    def get_performance_express(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "express_eps": [0.8], "date": ["2024-01-01"]}
        )

    def get_analyst_rank(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"analyst": ["张三"], "rank": [1], "date": ["2024-01-01"]}
        )

    def get_research_report(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "title": ["深度报告"], "date": ["2024-01-01"]}
        )

    def get_chip_distribution(
        self, symbol: str, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"price": [10.0, 10.5, 11.0], "ratio": [0.2, 0.5, 0.3]}
        )

    def get_stock_bonus(
        self, symbol: str, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "bonus_ratio": [0.1], "date": ["2024-01-01"]}
        )

    def get_rights_issue(
        self, symbol: str, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "rights_price": [8.0], "date": ["2024-01-01"]}
        )

    def get_dividend_by_date(
        self, date: Optional[Union[str, date]] = None, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"code": ["600000"], "dividend": [1.0], "date": [date or "2024-01-10"]}
        )

    def get_management_info(
        self, symbol: str, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "name": ["张三"], "position": ["CEO"]}
        )

    def get_name_history(
        self, symbol: str, **kwargs
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "old_name": ["原名"], "change_date": ["2020-01-01"]}
        )

    def get_shibor_rate(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "rate": [2.0, 2.1]}
        )

    def get_macro_gdp(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"quarter": ["2024Q1"], "gdp": [250000.0], "growth": [5.3]}
        )

    def get_social_financing(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "total": [3e6, 3.1e6]}
        )

    def get_macro_exchange_rate(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "usd_cny": [7.2, 7.21]}
        )

    def get_fof_list(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"fund_code": ["FOF001"], "name": ["FOF基金"]}
        )

    def get_fof_nav(
        self, fund_code: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "nav": [1.2, 1.21]}
        )

    def get_lof_spot(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"fund_code": ["LOF001"], "name": ["LOF基金"]}
        )

    def get_lof_nav(self, fund_code: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "nav": [1.5, 1.51]}
        )

    def get_convert_bond_premium(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"bond_code": ["113009"], "premium": [20.0]}
        )

    def get_convert_bond_spot(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"bond_code": ["113009"], "price": [120.0]}
        )

    def get_industry_performance(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        period: str = "日k",
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"industry": ["银行"], "change_pct": [1.5], "date": ["2024-01-10"]}
        )

    def get_concept_performance(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        period: str = "daily",
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"concept": ["AI"], "change_pct": [3.0], "date": ["2024-01-10"]}
        )

    def get_stock_industry(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "industry": ["银行"]}
        )

    def get_hot_rank(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"rank": [1, 2, 3], "symbol": ["600000", "600519", "000001"]}
        )

    def get_restricted_release_calendar(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "release_date": ["2024-01-01"], "shares": [1000000]}
        )

    def get_realtime_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "price": [10.5], "volume": [100000]}
        )

    def get_hk_stocks(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"stockCode": ["00700", "09988"], "name": ["腾讯", "阿里"]}
        )

    def get_us_stocks(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["AAPL", "MSFT"], "close": [150.0, 300.0]}
        )

    def get_northbound_holdings(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "holdings": [1000.0], "date": ["2024-01-01"]}
        )

    def get_block_deal(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "price": [10.0], "date": ["2024-01-01"]}
        )

    def get_dragon_tiger_list(self, date: Union[str, date], **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"code": ["600000"], "direction": ["买"], "date": [date]}
        )

    def get_margin_data(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "margin_balance": [1000000.0], "date": ["2024-01-01"]}
        )

    def get_north_money_flow(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "north_money": [1000.0, 1100.0]}
        )

    def get_dividend_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "dividend": [1.0], "date": ["2024-01-01"]}
        )

    def get_restricted_release(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "free_shares": [1000000], "date": ["2024-01-01"]}
        )

    def get_balance_sheet(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "total_assets": [1e9], "report_date": ["2024-01-01"]}
        )

    def get_income_statement(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "revenue": [1e8], "report_date": ["2024-01-01"]}
        )

    def get_cash_flow(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "operating_cf": [1e7], "report_date": ["2024-01-01"]}
        )

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        stocks = self.get_index_stocks(index_code)
        if include_weights:
            weight = 100.0 / len(stocks)
            return pd.DataFrame(
                {
                    "index_code": [index_code] * len(stocks),
                    "code": stocks,
                    "stock_name": [f"stock_{s}" for s in stocks],
                    "weight": [weight] * len(stocks),
                }
            )
        return pd.DataFrame({"index_code": [index_code] * len(stocks), "code": stocks})

    def get_etf_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if start_date is None:
            start_date = "2024-01-01"
        if end_date is None:
            end_date = "2024-01-31"
        return self.get_daily_data(symbol, start_date, end_date)

    def get_basic_info(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "name": ["浦发银行"]}
        )

    def get_stock_valuation(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "pe": [8.5], "pb": [0.8], "date": ["2024-01-01"]}
        )

    def get_index_valuation(self, index_code: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"index_code": [index_code], "pe": [12.0], "pb": [1.4], "date": ["2024-01-01"]}
        )

    def get_top_shareholders(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "name": ["大股东"], "shares": [1000000]}
        )

    def get_institution_holdings(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "inst_count": [100], "date": ["2024-01-01"]}
        )

    def get_latest_holder_number(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "holders": [50000], "date": ["2024-01-01"]}
        )

    def get_new_stocks(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"code": ["601688"], "name": ["华泰证券"]}
        )

    def get_ipo_info(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"code": ["601688"], "issue_price": [12.0]}
        )

    def get_concept_list(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"concept_code": ["BK0001"], "concept_name": ["人工智能"]}
        )

    def get_concept_components(self, concept_code: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"concept_code": [concept_code], "code": ["600000"]}
        )

    def get_stock_concepts(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"code": [symbol], "concept": ["人工智能"]}
        )

    def get_equity_pledge(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "pledged_shares": [1e6], "date": ["2024-01-01"]}
        )

    def get_sw_index_daily(
        self,
        index_code: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "close": [3000.0, 3010.0]}
        )

    def get_option_list(self, market: str = "sse", **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"option_code": ["10000001"], "name": ["50ETF购1月2.5"]}
        )

    def get_option_daily(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "close": [0.15, 0.16]}
        )

    def get_futures_daily(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "close": [3000.0, 3010.0]}
        )

    def get_futures_spot(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"commodity": ["铜"], "price": [70000.0]}
        )

    def get_spot_em(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "price": [10.5]}
        )

    def get_lof_hist(
        self,
        symbol: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "close": [1.5, 1.51]}
        )

    def get_shareholder_changes(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "change_ratio": [0.05], "date": ["2024-01-01"]}
        )

    def get_insider_trading(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "name": ["高管"], "date": ["2024-01-01"]}
        )

    def get_equity_freeze(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "frozen_shares": [1000000], "date": ["2024-01-01"]}
        )

    def get_capital_change(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "total_shares": [1e9], "date": ["2024-01-01"]}
        )

    def get_earnings_forecast(self, symbol: str, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": [symbol], "forecast_eps": [1.0], "date": ["2024-01-01"]}
        )

    def get_fund_open_daily(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"fund_code": ["000001"], "nav": [1.5], "date": ["2024-01-01"]}
        )

    def get_fund_open_nav(
        self, fund_code: str,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "nav": [1.5, 1.51]}
        )

    def get_fund_open_info(self, fund_code: str, **kwargs) -> Dict[str, Any]:
        return {"fund_code": fund_code, "name": "测试基金"}

    def get_restricted_release_detail(
        self,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"symbol": ["600000"], "free_shares": [1000000], "date": ["2024-01-01"]}
        )

    def get_sw_industry(self, level: str = "1", **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {"industry_code": ["801010"], "industry_name": ["农林牧渔"]}
        )
