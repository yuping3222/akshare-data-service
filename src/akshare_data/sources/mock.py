"""Mock 数据源: 用于离线测试和演示"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

from ..core.base import DataSource


class MockSource(DataSource):
    @property
    def name(self) -> str:
        return "mock"

    @property
    def source_type(self) -> str:
        return "mock"

    def get_daily_data(
        self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq", **kwargs
    ) -> pd.DataFrame:
        """生成随机日线数据"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

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
        """返回 5000 只模拟股票"""
        return [f"{i:06d}" for i in range(1, 5001)]

    def get_index_components(
        self, index_code: str, include_weights: bool = True, **kwargs
    ) -> pd.DataFrame:
        """返回模拟指数成分股"""
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
        return pd.DataFrame(
            {
                "index_code": [index_code] * len(stocks),
                "code": stocks,
            }
        )

    def get_trading_days(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs
    ) -> List[str]:
        if start_date is None:
            start_date = "2020-01-01"
        if end_date is None:
            end_date = "2020-01-31"
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        return [
            (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5
        ]

    def get_securities_list(
        self, security_type: str = "stock", date: Optional[str] = None, **kwargs
    ) -> pd.DataFrame:
        """返回模拟证券列表"""
        return pd.DataFrame(
            {
                "code": [f"{i:06d}" for i in range(1, 101)],
                "display_name": [f"股票{i}" for i in range(1, 101)],
                "type": [security_type] * 100,
                "start_date": ["2020-01-01"] * 100,
            }
        )

    def get_security_info(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """返回模拟证券信息"""
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
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """生成随机分钟线数据"""
        if start_date is None:
            start_date = "2024-01-01"
        if end_date is None:
            end_date = "2024-01-05"
        return self.get_daily_data(symbol, start_date, end_date)

    def get_money_flow(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """返回模拟资金流向"""
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
        self, start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs
    ) -> pd.DataFrame:
        """返回模拟北向资金"""
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
        """返回模拟行业成分股"""
        return [f"{i:06d}" for i in range(1, 101)]

    def get_industry_mapping(self, symbol: str, level: int = 1, **kwargs) -> str:
        """返回模拟行业映射"""
        return "801010"

    def get_finance_indicator(
        self,
        symbol: str,
        fields: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """返回模拟财务指标"""
        return pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "symbol": [symbol],
                "pe_ttm": [10.0],
                "pb": [1.5],
            }
        )

    def get_call_auction(
        self, symbol: str, date: Optional[str] = None, **kwargs
    ) -> pd.DataFrame:
        """返回模拟集合竞价数据"""
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
