"""分类推断器 - 基于函数名前缀推断接口分类"""

from __future__ import annotations

from typing import Dict

CATEGORY_RULES: Dict[str, str] = {
    "stock_": "equity",
    "fund_": "fund",
    "etf_": "fund",
    "index_": "index",
    "futures_": "futures",
    "option_": "options",
    "bond_": "bond",
    "macro_": "macro",
    "finance_": "finance",
    "fund_manager": "fund",
    "fund_nav": "fund",
    "sector_": "sector",
    "industry_": "sector",
    "concept_": "sector",
    "money_": "flow",
    "fund_flow": "flow",
    "north_": "flow",
    "dragon_": "event",
    "limit_": "event",
    "margin_": "event",
    "pledge": "corporate",
    "repurchase": "corporate",
    "insider": "corporate",
    "esg_": "corporate",
    "performance_": "corporate",
    "analyst": "corporate",
    "research": "corporate",
    "shareholder": "corporate",
    "dividend": "corporate",
    "bonus": "corporate",
    "chip_": "corporate",
    "management": "corporate",
    "goodwill": "corporate",
    "trading_": "meta",
    "securities": "meta",
    "stock_info": "meta",
    "spot_": "market",
    "convert_": "bond",
    "shibor": "macro",
    "lpr": "macro",
    "cpi": "macro",
    "ppi": "macro",
    "gdp": "macro",
    "pmi": "macro",
    "m2": "macro",
    "rate": "macro",
    "exchange": "macro",
    "social_financing": "macro",
    "sw_": "sector",
    "lof_": "fund",
    "fof_": "fund",
    "hf_": "futures",
    "currency_": "market",
}


class CategoryInferrer:
    """基于规则推断接口分类"""

    def infer(self, func_name: str) -> str:
        """推断函数分类"""
        for prefix, category in CATEGORY_RULES.items():
            if func_name.startswith(prefix) or func_name == prefix.rstrip("_"):
                return category
        return "other"
