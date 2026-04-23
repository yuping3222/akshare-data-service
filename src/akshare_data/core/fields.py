"""
字段映射模块 - 统一的字段映射和代码名称查询

合并来源:
- akshare-one-enhanced: mappings/mapping_utils.py, mappings/*.csv
- 多源字段统一映射 (sina/em/ts/bs)
"""

import csv
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

# ============================================================================
# 字段类型定义
# ============================================================================

FLOAT_FIELDS: Set[str] = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "pre_close",
    "change",
    "pct_chg",
    "turnover",
    "amplitude",
    "limit_up",
    "limit_down",
    "weight",
    "openinterest",
    "settle",
}

INT_FIELDS: Set[str] = {
    "symbol",
}

STR_FIELDS: Set[str] = {
    "name",
    "stock_name",
    "datetime",
    "date",
    "trade_date",
    "adjust_flag",
    "trade_status",
}

DATE_FIELDS: Set[str] = {
    "datetime",
    "date",
    "trade_date",
    "report_date",
}


def get_field_type(field_name: str) -> str:
    """获取字段的类型。

    Args:
        field_name: 字段名

    Returns:
        str: 'float', 'int', 'str', 或 'date'
    """
    field_lower = field_name.lower()
    if field_lower in FLOAT_FIELDS:
        return "float"
    elif field_lower in INT_FIELDS:
        return "int"
    elif field_lower in DATE_FIELDS:
        return "date"
    elif field_lower in STR_FIELDS:
        return "str"
    else:
        return "str"


def validate_field_types(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """验证 DataFrame 中的字段类型是否符合预期。

    Args:
        df: 待验证的 DataFrame

    Returns:
        (is_valid, error_messages): 是否有效，错误消息列表
    """
    errors = []
    for col in df.columns:
        expected_type = get_field_type(col)
        if expected_type == "float":
            if not pd.api.types.is_float_dtype(
                df[col]
            ) and not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(f"字段 {col} 应为 float 类型，实际为 {df[col].dtype}")
        elif expected_type == "int":
            if not pd.api.types.is_integer_dtype(df[col]):
                errors.append(f"字段 {col} 应为 int 类型，实际为 {df[col].dtype}")
    return len(errors) == 0, errors


# ============================================================================
# 中文→英文字段映射表
# ============================================================================

CN_TO_EN = {
    "日期": "datetime",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude",
    "涨跌幅": "pct_chg",
    "涨跌额": "change",
    "换手率": "turnover",
    "涨停价": "limit_up",
    "跌停价": "limit_down",
    "昨收": "pre_close",
    "今开": "open",
    "最高价": "high",
    "最低价": "low",
    "收盘价": "close",
    "成交量(手)": "volume",
    "成交额(元)": "amount",
    "名称": "name",
    "代码": "symbol",
    "股票代码": "symbol",
    "证券代码": "symbol",
    "品种代码": "symbol",
    "成分券代码": "symbol",
    "成分股代码": "symbol",
    "成分券名称": "stock_name",
    "成分股名称": "stock_name",
    "品种名称": "stock_name",
    "权重": "weight",
    "占比": "weight",
    "持仓量": "openinterest",
    "结算价": "settle",
    "时间": "datetime",
    "trade_date": "datetime",
    "vol": "volume",
    "turn": "turnover",
    "pctChg": "pct_chg",
    "preclose": "pre_close",
    "adjustflag": "adjust_flag",
    "tradestatus": "trade_status",
}

# ============================================================================
# 多源字段统一映射 (sina/em/ts/bs 各源的字段名 → 统一英文名)
# ============================================================================

FIELD_MAPS = {
    # AkShare 东财接口
    "eastmoney": {
        "日期": "datetime",
        "date": "datetime",
        "开盘": "open",
        "open": "open",
        "最高": "high",
        "high": "high",
        "最低": "low",
        "low": "low",
        "收盘": "close",
        "close": "close",
        "成交量": "volume",
        "volume": "volume",
        "成交额": "amount",
        "amount": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_chg",
        "涨跌额": "change",
        "换手率": "turnover",
    },
    # AkShare 新浪接口
    "sina": {
        "date": "datetime",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "amount",
        "symbol": "symbol",
    },
    # Tushare
    "tushare": {
        "trade_date": "datetime",
        "ts_code": "symbol",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "pre_close": "pre_close",
        "change": "change",
        "pct_chg": "pct_chg",
        "vol": "volume",
        "amount": "amount",
    },
    # Baostock
    "baostock": {
        "date": "datetime",
        "code": "symbol",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "preclose": "pre_close",
        "volume": "volume",
        "amount": "amount",
        "adjustflag": "adjust_flag",
        "turn": "turnover",
        "tradestatus": "trade_status",
        "pctChg": "pct_chg",
    },
    # OHLCV 通用 (standardize_ohlcv)
    "ohlcv": {
        "日期": "datetime",
        "date": "datetime",
        "开盘": "open",
        "open": "open",
        "最高": "high",
        "high": "high",
        "最低": "low",
        "low": "low",
        "收盘": "close",
        "close": "close",
        "成交量": "volume",
        "volume": "volume",
        "成交额": "amount",
        "amount": "amount",
    },
    # 实时行情
    "realtime": {
        "代码": "code",
        "code": "code",
        "名称": "name",
        "name": "name",
        "开盘": "open",
        "open": "open",
        "最高": "high",
        "high": "high",
        "最低": "low",
        "low": "low",
        "收盘": "close",
        "close": "close",
        "成交量": "volume",
        "volume": "volume",
        "成交额": "amount",
        "amount": "amount",
        "日期": "date",
        "date": "date",
        "时间": "time",
        "time": "time",
    },
    # 期权链
    "options_chain": {
        "期权代码": "option_code",
        "option_code": "option_code",
        "标的代码": "underlying_code",
        "underlying_code": "underlying_code",
        "期权名称": "option_name",
        "option_name": "option_name",
        "到期日": "expiration_date",
        "expiration_date": "expiration_date",
        "行权价": "strike_price",
        "strike_price": "strike_price",
        "期权类型": "option_type",
        "option_type": "option_type",
        "昨持仓": "open_interest",
        "open_interest": "open_interest",
        "成交量": "volume",
        "volume": "volume",
        "持仓量": "open_interest",
        "最新价": "close",
        "close": "close",
        "买价": "bid",
        "bid": "bid",
        "卖价": "ask",
        "ask": "ask",
        "内涵价值": "intrinsic_value",
        "intrinsic_value": "intrinsic_value",
        "时间价值": "time_value",
        "time_value": "time_value",
    },
    # 期权实时
    "options_realtime": {
        "期权代码": "option_code",
        "option_code": "option_code",
        "标的代码": "underlying_code",
        "underlying_code": "underlying_code",
        "最新价": "close",
        "close": "close",
        "涨跌": "change",
        "change": "change",
        "涨跌幅": "pct_change",
        "pct_change": "pct_change",
        "买价": "bid",
        "bid": "bid",
        "卖价": "ask",
        "ask": "ask",
        "成交量": "volume",
        "volume": "volume",
        "持仓量": "open_interest",
        "open_interest": "open_interest",
        "行权价": "strike_price",
        "strike_price": "strike_price",
        "到期日": "expiration_date",
        "expiration_date": "expiration_date",
        "期权类型": "option_type",
        "option_type": "option_type",
        "隐含波动率": "iv",
        "iv": "iv",
        "Greeks": "greeks",
        "greeks": "greeks",
    },
    # 期权历史
    "options_hist": {
        "日期": "datetime",
        "date": "datetime",
        "开盘": "open",
        "open": "open",
        "最高": "high",
        "high": "high",
        "最低": "low",
        "low": "low",
        "收盘": "close",
        "close": "close",
        "成交量": "volume",
        "volume": "volume",
        "成交额": "amount",
        "amount": "amount",
        "持仓量": "open_interest",
        "open_interest": "open_interest",
        "行权价": "strike_price",
        "strike_price": "strike_price",
    },
    # 分钟数据
    "minute": {
        "时间": "datetime",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "money",
    },
    # 期货/期权日线
    "futures": {
        "date": "datetime",
        "日期": "datetime",
        "open": "open",
        "开盘": "open",
        "high": "high",
        "最高": "high",
        "low": "low",
        "最低": "low",
        "close": "close",
        "收盘": "close",
        "volume": "volume",
        "成交量": "volume",
        "openinterest": "openinterest",
        "持仓量": "openinterest",
        "settle": "settle",
        "结算价": "settle",
    },
}

# 股票代码补齐宽度
SYMBOL_ZFILL_WIDTH = 6

# ============================================================================
# 代码→名称映射数据 (从 full_combined_mapping.csv 提取)
# ============================================================================

# 映射表目录 (当前文件同级的 mappings/ 目录)
_MAPPINGS_DIR = Path(__file__).parent.parent / "data" / "mappings"

# 内存缓存
_code_name_mappings: Dict[str, Dict[str, str]] = {}


def _load_code_name_mapping(table_name: str) -> Dict[str, str]:
    """从 CSV 文件加载代码→名称映射"""
    if table_name in _code_name_mappings:
        return _code_name_mappings[table_name]

    csv_path = _MAPPINGS_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        _code_name_mappings[table_name] = {}
        return _code_name_mappings[table_name]

    mapping = {}
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get("code", "")).strip()
            name = row.get("name", "")
            if code.isdigit() and len(code) < SYMBOL_ZFILL_WIDTH:
                code = code.zfill(SYMBOL_ZFILL_WIDTH)
            mapping[code] = name

    _code_name_mappings[table_name] = mapping
    return mapping


def get_name_by_code(table_name: str, code: str) -> Optional[str]:
    """根据代码获取名称"""
    mapping = _load_code_name_mapping(table_name)
    return mapping.get(code)


def get_stock_name(stock_code: str) -> Optional[str]:
    """获取股票名称"""
    return get_name_by_code("stock_code_to_name", stock_code)


def get_index_name(index_code: str) -> Optional[str]:
    """获取指数名称"""
    return get_name_by_code("index_code_to_name", index_code)


def get_etf_name(etf_code: str) -> Optional[str]:
    """获取ETF名称"""
    return get_name_by_code("etf_code_to_name", etf_code)


def get_industry_name(industry_code: str) -> Optional[str]:
    """获取行业名称"""
    return get_name_by_code("industry_code_to_name", industry_code)


def get_option_name(option_symbol: str) -> Optional[str]:
    """获取期权名称"""
    return get_name_by_code("option_symbol_to_name", option_symbol)


def get_all_codes(table_name: str) -> List[str]:
    """获取所有代码"""
    mapping = _load_code_name_mapping(table_name)
    return list(mapping.keys())


def get_all_names(table_name: str) -> List[str]:
    """获取所有名称"""
    mapping = _load_code_name_mapping(table_name)
    return list(mapping.values())


def search_by_name(table_name: str, name_pattern: str) -> Dict[str, str]:
    """根据名称模式搜索"""
    mapping = _load_code_name_mapping(table_name)
    results = {}
    for code, name in mapping.items():
        if name_pattern.lower() in name.lower():
            results[code] = name
    return results


def get_option_underlying_patterns(underlying_code: str) -> List[str]:
    """获取期权底层资产匹配模式"""
    mapping = _load_code_name_mapping("option_underlying_patterns")
    return mapping.get(underlying_code, [underlying_code])


def preload_mappings():
    """预加载常用映射表"""
    for table in [
        "stock_code_to_name",
        "index_code_to_name",
        "etf_code_to_name",
        "industry_code_to_name",
        "option_underlying_patterns",
    ]:
        _load_code_name_mapping(table)


# ============================================================================
# 标准化列名工具函数
# ============================================================================


def standardize_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """根据源标识标准化列名"""
    if df is None or df.empty:
        return df
    result = df.copy()
    if source in FIELD_MAPS:
        result = result.rename(columns=FIELD_MAPS[source])
    return result


def standardize_columns_generic(
    df: pd.DataFrame, col_map: Dict[str, str]
) -> pd.DataFrame:
    """使用自定义映射标准化列名"""
    if df is None or df.empty:
        return df
    result = df.copy()
    for old_col, new_col in col_map.items():
        if old_col in result.columns:
            result[new_col] = result[old_col]
    return result


def select_ohlcv_columns(df: pd.DataFrame, include_amount: bool = True) -> pd.DataFrame:
    """选择标准 OHLCV 列"""
    if df is None or df.empty:
        return df
    select_cols = ["datetime", "open", "high", "low", "close", "volume"]
    if include_amount and "amount" in df.columns:
        select_cols.append("amount")
    available = [c for c in select_cols if c in df.columns]
    return df[available].copy()
