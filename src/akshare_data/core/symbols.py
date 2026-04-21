"""
utils/symbol.py
股票代码格式转换工具（统一实现）。

支持的格式:
- 聚宽格式: '600519.XSHG', '000001.XSHE'
- AkShare格式: 'sh600519', 'sz000001'
- BaoStock格式: 'sh.600519', 'sz.000001'
- 纯数字: '600519', '000001'

主要函数:
- format_stock_symbol: 统一转为6位纯数字
- jq_code_to_ak: 聚宽格式 -> AkShare格式
- ak_code_to_jq: AkShare格式 -> 聚宽格式
- normalize_symbol: 标准化代码格式
"""

import re


def format_stock_symbol(symbol):
    """
    将各种格式的股票代码统一转为 6 位纯数字字符串。

    支持: sh600000, sz000001, 600000.XSHG, 000001.XSHE, sh.600000, sz.000001, 600000, 000001
    以及场外基金 (.OF) 和期货 (.CCFX) 格式

    参数:
        symbol: 股票代码（各种格式）

    返回:
        str: 6位纯数字股票代码（期货代码保留原始格式）

    示例:
        format_stock_symbol('sh600000') -> '600000'
        format_stock_symbol('600519.XSHG') -> '600519'
        format_stock_symbol('sh.600000') -> '600000'
        format_stock_symbol('000001') -> '000001'
        format_stock_symbol('159001.OF') -> '159001'
    """
    if symbol is None:
        return None

    symbol = str(symbol).strip()

    # 移除 baostock 格式前缀 sh. / sz. / bj.
    if re.match(r"^(sh|sz|bj)\.", symbol):
        symbol = symbol[3:]

    # 移除 sh/sz/bj 前缀
    if symbol.startswith("sh") or symbol.startswith("sz") or symbol.startswith("bj"):
        symbol = symbol[2:]

    # 移除 .XSHG/.XSHE/.SH/.SZ 后缀
    if (
        symbol.endswith(".XSHG")
        or symbol.endswith(".XSHE")
        or symbol.endswith(".SH")
        or symbol.endswith(".SZ")
    ):
        symbol = symbol[:6]

    # 移除 .OF 后缀（场外基金）
    if symbol.upper().endswith(".OF"):
        symbol = symbol.split(".")[0]

    # .CCFX 期货代码保留原始格式（不做 zfill）
    if symbol.upper().endswith(".CCFX"):
        return symbol.split(".")[0]

    return symbol.zfill(6)


def jq_code_to_ak(code):
    """
    聚宽代码格式 -> 带前缀的 AkShare 格式。

    参数:
        code: 聚宽格式代码，如 '600519.XSHG'

    返回:
        str: AkShare格式代码，如 'sh600519'

    示例:
        jq_code_to_ak('600519.XSHG') -> 'sh600519'
        jq_code_to_ak('000001.XSHE') -> 'sz000001'
        jq_code_to_ak('sh600519') -> 'sh600519' (不变)
    """
    if code is None:
        return None

    code = str(code)

    if code.endswith(".XSHG"):
        return "sh" + code[:6]
    elif code.endswith(".XSHE"):
        return "sz" + code[:6]
    elif code.startswith("sh") or code.startswith("sz"):
        return code
    else:
        # 纯数字，按首位判断交易所
        c = code.zfill(6)
        if c.startswith("6"):
            return "sh" + c
        else:
            return "sz" + c


def ak_code_to_jq(code):
    """
    AkShare 格式 -> 聚宽格式。

    参数:
        code: AkShare格式代码，如 'sh600519'

    返回:
        str: 聚宽格式代码，如 '600519.XSHG'

    示例:
        ak_code_to_jq('sh600519') -> '600519.XSHG'
        ak_code_to_jq('sz000001') -> '000001.XSHE'
    """
    if code is None:
        return None

    code = str(code)

    if code.startswith("sh"):
        return code[2:] + ".XSHG"
    elif code.startswith("sz"):
        return code[2:] + ".XSHE"
    elif code.endswith(".XSHG") or code.endswith(".XSHE"):
        return code
    else:
        # 纯数字，按首位判断
        c = code.zfill(6)
        if c.startswith("6"):
            return c + ".XSHG"
        else:
            return c + ".XSHE"


def extract_code_num(symbol: str) -> str:
    """
    提取6位纯数字股票代码。

    参数:
        symbol: 股票代码（各种格式）

    返回:
        str: 6位纯数字股票代码

    示例:
        extract_code_num('sh600000') -> '600000'
        extract_code_num('600519.XSHG') -> '600519'
        extract_code_num('sh.600000') -> '600000'
        extract_code_num('000001') -> '000001'
    """
    if symbol is None:
        return None

    symbol = str(symbol).strip()

    # BaoStock 格式 sh.600000 / sz.000001
    if re.match(r"^(sh|sz|bj)\.", symbol):
        return symbol[3:].zfill(6)

    if symbol.startswith("sh") or symbol.startswith("sz") or symbol.startswith("bj"):
        return symbol[2:].zfill(6)
    if ".XSHG" in symbol or ".XSHE" in symbol:
        return symbol.split(".")[0].zfill(6)
    return symbol.zfill(6)


def normalize_symbol(symbol):
    """
    统一股票代码格式为 6 位数字（兼容各种输入格式）。

    这是 format_stock_symbol 的别名，用于向后兼容。

    参数:
        symbol: 股票代码（各种格式）

    返回:
        str: 6位纯数字股票代码
    """
    return format_stock_symbol(symbol)


# JQData API 兼容别名
normalize_code = normalize_symbol


def get_symbol_prefix(symbol):
    """
    获取股票代码前缀（交易所标识）。

    参数:
        symbol: 股票代码

    返回:
        str: 'sh' 或 'sz'

    示例:
        get_symbol_prefix('600519.XSHG') -> 'sh'
        get_symbol_prefix('000001.XSHE') -> 'sz'
    """
    code = normalize_symbol(symbol)
    if code.startswith("6"):
        return "sh"
    return "sz"


def is_valid_stock_code(symbol):
    """
    验证是否为有效的股票代码格式。

    参数:
        symbol: 股票代码

    返回:
        bool: 是否有效
    """
    if symbol is None:
        return False

    symbol = str(symbol)

    # 匹配各种有效格式
    patterns = [
        r"^(sh|sz)[0-9]{6}$",  # sh600000, sz000001
        r"^[0-9]{6}\.[XSHGXSHE]{4}$",  # 600000.XSHG, 000001.XSHE
        r"^(sh|sz)\.[0-9]{6}$",  # sh.600000, sz.000001 (BaoStock)
        r"^[0-9]{6}$",  # 600000, 000001 (pure digits)
    ]

    for pattern in patterns:
        if re.match(pattern, symbol):
            return True

    return False


# 兼容别名
format_stock_symbol_for_akshare = format_stock_symbol


def is_gem_or_star(code: str) -> bool:
    """
    判断是否为创业板或科创板

    创业板: 300xxx
    科创板: 688xxx
    """
    c = normalize_symbol(code)
    return c.startswith("300") or c.startswith("688")


def calculate_limit_price(
    prev_close: float, code: str, direction: str = "up"
) -> float | None:
    """
    计算涨跌停价

    参数:
        prev_close: 前收盘价
        code: 股票代码
        direction: 'up'=涨停, 'down'=跌停

    返回:
        涨跌停价，若 prev_close 无效则返回 None
    """
    if prev_close is None or prev_close <= 0:
        return None

    c = normalize_symbol(code)

    limit_ratio = 0.10
    if is_gem_or_star(c):
        limit_ratio = 0.20

    if direction == "up":
        return round(prev_close * (1 + limit_ratio), 2)
    else:
        return round(prev_close * (1 - limit_ratio), 2)


# ── BaoStock 格式支持 ────────────────────────────────────────────


def jq_code_to_baostock(code):
    """
    聚宽代码格式 -> BaoStock 格式。

    参数:
        code: 聚宽格式代码，如 '600519.XSHG'

    返回:
        str: BaoStock格式代码，如 'sh.600519'

    示例:
        jq_code_to_baostock('600519.XSHG') -> 'sh.600519'
        jq_code_to_baostock('000001.XSHE') -> 'sz.000001'
    """
    if code is None:
        return None

    code = str(code)

    if code.endswith(".XSHG"):
        return "sh." + code[:6]
    elif code.endswith(".XSHE"):
        return "sz." + code[:6]
    elif code.startswith("sh.") or code.startswith("sz."):
        return code
    elif code.startswith("sh") or code.startswith("sz"):
        return code[:2] + "." + code[2:]
    else:
        # 纯数字，按首位判断交易所
        c = code.zfill(6)
        if c.startswith("6"):
            return "sh." + c
        else:
            return "sz." + c


def baostock_to_jq(code):
    """
    BaoStock 格式 -> 聚宽格式。

    参数:
        code: BaoStock格式代码，如 'sh.600519'

    返回:
        str: 聚宽格式代码，如 '600519.XSHG'

    示例:
        baostock_to_jq('sh.600519') -> '600519.XSHG'
        baostock_to_jq('sz.000001') -> '000001.XSHE'
    """
    if code is None:
        return None

    code = str(code)

    if code.startswith("sh."):
        return code[3:] + ".XSHG"
    elif code.startswith("sz."):
        return code[3:] + ".XSHE"
    elif code.endswith(".XSHG") or code.endswith(".XSHE"):
        return code
    else:
        # 尝试其他格式
        return ak_code_to_jq(code)


def ak_code_to_baostock(code):
    """
    AkShare 格式 -> BaoStock 格式。

    参数:
        code: AkShare格式代码，如 'sh600519'

    返回:
        str: BaoStock格式代码，如 'sh.600519'

    示例:
        ak_code_to_baostock('sh600519') -> 'sh.600519'
        ak_code_to_baostock('sz000001') -> 'sz.000001'
    """
    if code is None:
        return None

    code = str(code)

    if code.startswith("sh"):
        return "sh." + code[2:]
    elif code.startswith("sz"):
        return "sz." + code[2:]
    elif code.startswith("sh.") or code.startswith("sz."):
        return code
    else:
        # 纯数字，按首位判断
        c = code.zfill(6)
        if c.startswith("6"):
            return "sh." + c
        else:
            return "sz." + c


def baostock_to_ak(code):
    """
    BaoStock 格式 -> AkShare 格式。

    参数:
        code: BaoStock格式代码，如 'sh.600519'

    返回:
        str: AkShare格式代码，如 'sh600519'

    示例:
        baostock_to_ak('sh.600519') -> 'sh600519'
        baostock_to_ak('sz.000001') -> 'sz000001'
    """
    if code is None:
        return None

    code = str(code)

    if code.startswith("sh."):
        return "sh" + code[3:]
    elif code.startswith("sz."):
        return "sz" + code[3:]
    elif code.startswith("sh") or code.startswith("sz"):
        return code
    else:
        # 纯数字，按首位判断
        c = code.zfill(6)
        if c.startswith("6"):
            return "sh" + c
        else:
            return "sz" + c


def ts_code_to_jq(code: str) -> str | None:
    """
    Tushare 格式 -> 聚宽格式。

    参数:
        code: Tushare格式代码，如 '000001.SZ', '600000.SH'

    返回:
        str: 聚宽格式代码，如 '000001.XSHE', '600000.XSHG'

    示例:
        ts_code_to_jq('000001.SZ') -> '000001.XSHE'
        ts_code_to_jq('600000.SH') -> '600000.XSHG'
    """
    if code is None:
        return None

    code = str(code)

    if code.endswith(".SH"):
        return code[:6] + ".XSHG"
    elif code.endswith(".SZ"):
        return code[:6] + ".XSHE"
    elif code.endswith(".BJ"):
        return code[:6] + ".XJSE"
    elif code.endswith(".XSHG"):
        return code
    elif code.endswith(".XSHE"):
        return code
    else:
        return None


__all__ = [
    "format_stock_symbol",
    "format_stock_symbol_for_akshare",
    "jq_code_to_ak",
    "ak_code_to_jq",
    "ts_code_to_jq",
    "normalize_symbol",
    "normalize_code",
    "extract_code_num",
    "get_symbol_prefix",
    "is_valid_stock_code",
    "is_gem_or_star",
    "calculate_limit_price",
    "jq_code_to_baostock",
    "baostock_to_jq",
    "ak_code_to_baostock",
    "baostock_to_ak",
]
