"""AkShare fetcher - 通用配置驱动数据获取

所有接口定义、字段映射、参数转换都通过 akshare_registry.yaml 配置驱动。
不再有 100+ 个独立的 fetch_xxx 函数。

Design:
- fetch(interface_name, **kwargs) 是唯一入口
- 从 akshare_registry.yaml 读取接口定义
- 按优先级尝试数据源，无 sources 时直接调用同名 akshare 函数
- 自动应用 input_mapping, param_transforms, output_mapping, column_types
- 内置限速控制（按 rate_limit_key）
"""
# ruff: noqa: E402

from __future__ import annotations

import inspect
import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd
from akshare_data.core.config_cache import ConfigCache
from akshare_data.core.errors import SourceUnavailableError, ErrorCode
from akshare_data.core.options import (
    black_scholes_price as _bs_price,
    calculate_option_greeks as _calc_greeks,
)
from akshare_data.core.symbols import (
    jq_code_to_ak as _jq_code_to_ak,
    format_stock_symbol,
)
from akshare_data.ingestion.router import DomainRateLimiter as _DomainRateLimiter

logger = logging.getLogger(__name__)

# ── 配置缓存 ──────────────────────────────────────────────────────────
# Delegated to shared ConfigCache (core/config_cache.py).
# Only the adapter cache is local to this module.

_RATE_LIMITER: Optional["RateLimiter"] = None
_ADAPTER_CACHE: Dict[str, Any] = {}


def _load_registry() -> Dict:
    """加载 akshare_registry.yaml（通过 ConfigCache）"""
    return ConfigCache.load_registry()


def _load_interfaces() -> Dict:
    """加载接口定义（合并手工配置与注册表元数据）"""
    # Raw data from shared cache
    raw_interfaces = ConfigCache.load_interfaces()
    registry = ConfigCache.load_registry()
    registry_interfaces = registry.get("interfaces", {})

    # 合并：手工配置优先（sources/input/output），注册表补充（signature/domains/probe）
    merged: Dict[str, Any] = {}

    for iface_name, iface_def in raw_interfaces.items():
        entry = dict(iface_def)

        # 从注册表补充元数据
        for source in iface_def.get("sources", []):
            func_name = source.get("func")
            if func_name and func_name in registry_interfaces:
                reg_def = registry_interfaces[func_name]
                if "domains" not in entry and "domains" in reg_def:
                    # Kept for backward compatibility — rate limiting now reads
                    # from config/sources/domains.yaml, not from this field.
                    entry["domains"] = reg_def["domains"]
                if "probe" not in entry and "probe" in reg_def:
                    entry["probe"] = reg_def["probe"]
                if "signature" not in entry and "signature" in reg_def:
                    entry["signature"] = reg_def["signature"]
                break

        merged[iface_name] = entry

    # 注册表中没有手工定义的接口也加入
    for func_name, iface_def in registry_interfaces.items():
        if func_name not in merged:
            merged[func_name] = iface_def
        if "interface_name" in iface_def:
            alias = iface_def["interface_name"]
            if alias not in merged:
                merged[alias] = iface_def

    logger.debug("Total %d interfaces after merge", len(merged))
    return merged


def _load_rate_limits() -> Dict:
    """加载限速配置（通过 ConfigCache）"""
    return ConfigCache.load_rate_limits()


def _load_source_registry() -> Dict:
    """加载数据源注册表（通过 ConfigCache）"""
    return ConfigCache.load_sources()


# ── 适配器工厂 ────────────────────────────────────────────────────────

# interface_name -> adapter method 映射
_INTERFACE_METHOD_MAP: Dict[str, Dict[str, str]] = {
    "equity_daily": {
        "TushareAdapter": "get_daily_data",
        "LixingerAdapter": "get_daily_data",
    },
    "equity_minute": {
        "TushareAdapter": "get_minute_data",
        "LixingerAdapter": "get_minute_data",
    },
    "securities_list": {
        "TushareAdapter": "get_securities_list",
        "LixingerAdapter": "get_securities_list",
    },
    "security_info": {
        "TushareAdapter": "get_security_info",
        "LixingerAdapter": "get_security_info",
    },
    "trading_days": {
        "TushareAdapter": "get_trading_days",
        "LixingerAdapter": "get_trading_days",
    },
    "index_stocks": {
        "TushareAdapter": "get_index_stocks",
        "LixingerAdapter": "get_index_stocks",
    },
    "index_components": {
        "TushareAdapter": "get_index_components",
        "LixingerAdapter": "get_index_components",
    },
    "money_flow": {
        "TushareAdapter": "get_money_flow",
        "LixingerAdapter": "get_money_flow",
    },
    "north_money_flow": {
        "TushareAdapter": "get_north_money_flow",
        "LixingerAdapter": "get_north_money_flow",
    },
    "industry_stocks": {
        "TushareAdapter": "get_industry_stocks",
        "LixingerAdapter": "get_industry_stocks",
    },
    "industry_mapping": {
        "TushareAdapter": "get_industry_mapping",
        "LixingerAdapter": "get_industry_mapping",
    },
    "finance_indicator": {
        "TushareAdapter": "get_finance_indicator",
        "LixingerAdapter": "get_finance_indicator",
    },
    "call_auction": {
        "TushareAdapter": "get_call_auction",
        "LixingerAdapter": "get_call_auction",
    },
    "st_stocks": {
        "TushareAdapter": "get_st_stocks",
        "LixingerAdapter": "get_st_stocks",
    },
    "suspended_stocks": {
        "TushareAdapter": "get_suspended_stocks",
        "LixingerAdapter": "get_suspended_stocks",
    },
    "equity_realtime": {
        "TushareAdapter": "get_realtime_data",
        "LixingerAdapter": "get_realtime_data",
    },
    "index_daily": {"TushareAdapter": None, "LixingerAdapter": "get_index_daily"},
    "etf_daily": {"TushareAdapter": None, "LixingerAdapter": "get_etf_daily"},
    "etf_list": {"TushareAdapter": None, "LixingerAdapter": "get_etf_list"},
    "fund_net_value": {"TushareAdapter": None, "LixingerAdapter": "get_fund_net_value"},
    "fund_manager_info": {
        "TushareAdapter": None,
        "LixingerAdapter": "get_fund_manager_info",
    },
    "index_list": {"TushareAdapter": None, "LixingerAdapter": "get_index_list"},
    "financial_report": {
        "TushareAdapter": "get_financial_report",
        "LixingerAdapter": None,
    },
    "dividend": {"TushareAdapter": "get_dividend", "LixingerAdapter": None},
    "top10_holders": {"TushareAdapter": "get_top10_holders", "LixingerAdapter": None},
    "top10_float_holders": {
        "TushareAdapter": "get_top10_float_holders",
        "LixingerAdapter": None,
    },
    "margin_detail": {"TushareAdapter": "get_margin_detail", "LixingerAdapter": None},
    "macro_cpi": {"TushareAdapter": "get_macro_raw", "LixingerAdapter": None},
    "macro_ppi": {"TushareAdapter": "get_macro_raw", "LixingerAdapter": None},
    "macro_gdp": {"TushareAdapter": "get_macro_raw", "LixingerAdapter": None},
    "macro_pmi": {"TushareAdapter": "get_macro_raw", "LixingerAdapter": None},
    "billboard_list": {"TushareAdapter": "get_billboard_list", "LixingerAdapter": None},
    "futures_daily": {"TushareAdapter": None, "LixingerAdapter": None},
    "stock_pe_pb": {"TushareAdapter": "get_stock_pe_pb", "LixingerAdapter": None},
}


def _get_adapter(source_name: str) -> Any:
    """获取或创建适配器实例（单例缓存）"""
    if source_name in _ADAPTER_CACHE:
        return _ADAPTER_CACHE[source_name]

    registry = _load_source_registry()
    source_def = registry.get(source_name, {})
    if source_def.get("type") != "adapter":
        return None

    adapter_class_name = source_def.get("adapter_class")
    if not adapter_class_name:
        return None

    # 延迟导入以避免循环依赖
    try:
        if adapter_class_name == "TushareAdapter":
            from akshare_data.sources.tushare_source import TushareAdapter as _cls
        elif adapter_class_name == "LixingerAdapter":
            from akshare_data.sources.lixinger_source import LixingerAdapter as _cls
        else:
            logger.warning("Unknown adapter class: %s", adapter_class_name)
            return None

        _ADAPTER_CACHE[source_name] = _cls()
        logger.debug("Created adapter instance: %s", adapter_class_name)
        return _ADAPTER_CACHE[source_name]
    except Exception as e:
        logger.error("Failed to create adapter %s: %s", adapter_class_name, e)
        return None


def _resolve_adapter_method(adapter: Any, interface_name: str) -> Optional[str]:
    """解析 interface_name 对应的 adapter 方法名"""
    adapter_class = type(adapter).__name__
    method_map = _INTERFACE_METHOD_MAP.get(interface_name, {})
    return method_map.get(adapter_class)


def _call_adapter_source(
    adapter: Any, interface_name: str, method_name: str, kwargs: Dict
) -> pd.DataFrame:
    """通过 DataSource 协议调用非 akshare 源"""
    method = getattr(adapter, method_name, None)
    if method is None:
        raise SourceUnavailableError(
            f"{type(adapter).__name__} 不支持接口 {interface_name} (方法 {method_name} 不存在)",
            error_code=ErrorCode.SOURCE_UNAVAILABLE,
            source=type(adapter).__name__,
        )

    try:
        result = method(**kwargs)
    except NotImplementedError:
        raise SourceUnavailableError(
            f"{type(adapter).__name__}.{method_name} 未实现",
            error_code=ErrorCode.SOURCE_UNAVAILABLE,
            source=type(adapter).__name__,
        )

    if result is None or (isinstance(result, pd.DataFrame) and result.empty):
        raise SourceUnavailableError(
            f"{type(adapter).__name__}.{method_name} 返回空数据",
            error_code=ErrorCode.SOURCE_UNAVAILABLE,
            source=type(adapter).__name__,
        )

    if not isinstance(result, pd.DataFrame):
        raise ValueError(
            f"{type(adapter).__name__}.{method_name} 返回类型不是 DataFrame"
        )

    return result


# ── 限速器（委托给 router.DomainRateLimiter） ─────────────────────────


class RateLimiter:
    """线程安全的按 key 限速器，委托给 router.DomainRateLimiter"""

    def __init__(self):
        intervals: Dict[str, float] = {"default": 0.5}
        rate_limits = _load_rate_limits()
        for key, cfg in rate_limits.items():
            if isinstance(cfg, dict) and "interval" in cfg:
                intervals[key] = float(cfg["interval"])
        # domain_map is empty — we pass rate_limit_keys directly
        self._limiter = _DomainRateLimiter(intervals=intervals, domain_map={})

    def wait(self, key: str = "default") -> None:
        self._limiter.wait_if_needed(key)

    def set_interval(self, key: str, interval: float) -> None:
        self._limiter.set_interval(key, interval)


def _get_rate_limiter() -> RateLimiter:
    global _RATE_LIMITER
    if _RATE_LIMITER is None:
        _RATE_LIMITER = RateLimiter()
    return _RATE_LIMITER


# ── 工具函数 ──────────────────────────────────────────────────────────


def _normalize_symbol(symbol: str) -> str:
    return _jq_code_to_ak(symbol)


def _strip_symbol(symbol: str) -> str:
    return format_stock_symbol(symbol)


def _transform_param(value: Any, transform: str) -> Any:
    """参数转换"""
    if value is None:
        return None

    if transform == "YYYYMMDD":
        if isinstance(value, (datetime, date, pd.Timestamp)):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            return value.replace("-", "").replace("/", "")
        return str(value)

    if transform == "to_ts_code":
        code = str(value).zfill(6)
        return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"

    if transform == "to_ak_code":
        code = str(value)
        for suffix in (".XSHG", ".XSHE", ".XBSE"):
            code = code.replace(suffix, "")
        return code

    if transform == "strip":
        return _strip_symbol(value)

    if transform.startswith("append_suffix:"):
        suffix = transform.split(":", 1)[1]
        return f"{value}{suffix}"

    if transform.startswith("prepend_prefix:SH/SZ"):
        code = str(value).lstrip("0")
        return f"SH{value}" if code.startswith("6") else f"SZ{value}"

    return value


def _to_pandas_type(type_name: str) -> str:
    return {
        "str": "str",
        "float": "float64",
        "int": "int64",
        "date": "datetime64[ns]",
        "datetime": "datetime64[ns]",
        "bool": "bool",
    }.get(type_name, "str")


# ── 核心 fetch 函数 ──────────────────────────────────────────────────


def fetch(
    interface_name: str,
    akshare=None,
    **kwargs,
) -> pd.DataFrame:
    """通用配置驱动数据获取。

    Args:
        interface_name: 接口名称，如 "equity_daily", "macro_cpi" 或 akshare 函数名
        akshare: akshare 模块实例（由 adapter 传入）
        **kwargs: 统一参数，如 symbol, start_date, end_date

    Returns:
        统一字段格式的 DataFrame

    Raises:
        ValueError: 接口未定义
        SourceUnavailableError: 所有数据源都失败
    """
    if akshare is None:
        import akshare as _ak

        akshare = _ak

    interfaces = _load_interfaces()
    iface = interfaces.get(interface_name)
    if iface is None:
        raise ValueError(f"接口 {interface_name} 未定义")

    sources = iface.get("sources", [])
    rate_key = iface.get("rate_limit_key", "default")
    rate_limiter = _get_rate_limiter()

    # 如果没有定义 sources，直接使用同名 akshare 函数
    if not sources:
        ak_func = getattr(akshare, interface_name, None)
        if ak_func is None:
            raise ValueError(f"akshare.{interface_name} 不存在")

        try:
            sig = inspect.signature(ak_func)
            valid_params = set(sig.parameters.keys())
            kwargs = {k: v for k, v in kwargs.items() if k in valid_params}
        except (TypeError, ValueError):
            pass

        rate_limiter.wait(rate_key)
        raw_df = ak_func(**kwargs)

        if raw_df is None or (isinstance(raw_df, pd.DataFrame) and raw_df.empty):
            raise SourceUnavailableError(
                f"接口 {interface_name} 返回空数据",
                error_code=ErrorCode.SOURCE_UNAVAILABLE,
                source="akshare",
            )

        if not isinstance(raw_df, pd.DataFrame):
            raise ValueError(f"akshare.{interface_name} 返回类型不是 DataFrame")

        raw_df.attrs["source"] = "akshare"
        raw_df.attrs["interface"] = interface_name
        return raw_df

    errors = []
    source_registry = _load_source_registry()

    for source in sources:
        if not source.get("enabled", True):
            continue

        source_name = source.get("name", "")
        source_type = source.get("type") or source_registry.get(source_name, {}).get(
            "type", "akshare"
        )

        # 限速
        rate_limiter.wait(rate_key)

        if source_type == "adapter":
            # ── Adapter 源路由 ──────────────────────────────────
            adapter = _get_adapter(source_name)
            if adapter is None:
                errors.append(f"{source_name}: 适配器未初始化")
                continue

            method_name = _resolve_adapter_method(adapter, interface_name)
            if not method_name:
                errors.append(f"{source_name}: 不支持接口 {interface_name}")
                continue

            # 构建参数（复用 _build_call_kwargs 但不走 akshare 签名过滤）
            call_kwargs = _build_adapter_kwargs(kwargs, source)

            try:
                raw_df = _call_adapter_source(
                    adapter, interface_name, method_name, call_kwargs
                )
            except SourceUnavailableError as e:
                errors.append(f"{source_name}: {e}")
                logger.debug("数据源 %s 调用失败: %s", source_name, e)
                continue

            # 标准化输出
            df = _normalize_output(raw_df, source)
            df.attrs["source"] = source_name
            df.attrs["interface"] = interface_name
            return df

        else:
            # ── AKShare 源路由（原有逻辑） ─────────────────────
            func_name = source.get("func")
            if not func_name:
                continue

            ak_func = getattr(akshare, func_name, None)
            if ak_func is None:
                errors.append(f"{source_name}: akshare.{func_name} 不存在")
                continue

            # 构建调用参数
            call_kwargs = _build_call_kwargs(kwargs, source, ak_func=ak_func)

            try:
                raw_df = ak_func(**call_kwargs)
            except Exception as e:
                errors.append(f"{source_name}: {e}")
                logger.debug("数据源 %s 调用 %s 失败: %s", source_name, func_name, e)
                continue

            if raw_df is None or (isinstance(raw_df, pd.DataFrame) and raw_df.empty):
                errors.append(f"{source_name}: 返回空数据")
                continue

            if not isinstance(raw_df, pd.DataFrame):
                errors.append(f"{source_name}: 返回类型不是 DataFrame")
                continue

            # 标准化输出
            df = _normalize_output(raw_df, source)
            df.attrs["source"] = source_name
            df.attrs["interface"] = interface_name
            return df

    raise SourceUnavailableError(
        f"所有数据源都失败: {interface_name}\n" + "\n".join(f"  {e}" for e in errors),
        error_code=ErrorCode.SOURCE_UNAVAILABLE,
        source="akshare",
    )


def _build_call_kwargs(kwargs: Dict, source: Dict, ak_func=None) -> Dict:
    """将统一参数映射到 akshare 函数参数"""
    input_mapping = source.get("input_mapping", {})
    param_transforms = source.get("param_transforms", {})

    result = {}

    # 映射已知参数
    for unified_name, source_name in input_mapping.items():
        if unified_name in kwargs:
            value = kwargs[unified_name]
            transform = param_transforms.get(unified_name)
            if transform:
                value = _transform_param(value, transform)
            result[source_name] = value

    # 传递未映射的参数
    for key, value in kwargs.items():
        if key not in input_mapping:
            result[key] = value

    # 过滤掉 akshare 函数不接受的参数
    if ak_func is not None:
        try:
            sig = inspect.signature(ak_func)
            valid_params = set(sig.parameters.keys())
            filtered_out = {k for k in result if k not in valid_params}
            if filtered_out:
                func_name = getattr(ak_func, "__name__", str(ak_func))
                logger.warning(
                    f"{func_name} 不接受参数 {filtered_out}，已自动过滤 (有效签名: {list(valid_params)})"
                )
            result = {k: v for k, v in result.items() if k in valid_params}
        except (TypeError, ValueError):
            pass

    return result


def _build_adapter_kwargs(kwargs: Dict, source: Dict, ak_func=None) -> Dict:
    """将统一参数映射到 Adapter 方法参数。

    Adapter 使用统一参数名（不经过 func->akshare 映射），
    但支持 param_transforms（如日期格式转换、代码格式转换）。
    """
    input_mapping = source.get("input_mapping", {})
    param_transforms = source.get("param_transforms", {})

    result = {}

    # 使用映射后的参数名
    for unified_name, source_name in input_mapping.items():
        if unified_name in kwargs:
            value = kwargs[unified_name]
            transform = param_transforms.get(unified_name)
            if transform:
                value = _transform_param(value, transform)
            result[source_name] = value

    # 传递未映射的参数
    for key, value in kwargs.items():
        if key not in input_mapping:
            result[key] = value

    # 可选：过滤掉 Adapter 方法不接受的参数
    if ak_func is not None:
        try:
            sig = inspect.signature(ak_func)
            valid_params = set(sig.parameters.keys())
            filtered_out = {k for k in result if k not in valid_params}
            if filtered_out:
                func_name = getattr(ak_func, "__name__", str(ak_func))
                logger.warning(
                    f"{func_name} 不接受参数 {filtered_out}，已自动过滤 (有效签名: {list(valid_params)})"
                )
            result = {k: v for k, v in result.items() if k in valid_params}
        except (TypeError, ValueError):
            pass

    return result


def _normalize_output(df: pd.DataFrame, source: Dict) -> pd.DataFrame:
    """标准化输出 DataFrame"""
    if df.empty:
        return df

    output_mapping = source.get("output_mapping", {})
    column_types = source.get("column_types", {})

    # 重命名列
    rename_map = {}
    for orig, unified in output_mapping.items():
        if orig in df.columns:
            rename_map[orig] = unified
    if rename_map:
        df = df.rename(columns=rename_map)

    # 转换列类型
    for col, type_name in column_types.items():
        if col not in df.columns:
            continue
        pandas_type = _to_pandas_type(type_name)
        try:
            if pandas_type == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(pandas_type)
        except (ValueError, TypeError):
            pass

    return df


# ── 配置重载（用于测试和热更新） ─────────────────────────────────────


def reload_config():
    """重载所有配置（用于测试和热更新）"""
    global _RATE_LIMITER
    ConfigCache.invalidate()
    _RATE_LIMITER = None
    _ADAPTER_CACHE.clear()


# ── 向后兼容：保留旧函数名作为别名 ──────────────────────────────────


def fetch_daily_data(akshare, symbol, start_date, end_date, adjust="qfq", **kwargs):
    return fetch(
        "equity_daily",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        **kwargs,
    )


def fetch_minute_data(
    akshare, symbol, freq="1min", start_date=None, end_date=None, **kwargs
):
    return fetch(
        "equity_minute",
        akshare=akshare,
        symbol=symbol,
        period=freq.replace("min", ""),
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_realtime_data(akshare, symbol, **kwargs):
    return fetch("equity_realtime", akshare=akshare, symbol=symbol, **kwargs)


def fetch_index_daily(akshare, symbol, start_date, end_date, **kwargs):
    return fetch(
        "index_daily",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_etf_daily(akshare, symbol, start_date, end_date, **kwargs):
    return fetch(
        "etf_daily",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_futures_hist_data(akshare, symbol, start_date, end_date, **kwargs):
    return fetch(
        "futures_daily",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_options_chain(akshare, symbol, exchange=None, **kwargs):
    return fetch(
        "options_chain", akshare=akshare, symbol=symbol, exchange=exchange, **kwargs
    )


def fetch_options_realtime_data(akshare, symbol, exchange=None, **kwargs):
    return fetch(
        "options_realtime", akshare=akshare, symbol=symbol, exchange=exchange, **kwargs
    )


def fetch_options_expirations(akshare, symbol, exchange=None, **kwargs):
    return fetch(
        "options_expirations",
        akshare=akshare,
        symbol=symbol,
        exchange=exchange,
        **kwargs,
    )


def fetch_options_hist_data(
    akshare, symbol, start_date, end_date, exchange=None, **kwargs
):
    return fetch(
        "options_hist",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_convert_bond_premium(akshare, **kwargs):
    return fetch("convert_bond_premium", akshare=akshare, **kwargs)


def fetch_convert_bond_spot(akshare, **kwargs):
    return fetch("convert_bond_spot", akshare=akshare, **kwargs)


def fetch_lpr_rate(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_lpr", akshare=akshare, start_date=start_date, end_date=end_date, **kwargs
    )


def fetch_pmi_index(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_pmi", akshare=akshare, start_date=start_date, end_date=end_date, **kwargs
    )


def fetch_cpi_data(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_cpi", akshare=akshare, start_date=start_date, end_date=end_date, **kwargs
    )


def fetch_ppi_data(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_ppi", akshare=akshare, start_date=start_date, end_date=end_date, **kwargs
    )


def fetch_m2_supply(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_m2", akshare=akshare, start_date=start_date, end_date=end_date, **kwargs
    )


def fetch_shibor_rate(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_shibor",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_social_financing(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_social_financing",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_macro_gdp(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_gdp", akshare=akshare, start_date=start_date, end_date=end_date, **kwargs
    )


def fetch_macro_exchange_rate(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "macro_exchange_rate",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_finance_indicator(
    akshare, symbol, fields=None, start_date=None, end_date=None, **kwargs
):
    return fetch(
        "finance_indicator",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_balance_sheet(akshare, symbol, start_date=None, end_date=None, **kwargs):
    return fetch(
        "balance_sheet",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_income_statement(akshare, symbol, start_date=None, end_date=None, **kwargs):
    return fetch(
        "income_statement",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_cash_flow(akshare, symbol, start_date=None, end_date=None, **kwargs):
    return fetch(
        "cash_flow",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_basic_info(akshare, symbol, **kwargs):
    return fetch("basic_info", akshare=akshare, symbol=symbol, **kwargs)


def fetch_money_flow(akshare, symbol, start_date=None, end_date=None, **kwargs):
    return fetch(
        "money_flow",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_north_money_flow(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "north_money_flow",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_northbound_holdings(akshare, hold_type="all", date=None, **kwargs):
    return fetch(
        "northbound_holdings",
        akshare=akshare,
        symbol=kwargs.get("symbol", ""),
        start_date=date,
        end_date=date,
        **kwargs,
    )


def fetch_northbound_top_stocks(akshare, date, direction="all", top_n=10, **kwargs):
    return fetch("northbound_top_stocks", akshare=akshare, date=date, **kwargs)


def fetch_dragon_tiger_list(akshare, date=None, **kwargs):
    # Convert single date to date range for stock_lhb_detail_em
    start = date or kwargs.pop("start_date", None)
    end = kwargs.pop("end_date", start)
    return fetch(
        "dragon_tiger_list", akshare=akshare, start_date=start, end_date=end, **kwargs
    )


def fetch_dragon_tiger_summary(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "dragon_tiger_summary",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_limit_up_pool(akshare, date, **kwargs):
    return fetch("limit_up_pool", akshare=akshare, date=date, **kwargs)


def fetch_limit_down_pool(akshare, date, **kwargs):
    return fetch("limit_down_pool", akshare=akshare, date=date, **kwargs)


def fetch_block_deal(akshare, date, **kwargs):
    return fetch("block_deal", akshare=akshare, date=date, **kwargs)


def fetch_block_deal_summary(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "block_deal",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_margin_data(akshare, date, **kwargs):
    return fetch(
        "margin_data",
        akshare=akshare,
        symbol=kwargs.get("symbol", ""),
        start_date=date,
        end_date=date,
        **kwargs,
    )


def fetch_margin_summary(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "margin_summary",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_equity_pledge(akshare, symbol=None, start_date=None, end_date=None, **kwargs):
    return fetch(
        "equity_pledge",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_equity_pledge_rank(akshare, date=None, top_n=50, **kwargs):
    return fetch(
        "equity_pledge_rank", akshare=akshare, date=date, top_n=top_n, **kwargs
    )


def fetch_restricted_release(
    akshare, symbol=None, start_date=None, end_date=None, **kwargs
):
    return fetch(
        "restricted_release",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_restricted_release_calendar(
    akshare, start_date=None, end_date=None, **kwargs
):
    return fetch(
        "restricted_release_calendar",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_restricted_release_detail(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "restricted_release_detail",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_stock_bonus(akshare, symbol, **kwargs):
    return fetch("stock_bonus", akshare=akshare, symbol=symbol, **kwargs)


def fetch_dividend_by_date(akshare, date=None, **kwargs):
    return fetch("dividend_by_date", akshare=akshare, date=date, **kwargs)


def fetch_rights_issue(akshare, symbol, **kwargs):
    return fetch("rights_issue", akshare=akshare, symbol=symbol, **kwargs)


def fetch_repurchase_data(
    akshare, symbol=None, start_date=None, end_date=None, **kwargs
):
    return fetch(
        "repurchase_data",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_esg_rating(akshare, symbol=None, start_date=None, end_date=None, **kwargs):
    return fetch(
        "esg_rating",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_esg_rank(akshare, date=None, top_n=50, **kwargs):
    return fetch("esg_rank", akshare=akshare, date=date, top_n=top_n, **kwargs)


def fetch_performance_forecast(
    akshare, symbol=None, start_date=None, end_date=None, **kwargs
):
    return fetch(
        "performance_forecast",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_analyst_rank(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "analyst_rank",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_research_report(
    akshare, symbol=None, start_date=None, end_date=None, **kwargs
):
    return fetch(
        "research_report",
        akshare=akshare,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_chip_distribution(akshare, symbol, **kwargs):
    return fetch("chip_distribution", akshare=akshare, symbol=symbol, **kwargs)


def fetch_management_info(akshare, symbol, **kwargs):
    return fetch("management_info", akshare=akshare, symbol=symbol, **kwargs)


def fetch_shareholder_change(akshare, symbol, **kwargs):
    return fetch("shareholder_changes", akshare=akshare, symbol=symbol, **kwargs)


def fetch_capital_change(akshare, symbol, **kwargs):
    return fetch("capital_change", akshare=akshare, symbol=symbol, **kwargs)


def fetch_earnings_forecast(akshare, symbol, **kwargs):
    return fetch("earnings_forecast", akshare=akshare, symbol=symbol, **kwargs)


def fetch_disclosure_news(akshare, symbol, **kwargs):
    return fetch("disclosure_news", akshare=akshare, symbol=symbol, **kwargs)


def fetch_call_auction(akshare, symbol, date=None, **kwargs):
    return fetch("call_auction", akshare=akshare, symbol=symbol, date=date, **kwargs)


def fetch_securities_list(akshare, security_type="stock", date=None, **kwargs):
    return fetch(
        "securities_list",
        akshare=akshare,
        security_type=security_type,
        date=date,
        **kwargs,
    )


def fetch_security_info(akshare, symbol, **kwargs):
    return fetch("security_info", akshare=akshare, symbol=symbol, **kwargs)


def fetch_trading_days(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "trading_days",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_st_stocks(akshare, **kwargs):
    return fetch("st_stocks", akshare=akshare, **kwargs)


def fetch_suspended_stocks(akshare, **kwargs):
    return fetch("suspended_stocks", akshare=akshare, **kwargs)


def fetch_index_stocks(akshare, index_code, **kwargs):
    return fetch("index_components", akshare=akshare, symbol=index_code, **kwargs)


def fetch_index_components(akshare, index_code, include_weights=True, **kwargs):
    return fetch("index_components", akshare=akshare, symbol=index_code, **kwargs)


def fetch_index_list(akshare, **kwargs):
    return fetch("index_list", akshare=akshare, **kwargs)


def fetch_etf_list(akshare, **kwargs):
    return fetch("etf_list", akshare=akshare, **kwargs)


def fetch_lof_list(akshare, **kwargs):
    return fetch("lof_list", akshare=akshare, **kwargs)


def fetch_fund_manager_info(akshare, fund_code, **kwargs):
    return fetch("fund_manager_info", akshare=akshare, symbol=fund_code, **kwargs)


def fetch_fund_net_value(akshare, fund_code, start_date=None, end_date=None, **kwargs):
    return fetch(
        "fund_net_value",
        akshare=akshare,
        symbol=fund_code,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_fof_list(akshare, **kwargs):
    return fetch("fof_list", akshare=akshare, **kwargs)


def fetch_fof_nav(akshare, fund_code, start_date=None, end_date=None, **kwargs):
    return fetch(
        "fof_nav",
        akshare=akshare,
        symbol=fund_code,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_lof_spot(akshare, **kwargs):
    return fetch("lof_spot", akshare=akshare, **kwargs)


def fetch_lof_nav(akshare, fund_code, **kwargs):
    return fetch("lof_nav", akshare=akshare, symbol=fund_code, **kwargs)


def fetch_fund_open_daily(akshare, **kwargs):
    return fetch("fund_open_daily", akshare=akshare, **kwargs)


def fetch_fund_open_nav(akshare, fund_code, start_date=None, end_date=None, **kwargs):
    return fetch(
        "fund_open_nav",
        akshare=akshare,
        symbol=fund_code,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_fund_open_info(akshare, fund_code, **kwargs):
    return fetch("fund_open_info", akshare=akshare, symbol=fund_code, **kwargs)


def fetch_sector_fund_flow(akshare, date=None, sector_type="industry", **kwargs):
    return fetch(
        "sector_fund_flow",
        akshare=akshare,
        date=date,
        sector_type=sector_type,
        **kwargs,
    )


def fetch_main_fund_flow_rank(akshare, start_date=None, end_date=None, **kwargs):
    return fetch(
        "main_fund_flow_rank",
        akshare=akshare,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def fetch_industry_stocks(akshare, industry_code, level=1, **kwargs):
    return fetch("industry_stocks", akshare=akshare, symbol=industry_code, **kwargs)


def fetch_industry_mapping(akshare, symbol, level=1, **kwargs):
    return fetch("industry_mapping", akshare=akshare, symbol=symbol, **kwargs)


def fetch_industry_performance(akshare, date=None, **kwargs):
    return fetch("industry_performance", akshare=akshare, date=date, **kwargs)


def fetch_concept_list(akshare, **kwargs):
    return fetch("concept_list", akshare=akshare, **kwargs)


def fetch_concept_stocks(akshare, concept_code, **kwargs):
    return fetch("concept_stocks", akshare=akshare, symbol=concept_code, **kwargs)


def fetch_stock_concepts(akshare, symbol, **kwargs):
    return fetch("stock_concepts", akshare=akshare, symbol=symbol, **kwargs)


def fetch_concept_performance(akshare, date=None, **kwargs):
    return fetch("concept_performance", akshare=akshare, date=date, **kwargs)


def fetch_stock_industry(akshare, symbol, **kwargs):
    return fetch("stock_industry", akshare=akshare, symbol=symbol, **kwargs)


def fetch_hot_rank(akshare, **kwargs):
    return fetch("hot_rank", akshare=akshare, **kwargs)


def fetch_sw_industry_list(akshare, **kwargs):
    return fetch("sw_industry_list", akshare=akshare, **kwargs)


def fetch_sw_industry_daily(akshare, industry_code, **kwargs):
    return fetch("sw_industry_daily", akshare=akshare, symbol=industry_code, **kwargs)


def fetch_convert_bond_list(akshare, **kwargs):
    return fetch("convert_bond_premium", akshare=akshare, **kwargs)


def fetch_convert_bond_info(akshare, symbol, **kwargs):
    return fetch("convert_bond_spot", akshare=akshare, **kwargs)


def fetch_futures_realtime_data(akshare, symbol, exchange=None, **kwargs):
    return fetch(
        "futures_realtime", akshare=akshare, symbol=symbol, exchange=exchange, **kwargs
    )


def fetch_futures_main_contracts(akshare, exchange=None, **kwargs):
    return fetch("futures_main_contracts", akshare=akshare, exchange=exchange, **kwargs)


def fetch_news_data(akshare, symbol=None, date=None, **kwargs):
    return fetch(
        "disclosure_news", akshare=akshare, symbol=symbol or "000001", **kwargs
    )


# ── 计算函数（委托给 core.options） ─────────────────────────────────


def calculate_option_greeks(
    spot, strike, time_to_expiry, rate, sigma, option_type, norm=None, np=None
):
    return _calc_greeks(spot, strike, time_to_expiry, rate, sigma, option_type)


def black_scholes_price(S, K, T, r, sigma, option_type, norm=None, np=None):
    return _bs_price(S, K, T, r, sigma, option_type)
