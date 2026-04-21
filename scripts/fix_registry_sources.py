"""修复 akshare_registry.yaml 中的 sources 配置以匹配实际的 akshare 函数签名"""

import yaml
from pathlib import Path

REGISTRY_PATH = Path(__file__).parent.parent / "config" / "akshare_registry.yaml"

# 修复后的 sources 配置
FIXED_SOURCES = {
    # 股票实时行情 - stock_zh_a_spot_em() 无参数
    "equity_realtime": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_spot_em",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "最新价": "price",
                    "涨跌幅": "change_pct",
                    "涨跌额": "change",
                    "成交量": "volume",
                    "成交额": "amount",
                    "今开": "open",
                    "最高": "high",
                    "最低": "low",
                    "昨收": "close",
                },
                "column_types": {
                    "price": "float",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "close": "float",
                    "volume": "float",
                    "amount": "float",
                },
            }
        ]
    },
    # 分钟线 - 参数正确，但需要确认 period 映射
    "equity_minute": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_hist_min_em",
                "input_mapping": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                    "period": "period",
                },
                "output_mapping": {
                    "时间": "datetime",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                },
                "column_types": {
                    "datetime": "datetime",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "close": "float",
                    "volume": "float",
                    "amount": "float",
                },
            }
        ]
    },
    # 证券信息
    "security_info": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_individual_info_em",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
    # 指数日线 - symbol 格式需要转换
    "index_daily": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_index_daily_em",
                "input_mapping": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
            }
        ]
    },
    # 大宗交易 - 需要 symbol, start_date, end_date
    "block_deal": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_dzjy_mrmx",
                "input_mapping": {
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "成交价": "deal_price",
                    "成交量": "volume",
                    "成交额": "amount",
                    "溢价率": "premium_rate",
                },
                "column_types": {
                    "deal_price": "float",
                    "volume": "float",
                    "amount": "float",
                    "premium_rate": "float",
                },
            }
        ]
    },
    # 宏观经济 - 都无参数
    "macro_lpr": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "macro_china_lpr",
                "input_mapping": {},
            }
        ]
    },
    "macro_pmi": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "macro_china_pmi",
                "input_mapping": {},
            }
        ]
    },
    "macro_cpi": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "macro_china_cpi",
                "input_mapping": {},
            }
        ]
    },
    "macro_ppi": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "macro_china_ppi",
                "input_mapping": {},
            }
        ]
    },
    "macro_m2": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "macro_china_money_supply",
                "input_mapping": {},
            }
        ]
    },
    # 期货日线 - 只需要 symbol
    "futures_daily": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "futures_zh_daily_sina",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
    # 期货实时 - symbol, market (不是 exchange)
    "futures_realtime": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "futures_zh_spot",
                "input_mapping": {
                    "symbol": "symbol",
                    "exchange": "market",
                },
            }
        ]
    },
    # 期货主力合约 - symbol, start_date, end_date
    "futures_main_contracts": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "futures_main_sina",
                "input_mapping": {},
            }
        ]
    },
    # 龙虎榜 - start_date, end_date
    "dragon_tiger_list": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_lhb_detail_em",
                "input_mapping": {
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
            }
        ]
    },
    "dragon_tiger_summary": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_lhb_detail_em",
                "input_mapping": {
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
            }
        ]
    },
    # 停牌股票 - 无参数
    "suspended_stocks": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_stop_em",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "code",
                    "名称": "display_name",
                },
            }
        ]
    },
    # 可转债实时 - 无参数
    "convert_bond_spot": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "bond_cov_comparison",
                "input_mapping": {},
            }
        ]
    },
    # 融资融券 - 只需要 date
    "margin_data": {
        "sources": [
            {
                "name": "sse",
                "enabled": True,
                "func": "stock_margin_detail_sse",
                "input_mapping": {
                    "date": "date",
                },
                "param_transforms": {
                    "date": "YYYYMMDD",
                },
            }
        ]
    },
    # 北向资金 - symbol 如 '北向资金'
    "north_money_flow": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_hsgt_hist_em",
                "input_mapping": {},
            }
        ]
    },
    # 基金净值 - symbol, indicator, period (无日期)
    "fund_net_value": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "fund_open_fund_info_em",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
    # 基金经理 - 无参数
    "fund_manager_info": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "fund_manager_em",
                "input_mapping": {},
            }
        ]
    },
    # 板块资金流 - indicator, sector_type
    "sector_fund_flow": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_sector_fund_flow_rank",
                "input_mapping": {
                    "sector_type": "sector_type",
                },
            }
        ]
    },
    # 主力资金排行 - indicator
    "main_fund_flow_rank": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_individual_fund_flow_rank",
                "input_mapping": {},
            }
        ]
    },
    # 资金流向 - stock, market
    "money_flow": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_individual_fund_flow",
                "input_mapping": {
                    "symbol": "stock",
                },
            }
        ]
    },
    # 行业映射 - 无参数
    "industry_mapping": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_board_industry_name_em",
                "input_mapping": {},
            }
        ]
    },
    # 行业成分股 - symbol 是行业名称
    "industry_stocks": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_board_industry_cons_em",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
    # 财务指标 - symbol, indicator
    "finance_indicator": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_financial_analysis_indicator_em",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
    # 集合竞价
    "call_auction": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_tick_tx_js",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
    # 证券列表 - 无参数
    "securities_list": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_spot_em",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "code",
                    "名称": "display_name",
                },
            }
        ]
    },
    # 指数成分股
    "index_components": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "index_stock_cons_csindex",
                "input_mapping": {
                    "symbol": "symbol",
                },
            }
        ]
    },
}


def update_registry():
    """更新 registry 文件"""
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        registry = yaml.safe_load(f)

    interfaces = registry.get("interfaces", {})
    updated_count = 0

    for interface_name, new_config in FIXED_SOURCES.items():
        if interface_name in interfaces:
            iface = interfaces[interface_name]
            # 只更新 sources，保留其他字段
            if "sources" in new_config:
                iface["sources"] = new_config["sources"]
            updated_count += 1
            print(f"Updated: {interface_name}")

    # 保存
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        yaml.dump(
            registry, f, allow_unicode=True, default_flow_style=False, sort_keys=False
        )

    print(f"\nTotal updated: {updated_count}")


if __name__ == "__main__":
    update_registry()
