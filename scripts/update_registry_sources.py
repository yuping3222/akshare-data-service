"""为 akshare_registry.yaml 中的接口添加 sources 配置"""

import yaml
from pathlib import Path

REGISTRY_PATH = Path(__file__).parent.parent / "config" / "akshare_registry.yaml"

# 接口 -> sources 配置映射
INTERFACE_SOURCES = {
    # 股票日线
    "equity_daily": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_hist",
                "input_mapping": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                    "adjust": "adjust",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
                "output_mapping": {
                    "日期": "date",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                },
                "column_types": {
                    "date": "datetime",
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
    # 股票分钟线
    "equity_minute": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_hist_min_em",
                "input_mapping": {
                    "symbol": "symbol",
                    "period": "period",
                    "start_date": "start_date",
                    "end_date": "end_date",
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
    # 股票实时行情
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
    # 指数日线
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
                "output_mapping": {
                    "date": "date",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "volume",
                },
                "column_types": {
                    "date": "datetime",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "close": "float",
                    "volume": "float",
                },
            }
        ]
    },
    # ETF日线
    "etf_daily": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "fund_etf_hist_em",
                "input_mapping": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
                "output_mapping": {
                    "日期": "date",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                },
                "column_types": {
                    "date": "datetime",
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
    # 交易日
    "trading_days": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "tool_trade_date_hist_sina",
                "input_mapping": {},
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
                "output_mapping": {
                    "成分券代码": "code",
                    "成分券名称": "name",
                    "权重": "weight",
                },
            }
        ]
    },
    # ST股票
    "st_stocks": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zh_a_st_em",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "code",
                    "名称": "display_name",
                },
            }
        ]
    },
    # 停牌股票
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
    # 大宗交易
    "block_deal": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_dzjy_mrmx",
                "input_mapping": {
                    "date": "date",
                },
                "param_transforms": {
                    "date": "YYYYMMDD",
                },
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "成交价": "deal_price",
                    "成交量": "volume",
                    "成交额": "amount",
                    "溢价率": "premium_rate",
                    "买方营业部": "buyer",
                    "卖方营业部": "seller",
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
    # 融资融券
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
                "output_mapping": {
                    "证券代码": "code",
                    "证券简称": "name",
                    "融资买入额": "margin_buy",
                    "融资余额": "margin_balance",
                    "融券卖出量": "short_sell",
                    "融券余量": "short_balance",
                },
                "column_types": {
                    "margin_buy": "float",
                    "margin_balance": "float",
                    "short_sell": "float",
                    "short_balance": "float",
                },
            }
        ]
    },
    # 龙虎榜
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
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "买入额": "buy_amount",
                    "卖出额": "sell_amount",
                    "净额": "net_amount",
                },
                "column_types": {
                    "buy_amount": "float",
                    "sell_amount": "float",
                    "net_amount": "float",
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
    # 涨停池
    "limit_up_pool": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zt_pool_em",
                "input_mapping": {
                    "date": "date",
                },
                "param_transforms": {
                    "date": "YYYYMMDD",
                },
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "涨跌幅": "change_pct",
                    "最新价": "price",
                    "涨停价": "limit_up_price",
                    "连板数": "continuous_limit_up",
                    "所属行业": "industry",
                },
                "column_types": {
                    "price": "float",
                    "change_pct": "float",
                    "limit_up_price": "float",
                    "continuous_limit_up": "int",
                },
            }
        ]
    },
    # 跌停池
    "limit_down_pool": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_zt_pool_dtgc_em",
                "input_mapping": {
                    "date": "date",
                },
                "param_transforms": {
                    "date": "YYYYMMDD",
                },
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "涨跌幅": "change_pct",
                    "最新价": "price",
                    "跌停价": "limit_down_price",
                },
                "column_types": {
                    "price": "float",
                    "change_pct": "float",
                    "limit_down_price": "float",
                },
            }
        ]
    },
    # 宏观经济
    "macro_lpr": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "macro_china_lpr",
                "input_mapping": {},
                "output_mapping": {
                    "日期": "date",
                    "1年": "lpr_1y",
                    "5年": "lpr_5y",
                },
                "column_types": {
                    "date": "datetime",
                    "lpr_1y": "float",
                    "lpr_5y": "float",
                },
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
                "output_mapping": {
                    "日期": "date",
                    "制造业": "manufacturing_pmi",
                    "非制造业": "non_manufacturing_pmi",
                },
                "column_types": {
                    "date": "datetime",
                    "manufacturing_pmi": "float",
                    "non_manufacturing_pmi": "float",
                },
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
                "output_mapping": {
                    "日期": "date",
                    "同比": "cpi_yoy",
                    "环比": "cpi_mom",
                },
                "column_types": {
                    "date": "datetime",
                    "cpi_yoy": "float",
                    "cpi_mom": "float",
                },
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
                "output_mapping": {
                    "日期": "date",
                    "同比": "ppi_yoy",
                },
                "column_types": {
                    "date": "datetime",
                    "ppi_yoy": "float",
                },
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
                "output_mapping": {
                    "日期": "date",
                    "M2": "m2",
                    "M1": "m1",
                    "M0": "m0",
                },
                "column_types": {
                    "date": "datetime",
                    "m2": "float",
                    "m1": "float",
                    "m0": "float",
                },
            }
        ]
    },
    # 财务指标
    "finance_indicator": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_financial_analysis_indicator_em",
                "input_mapping": {
                    "symbol": "symbol",
                },
                "output_mapping": {
                    "报告期": "report_date",
                    "每股收益": "eps",
                    "净资产收益率": "roe",
                    "净利润": "net_profit",
                    "营业收入": "revenue",
                },
                "column_types": {
                    "report_date": "datetime",
                    "eps": "float",
                    "roe": "float",
                    "net_profit": "float",
                    "revenue": "float",
                },
            }
        ]
    },
    # 资金流向
    "money_flow": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_individual_fund_flow",
                "input_mapping": {
                    "symbol": "symbol",
                },
                "output_mapping": {
                    "日期": "date",
                    "主力净流入": "main_net_inflow",
                    "超大单净流入": "super_large_net_inflow",
                    "大单净流入": "large_net_inflow",
                    "中单净流入": "medium_net_inflow",
                    "小单净流入": "small_net_inflow",
                },
                "column_types": {
                    "date": "datetime",
                    "main_net_inflow": "float",
                    "super_large_net_inflow": "float",
                    "large_net_inflow": "float",
                    "medium_net_inflow": "float",
                    "small_net_inflow": "float",
                },
            }
        ]
    },
    # 北向资金
    "north_money_flow": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_hsgt_hist_em",
                "input_mapping": {
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "param_transforms": {
                    "start_date": "YYYYMMDD",
                    "end_date": "YYYYMMDD",
                },
                "output_mapping": {
                    "日期": "date",
                    "北向资金净流入": "net_flow",
                    "北向资金买入": "buy_amount",
                    "北向资金卖出": "sell_amount",
                },
                "column_types": {
                    "date": "datetime",
                    "net_flow": "float",
                    "buy_amount": "float",
                    "sell_amount": "float",
                },
            }
        ]
    },
    # 证券列表
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
                "output_mapping": {
                    "代码": "code",
                    "名称": "display_name",
                    "行业": "industry",
                    "上市时间": "start_date",
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
                "func": "stock_zh_a_tick_163",
                "input_mapping": {
                    "symbol": "symbol",
                    "date": "date",
                },
                "output_mapping": {
                    "时间": "datetime",
                    "价格": "price",
                    "成交量": "volume",
                },
                "column_types": {
                    "datetime": "datetime",
                    "price": "float",
                    "volume": "float",
                },
            }
        ]
    },
    # 行业成分股
    "industry_stocks": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_board_industry_cons_em",
                "input_mapping": {
                    "symbol": "symbol",
                },
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                },
            }
        ]
    },
    # 行业映射
    "industry_mapping": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_board_industry_name_em",
                "input_mapping": {},
                "output_mapping": {
                    "板块名称": "industry_name",
                    "板块代码": "industry_code",
                },
            }
        ]
    },
    # 基金净值
    "fund_net_value": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "fund_open_fund_info_em",
                "input_mapping": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
                "output_mapping": {
                    "净值日期": "date",
                    "单位净值": "nav",
                    "累计净值": "accumulated_nav",
                },
                "column_types": {
                    "date": "datetime",
                    "nav": "float",
                    "accumulated_nav": "float",
                },
            }
        ]
    },
    # 基金经理
    "fund_manager_info": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "fund_manager_em",
                "input_mapping": {},
                "output_mapping": {
                    "基金经理代码": "manager_code",
                    "基金经理": "manager_name",
                    "任职基金": "managed_funds",
                },
            }
        ]
    },
    # LOF列表
    "lof_list": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "fund_lof_spot_em",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "最新价": "price",
                },
                "column_types": {
                    "price": "float",
                },
            }
        ]
    },
    # 可转债溢价
    "convert_bond_premium": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "bond_zh_cov_value_analysis",
                "input_mapping": {},
                "output_mapping": {
                    "债券代码": "code",
                    "债券名称": "name",
                    "转债价格": "bond_price",
                    "转股价值": "conversion_value",
                    "转股溢价率": "premium_rate",
                },
                "column_types": {
                    "bond_price": "float",
                    "conversion_value": "float",
                    "premium_rate": "float",
                },
            }
        ]
    },
    # 可转债实时
    "convert_bond_spot": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "bond_cov_comparison",
                "input_mapping": {},
                "output_mapping": {
                    "债券代码": "code",
                    "债券名称": "name",
                    "现价": "bond_price",
                    "正股价": "stock_price",
                    "转股价": "conversion_price",
                    "转股溢价率": "premium_rate",
                },
                "column_types": {
                    "bond_price": "float",
                    "stock_price": "float",
                    "conversion_price": "float",
                    "premium_rate": "float",
                },
            }
        ]
    },
    # 期货日线
    "futures_daily": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "futures_zh_daily_sina",
                "input_mapping": {
                    "symbol": "symbol",
                },
                "output_mapping": {
                    "date": "date",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "volume",
                    "hold": "open_interest",
                },
                "column_types": {
                    "date": "datetime",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "close": "float",
                    "volume": "float",
                    "open_interest": "float",
                },
            }
        ]
    },
    # 期货实时
    "futures_realtime": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "futures_zh_spot",
                "input_mapping": {
                    "symbol": "symbol",
                },
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "最新价": "price",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                    "持仓量": "open_interest",
                },
                "column_types": {
                    "price": "float",
                    "open": "float",
                    "high": "float",
                    "low": "float",
                    "volume": "float",
                    "open_interest": "float",
                },
            }
        ]
    },
    # 期货主力合约
    "futures_main_contracts": {
        "sources": [
            {
                "name": "sina",
                "enabled": True,
                "func": "futures_main_sina",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "symbol",
                    "名称": "name",
                    "交易所": "exchange",
                    "最新价": "price",
                    "成交量": "volume",
                },
                "column_types": {
                    "price": "float",
                    "volume": "float",
                },
            }
        ]
    },
    # 板块资金流
    "sector_fund_flow": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_sector_fund_flow_rank",
                "input_mapping": {
                    "sector_type": "indicator",
                },
                "output_mapping": {
                    "名称": "name",
                    "涨跌幅": "change_pct",
                    "主力净流入": "main_net_inflow",
                    "主力净流入-净额": "main_net_inflow_amount",
                },
                "column_types": {
                    "change_pct": "float",
                    "main_net_inflow": "float",
                    "main_net_inflow_amount": "float",
                },
            }
        ]
    },
    # 主力资金排行
    "main_fund_flow_rank": {
        "sources": [
            {
                "name": "east_money",
                "enabled": True,
                "func": "stock_individual_fund_flow_rank",
                "input_mapping": {},
                "output_mapping": {
                    "代码": "code",
                    "名称": "name",
                    "最新价": "price",
                    "涨跌幅": "change_pct",
                    "主力净流入": "main_net_inflow",
                },
                "column_types": {
                    "price": "float",
                    "change_pct": "float",
                    "main_net_inflow": "float",
                },
            }
        ]
    },
}


def update_registry():
    """更新 registry 文件，添加 sources 配置"""
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        registry = yaml.safe_load(f)

    interfaces = registry.get("interfaces", {})
    updated_count = 0

    for interface_name, sources_config in INTERFACE_SOURCES.items():
        # 查找对应的接口
        if interface_name in interfaces:
            iface = interfaces[interface_name]
            iface.update(sources_config)
            updated_count += 1
            print(f"Updated: {interface_name}")
        else:
            # 如果接口不存在，创建一个新的
            interfaces[interface_name] = {
                "name": interface_name,
                "interface_name": interface_name,
                "category": "equity",
                "description": f"Auto-generated interface: {interface_name}",
                "signature": [],
                "domains": [],
                "rate_limit_key": "default",
                "sources": sources_config.get("sources", []),
                "probe": {"params": {}, "skip": True, "check_interval": 86400},
            }
            updated_count += 1
            print(f"Created: {interface_name}")

    # 保存更新后的 registry
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        yaml.dump(
            registry, f, allow_unicode=True, default_flow_style=False, sort_keys=False
        )

    print(f"\nTotal updated/created: {updated_count}")


if __name__ == "__main__":
    update_registry()
