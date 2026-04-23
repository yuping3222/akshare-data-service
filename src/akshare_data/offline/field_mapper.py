"""离线字段映射分析器 - 自动发现并映射 AkShare 接口字段

通过实际调用 AkShare 接口，分析返回数据的列名，自动匹配中英文字段映射。

用法:
    python -m akshare_data.offline.field_mapper                    # 全量分析
    python -m akshare_data.offline.field_mapper --sample-size 50   # 只分析前50个
    python -m akshare_data.offline.field_mapper --category equity  # 只分析指定分类
    python -m akshare_data.offline.field_mapper --merge            # 分析后合并到注册表
    python -m akshare_data.offline.field_mapper --report-only      # 仅生成报告，不调用接口
"""

from __future__ import annotations

import csv
import json
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger(__name__)

# ── 路径常量 ──────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
REGISTRY_FILE = PROJECT_ROOT / "config" / "akshare_registry.yaml"
OUTPUT_DIR = PROJECT_ROOT / "config" / "field_mappings"
REPORT_FILE = OUTPUT_DIR / "field_mapping_report.md"
UNMAPPED_FILE = OUTPUT_DIR / "unmapped_columns.csv"
MAPPING_RESULT_FILE = OUTPUT_DIR / "field_mappings.json"

# ── 扩充的中文→英文字段映射 ──────────────────────────────────────────

# 在 fields.py 的 CN_TO_EN 基础上，补充更多 AkShare 常见字段
EXTENDED_CN_TO_EN = {
    # === 日期时间类 ===
    "日期": "date",
    "时间": "time",
    "datetime": "datetime",
    "trade_date": "date",
    "report_date": "report_date",
    "update_date": "update_date",
    "announce_date": "announce_date",
    "announcement_date": "announcement_date",
    "ex_date": "ex_date",
    "record_date": "record_date",
    "pay_date": "pay_date",
    "list_date": "list_date",
    "delist_date": "delist_date",
    "change_date": "change_date",
    "pledge_date": "pledge_date",
    "release_date": "release_date",
    "transaction_date": "transaction_date",
    "rating_date": "rating_date",
    "status_date": "status_date",
    "nav_date": "nav_date",
    "start_date": "start_date",
    "end_date": "end_date",
    "year": "year",
    "month": "month",
    "quarter": "quarter",
    "到期日": "expiration_date",
    "公告日期": "announce_date",
    "更新日期": "update_date",
    "报告期": "report_date",
    "交易日期": "date",
    "发布日期": "publish_date",
    "成立时间": "establish_date",
    "上市日期": "list_date",
    "摘牌日期": "delist_date",
    "除权除息日": "ex_date",
    "股权登记日": "record_date",
    "红利发放日": "pay_date",
    "质押日期": "pledge_date",
    "解禁日期": "release_date",
    "成交日期": "transaction_date",
    "评级日期": "rating_date",
    "净值日期": "nav_date",
    # === OHLCV 行情类 ===
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude",
    "涨跌幅": "pct_change",
    "涨跌额": "change",
    "换手率": "turnover_rate",
    "涨停价": "limit_up",
    "跌停价": "limit_down",
    "昨收": "pre_close",
    "今开": "open",
    "最高价": "high",
    "最低价": "low",
    "收盘价": "close",
    "开盘价": "open",
    "成交量(手)": "volume",
    "成交额(元)": "amount",
    "成交量(万股)": "volume",
    "成交额(万元)": "amount",
    "均价": "avg_price",
    "平均价": "avg_price",
    "前收": "pre_close",
    "前收盘": "pre_close",
    "昨结": "pre_settle",
    "结算价": "settle",
    "最新价": "close",
    "当前价": "price",
    "价格": "price",
    "买入价": "bid",
    "卖出价": "ask",
    "买价": "bid",
    "卖价": "ask",
    "买量": "bid_volume",
    "卖量": "ask_volume",
    # === 代码名称类 ===
    "代码": "symbol",
    "名称": "name",
    "股票代码": "symbol",
    "证券代码": "symbol",
    "品种代码": "symbol",
    "成分券代码": "symbol",
    "成分股代码": "symbol",
    "基金代码": "symbol",
    "指数代码": "index_code",
    "行业代码": "industry_code",
    "概念代码": "concept_code",
    "债券代码": "bond_code",
    "期权代码": "option_code",
    "标的代码": "underlying_code",
    "成分券名称": "stock_name",
    "成分股名称": "stock_name",
    "品种名称": "name",
    "基金名称": "name",
    "指数名称": "index_name",
    "行业名称": "industry_name",
    "概念名称": "concept_name",
    "债券名称": "bond_name",
    "期权名称": "option_name",
    "股票名称": "stock_name",
    "证券名称": "name",
    "简称": "short_name",
    "全称": "full_name",
    "公司全称": "company_name",
    "公司名称": "company_name",
    "股东名称": "holder_name",
    "股东": "holder_name",
    "高管姓名": "name",
    "姓名": "name",
    "分析师": "analyst",
    "券商": "broker",
    "机构名称": "institution_name",
    "评级机构": "rating_agency",
    "质权人": "pledgee",
    "受让人": "transferee",
    "出让方": "transferor",
    # === 财务指标类 ===
    "总市值": "total_market_cap",
    "流通市值": "circulating_market_cap",
    "市值": "market_cap",
    "市盈率": "pe_ratio",
    "市净率": "pb_ratio",
    "市销率": "ps_ratio",
    "市现率": "pcf_ratio",
    "市盈率(动)": "pe_dynamic",
    "市盈率(静)": "pe_static",
    "市盈率TTM": "pe_ttm",
    "市净率MRQ": "pb_mrq",
    "净资产收益率": "roe",
    "总资产收益率": "roa",
    "毛利率": "gross_margin",
    "净利率": "net_margin",
    "净利润": "net_profit",
    "净利润(元)": "net_profit",
    "归母净利润": "net_profit_attributable",
    "扣非净利润": "net_profit_deducted",
    "营业收入": "revenue",
    "营业收入(元)": "revenue",
    "营业总收入": "total_revenue",
    "营业支出": "operating_expense",
    "营业成本": "operating_cost",
    "营业利润": "operating_profit",
    "利润总额": "total_profit",
    "所得税": "income_tax",
    "净利润率": "net_profit_margin",
    "每股收益": "eps",
    "每股净资产": "bps",
    "每股现金流": "cps",
    "每股经营现金流": "operating_cash_flow_per_share",
    "总股本": "total_shares",
    "流通股本": "circulating_shares",
    "A股总股本": "a_shares",
    "流通股": "circulating_shares",
    "限售股": "restricted_shares",
    "自由流通股本": "free_float_shares",
    "总资产": "total_assets",
    "总负债": "total_liabilities",
    "净资产": "net_assets",
    "股东权益": "equity",
    "资产负债率": "debt_to_asset_ratio",
    "流动比率": "current_ratio",
    "速动比率": "quick_ratio",
    "现金流": "cash_flow",
    "经营现金流": "operating_cash_flow",
    "投资现金流": "investing_cash_flow",
    "筹资现金流": "financing_cash_flow",
    "自由现金流": "free_cash_flow",
    "现金净增加额": "net_cash_increase",
    "期末现金余额": "cash_balance",
    "资本公积": "capital_reserve",
    "盈余公积": "surplus_reserve",
    "未分配利润": "retained_earnings",
    "商誉": "goodwill",
    "商誉减值": "goodwill_impairment",
    "商誉占净资产比例": "goodwill_to_net_assets",
    "研发投入": "rd_expense",
    "研发占比": "rd_to_revenue",
    # === 资金流向类 ===
    "主力净流入": "main_net_inflow",
    "超大单净流入": "super_large_net_inflow",
    "大单净流入": "large_net_inflow",
    "中单净流入": "medium_net_inflow",
    "小单净流入": "small_net_inflow",
    "主力净流入占比": "main_net_inflow_ratio",
    "净流入": "net_inflow",
    "净流入额": "net_inflow",
    "净流入(万元)": "net_inflow",
    "流入": "inflow",
    "流出": "outflow",
    "资金流向": "fund_flow",
    "北向资金净流入": "northbound_net_inflow",
    "沪股通净流入": "shhk_net_inflow",
    "深股通净流入": "szhk_net_inflow",
    "持股数量": "hold_count",
    "持股比例": "hold_ratio",
    "持股市值": "hold_market_cap",
    "持股占流通股比": "hold_to_circulating_ratio",
    "增减持": "change_direction",
    "变动数量": "change_count",
    "变动比例": "change_ratio",
    "变动占流通股比例": "change_to_circulating_ratio",
    # === 板块行业类 ===
    "行业": "industry",
    "所属行业": "industry",
    "行业排名": "industry_rank",
    "概念": "concept",
    "所属概念": "concepts",
    "地域": "region",
    "地区": "region",
    "省份": "province",
    "城市": "city",
    "板块": "sector",
    "板块名称": "sector_name",
    "板块类型": "sector_type",
    "股票数量": "stock_count",
    "公司数量": "company_count",
    "领涨股票": "leading_stock",
    "领涨股票涨跌幅": "leading_stock_change",
    "换手": "turnover_rate",
    "家数": "count",
    "上涨家数": "up_count",
    "下跌家数": "down_count",
    "平盘家数": "flat_count",
    # === 交易状态类 ===
    "交易状态": "trade_status",
    "状态": "status",
    "停牌": "suspended",
    "复牌": "resumed",
    "ST": "is_st",
    "退市": "is_delisted",
    "上市": "is_listed",
    "是否停牌": "is_suspended",
    # === 复权/调整类 ===
    "复权类型": "adjust_type",
    "复权": "adjust",
    "前复权": "qfq",
    "后复权": "hfq",
    "不复权": "none",
    # === 期货期权类 ===
    "持仓量": "open_interest",
    "昨持仓": "pre_open_interest",
    "持仓变化": "oi_change",
    "行权价": "strike_price",
    "执行价": "strike_price",
    "期权类型": "option_type",
    "认购": "call",
    "认沽": "put",
    "隐含波动率": "implied_volatility",
    "历史波动率": "historical_volatility",
    "Delta": "delta",
    "Gamma": "gamma",
    "Theta": "theta",
    "Vega": "vega",
    "Rho": "rho",
    "内在价值": "intrinsic_value",
    "时间价值": "time_value",
    "理论价格": "theoretical_price",
    "溢价率": "premium_rate",
    "杠杆比率": "leverage_ratio",
    "交割月": "delivery_month",
    "合约月份": "contract_month",
    "最小变动价位": "min_price_tick",
    "交易单位": "contract_size",
    "涨跌停板幅度": "limit_range",
    "保证金比例": "margin_ratio",
    "仓单数量": "warrant_count",
    "注册仓单": "registered_warrants",
    "有效仓单": "valid_warrants",
    # === 债券类 ===
    "转债代码": "bond_code",
    "转债名称": "bond_name",
    "正股代码": "stock_code",
    "正股名称": "stock_name",
    "转债价格": "bond_price",
    "转股价格": "conversion_price",
    "转股价值": "conversion_value",
    "转股溢价率": "conversion_premium_rate",
    "纯债价值": "pure_bond_value",
    "纯债溢价率": "pure_bond_premium_rate",
    "到期收益率": "ytm",
    "票面利率": "coupon_rate",
    "剩余期限": "remaining_term",
    "剩余年限": "remaining_years",
    "回售触发价": "put_trigger_price",
    "强赎触发价": "call_trigger_price",
    "转股价修正": "conversion_price_adjust",
    "信用评级": "credit_rating",
    "评级展望": "rating_outlook",
    "担保": "guarantee",
    "发行规模": "issue_size",
    "剩余规模": "remaining_size",
    "申购代码": "subscription_code",
    "中签号": "lottery_number",
    "中签率": "lottery_rate",
    # === 宏观数据类 ===
    "指标": "indicator",
    "数值": "value",
    "同比增长": "yoy_change",
    "环比增长": "mom_change",
    "同比": "yoy",
    "环比": "mom",
    "累计值": "cumulative_value",
    "累计同比": "cumulative_yoy",
    "当月值": "monthly_value",
    "预测值": "forecast_value",
    "前值": "previous_value",
    "公布值": "actual_value",
    "重要度": "importance",
    "CPI": "cpi",
    "PPI": "ppi",
    "GDP": "gdp",
    "PMI": "pmi",
    "M2": "m2_supply",
    "M1": "m1_supply",
    "M0": "m0_supply",
    "社会融资规模": "social_financing",
    "新增人民币贷款": "new_loans",
    "外汇储备": "forex_reserve",
    "利率": "interest_rate",
    "汇率": "exchange_rate",
    "中间价": "central_parity_rate",
    "美元指数": "dollar_index",
    # === 其他通用类 ===
    "权重": "weight",
    "占比": "ratio",
    "比例": "ratio",
    "百分比": "percentage",
    "涨幅": "change_pct",
    "跌幅": "drop_pct",
    "排名": "rank",
    "序号": "index",
    "编号": "id",
    "ID": "id",
    "类型": "type",
    "分类": "category",
    "来源": "source",
    "备注": "remark",
    "说明": "description",
    "链接": "url",
    "网址": "url",
    "新闻标题": "news_title",
    "新闻内容": "news_content",
    "新闻来源": "news_source",
    "新闻时间": "news_time",
    "标题": "title",
    "内容": "content",
    "摘要": "summary",
    "关键词": "keywords",
    "页数": "page",
    "总页数": "total_pages",
    "总数": "total_count",
    "数量": "count",
    "次数": "times",
    "频次": "frequency",
    "天数": "days",
    "年限": "years",
    "月数": "months",
    "年龄": "age",
    "学历": "education",
    "职务": "title",
    "职位": "position",
    "性别": "gender",
    "民族": "ethnicity",
    "国籍": "nationality",
    "地址": "address",
    "电话": "phone",
    "传真": "fax",
    "邮箱": "email",
    "网站": "website",
    "经营范围": "business_scope",
    "主营业务": "main_business",
    "法人代表": "legal_representative",
    "注册资本": "registered_capital",
    "实收资本": "paid_in_capital",
    "员工人数": "employee_count",
    "办公地址": "office_address",
    "注册地址": "registered_address",
    "邮编": "zip_code",
    "董秘": "board_secretary",
    "董秘电话": "board_secretary_phone",
    "董秘邮箱": "board_secretary_email",
    "会计师事务所": "accounting_firm",
    "律师事务所": "law_firm",
    "保荐机构": "sponsor",
    "主承销商": "lead_underwriter",
    "中签缴款日": "payment_date",
    "网上发行数量": "online_issue_count",
    "网下发行数量": "offline_issue_count",
    "发行价格": "issue_price",
    "发行市盈率": "issue_pe",
    "行业市盈率": "industry_pe",
    "募资总额": "total_raised",
    "募资净额": "net_raised",
    "发行费用": "issue_expense",
    "申购上限": "subscription_limit",
    "申购下限": "subscription_min",
    "顶格申购需配市值": "max_subscription_market_cap",
    "是否盈利": "is_profitable",
    "盈利": "profit",
    "亏损": "loss",
    "分红": "dividend",
    "送股": "stock_dividend",
    "转增": "capitalization_reserve",
    "派息": "cash_dividend",
    "每10股派": "dividend_per_10",
    "每10股送": "stock_dividend_per_10",
    "每10股转": "capitalization_per_10",
    "除权参考价": "ex_reference_price",
    "股权登记": "record_date",
    "除权除息": "ex_date",
    "红股上市": "bonus_list_date",
    "事件": "event",
    "原因": "reason",
    "进展": "progress",
    "结果": "result",
    "影响": "impact",
    "风险": "risk",
    "机会": "opportunity",
    "优势": "strength",
    "劣势": "weakness",
    "评分": "score",
    "得分": "score",
    "等级": "grade",
    "评级": "rating",
    "目标价": "target_price",
    "一致预期": "consensus",
    "买入": "buy_count",
    "增持": "overweight_count",
    "持有": "hold_count",
    "减持": "underweight_count",
    "卖出": "sell_count",
    "研报数量": "research_count",
    "关注度": "attention",
    "人气": "popularity",
    "热度": "heat",
    "热度排名": "heat_rank",
    "浏览": "views",
    "评论": "comments",
    "点赞": "likes",
    "收藏": "favorites",
    "分享": "shares",
    "转发": "forwards",
    "曝光量": "exposure",
    "点击量": "clicks",
    "转化率": "conversion_rate",
    "留存率": "retention_rate",
    "活跃用户": "active_users",
    "日活": "dau",
    "月活": "mau",
}


# ── 数据类 ──────────────────────────────────────────────────────────


@dataclass
class ColumnInfo:
    """单个列的映射信息"""

    original_name: str
    mapped_name: Optional[str] = None
    is_mapped: bool = False
    dtype: str = ""
    sample_value: str = ""


@dataclass
class InterfaceFieldResult:
    """单个接口的字段分析结果"""

    interface_name: str
    status: str = ""  # success / failed / skipped / empty
    error_msg: str = ""
    total_columns: int = 0
    mapped_columns: int = 0
    unmapped_columns: int = 0
    columns: List[Dict[str, Any]] = field(default_factory=list)
    output_mapping: Dict[str, str] = field(default_factory=dict)
    row_count: int = 0
    exec_time: float = 0.0


# ── 字段映射分析器 ───────────────────────────────────────────────────


class FieldMapper:
    """离线字段映射分析器

    通过实际调用 AkShare 接口，分析返回数据的列名，
    自动匹配中英文字段映射，并生成映射报告。
    """

    def __init__(
        self,
        registry_path: Optional[Path] = None,
        output_dir: Optional[Path] = None,
    ):
        self.registry_path = registry_path or REGISTRY_FILE
        self.output_dir = output_dir or OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.registry: Dict[str, Any] = {}
        self.results: List[InterfaceFieldResult] = []
        self.global_column_stats: Dict[str, int] = defaultdict(int)
        self.global_unmapped: Dict[str, List[str]] = defaultdict(list)

        # 加载 akshare
        self.ak = None
        self._load_akshare()

    def _load_akshare(self):
        """加载 akshare 模块"""
        try:
            import akshare as ak

            self.ak = ak
            logger.info(f"AkShare version: {ak.__version__}")
        except ImportError:
            logger.error("AkShare 未安装，无法进行字段分析")
            raise

    def load_registry(self) -> Dict[str, Any]:
        """加载注册表"""
        if not self.registry_path.exists():
            logger.error(f"注册表不存在: {self.registry_path}")
            raise FileNotFoundError(f"Registry not found: {self.registry_path}")

        with open(self.registry_path, "r", encoding="utf-8") as f:
            self.registry = yaml.safe_load(f) or {}

        logger.info(
            f"Loaded registry: {len(self.registry.get('interfaces', {}))} interfaces"
        )
        return self.registry

    def get_interfaces(
        self,
        category: Optional[str] = None,
        sample_size: Optional[int] = None,
    ) -> List[Tuple[str, Dict]]:
        """获取待分析的接口列表"""
        interfaces = self.registry.get("interfaces", {})

        # 按分类过滤
        if category:
            interfaces = {
                name: iface
                for name, iface in interfaces.items()
                if iface.get("category") == category
            }
            logger.info(
                f"Filtered by category '{category}': {len(interfaces)} interfaces"
            )

        # 限制数量
        if sample_size:
            items = list(interfaces.items())[:sample_size]
            logger.info(f"Limited to {sample_size} interfaces")
        else:
            items = list(interfaces.items())

        return items

    def _call_interface(self, func_name: str, probe_params: Dict) -> Tuple[Any, str]:
        """调用单个接口获取样本数据"""
        if self.ak is None:
            return None, "AkShare not loaded"

        func = getattr(self.ak, func_name, None)
        if func is None:
            return None, f"Function {func_name} not found"

        try:
            result = func(**probe_params)
            return result, ""
        except Exception as e:
            error_msg = str(e).strip().replace("\n", " ")[:200]
            return None, error_msg

    def _analyze_columns(self, df) -> List[ColumnInfo]:
        """分析 DataFrame 的列"""
        import pandas as pd

        columns = []
        for col in df.columns:
            col_info = ColumnInfo(
                original_name=str(col),
                dtype=str(df[col].dtype),
            )

            # 获取样本值
            if len(df) > 0:
                val = df[col].iloc[0]
                col_info.sample_value = str(val)[:100] if pd.notna(val) else ""

            # 尝试匹配
            mapped = EXTENDED_CN_TO_EN.get(col)
            if mapped:
                col_info.mapped_name = mapped
                col_info.is_mapped = True
            elif col.islower() and re.match(r"^[a-z][a-z0-9_]*$", col):
                # 已经是英文小写下划线格式，保持原样
                col_info.mapped_name = col
                col_info.is_mapped = True
            else:
                col_info.is_mapped = False

            columns.append(col_info)

        return columns

    def analyze_interface(
        self, func_name: str, iface_def: Dict
    ) -> InterfaceFieldResult:
        """分析单个接口的字段"""
        result = InterfaceFieldResult(interface_name=func_name)

        start_time = time.time()

        # 获取探测参数
        probe = iface_def.get("probe", {})
        probe_params = probe.get("params", {})

        # 调用接口
        data, error = self._call_interface(func_name, probe_params)
        result.exec_time = time.time() - start_time

        if error:
            result.status = "failed"
            result.error_msg = error
            return result

        if data is None:
            result.status = "empty"
            return result

        # 转换为 DataFrame
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, (list, dict)):
            try:
                df = pd.DataFrame(data)
            except Exception:
                result.status = "failed"
                result.error_msg = "Cannot convert to DataFrame"
                return result
        else:
            result.status = "failed"
            result.error_msg = f"Unsupported type: {type(data)}"
            return result

        if df.empty:
            result.status = "empty"
            return result

        result.status = "success"
        result.row_count = len(df)

        # 分析列
        columns = self._analyze_columns(df)
        result.total_columns = len(columns)

        for col_info in columns:
            col_dict = {
                "original_name": col_info.original_name,
                "mapped_name": col_info.mapped_name,
                "is_mapped": col_info.is_mapped,
                "dtype": col_info.dtype,
                "sample_value": col_info.sample_value,
            }
            result.columns.append(col_dict)

            if col_info.is_mapped:
                result.mapped_columns += 1
                if col_info.original_name != col_info.mapped_name:
                    result.output_mapping[col_info.original_name] = col_info.mapped_name
                self.global_column_stats[col_info.mapped_name] += 1
            else:
                result.unmapped_columns += 1
                self.global_unmapped[col_info.original_name].append(func_name)

        return result

    def analyze_all(
        self,
        category: Optional[str] = None,
        sample_size: Optional[int] = None,
        skip_existing: bool = False,
    ) -> List[InterfaceFieldResult]:
        """批量分析所有接口"""
        self.load_registry()
        interfaces = self.get_interfaces(category=category, sample_size=sample_size)

        total = len(interfaces)
        logger.info(f"Starting field analysis for {total} interfaces...")

        results = []
        success_count = 0
        failed_count = 0
        skipped_count = 0

        for idx, (func_name, iface_def) in enumerate(interfaces):
            # 跳过已有映射的接口
            if skip_existing:
                sources = iface_def.get("sources", [])
                if sources and any(s.get("output_mapping") for s in sources):
                    skipped_count += 1
                    continue

            logger.info(f"[{idx + 1}/{total}] Analyzing: {func_name}")

            result = self.analyze_interface(func_name, iface_def)
            results.append(result)

            if result.status == "success":
                success_count += 1
                logger.info(
                    f"  -> {result.total_columns} cols, "
                    f"{result.mapped_columns} mapped, "
                    f"{result.unmapped_columns} unmapped "
                    f"({result.exec_time:.1f}s)"
                )
            elif result.status == "empty":
                logger.info("  -> Empty data")
            else:
                failed_count += 1
                logger.warning(f"  -> Failed: {result.error_msg[:80]}")

            # 限速
            time.sleep(0.5)

        logger.info(
            f"\nAnalysis complete: {success_count} success, "
            f"{failed_count} failed, {skipped_count} skipped"
        )

        self.results = results
        return results

    def generate_report(self) -> str:
        """生成 Markdown 报告"""
        lines = [
            "# AkShare 字段映射分析报告",
            f"\n生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"分析接口数: {len(self.results)}",
            "",
        ]

        # 统计
        success = [r for r in self.results if r.status == "success"]
        failed = [r for r in self.results if r.status == "failed"]
        empty = [r for r in self.results if r.status == "empty"]

        total_cols = sum(r.total_columns for r in success)
        total_mapped = sum(r.mapped_columns for r in success)
        total_unmapped = sum(r.unmapped_columns for r in success)

        lines.extend(
            [
                "## 概览",
                "",
                "| 指标 | 数量 |",
                "|------|------|",
                f"| 成功分析 | {len(success)} |",
                f"| 调用失败 | {len(failed)} |",
                f"| 返回空数据 | {len(empty)} |",
                f"| 总列数 | {total_cols} |",
                f"| 已映射列 | {total_mapped} ({total_mapped / max(total_cols, 1) * 100:.1f}%) |",
                f"| 未映射列 | {total_unmapped} ({total_unmapped / max(total_cols, 1) * 100:.1f}%) |",
                "",
            ]
        )

        # 高频字段统计
        if self.global_column_stats:
            lines.extend(
                [
                    "## 高频字段 Top 30",
                    "",
                    "| 字段名 | 出现次数 |",
                    "|--------|----------|",
                ]
            )
            sorted_stats = sorted(
                self.global_column_stats.items(), key=lambda x: x[1], reverse=True
            )[:30]
            for col_name, count in sorted_stats:
                lines.append(f"| {col_name} | {count} |")
            lines.append("")

        # 未映射字段统计
        if self.global_unmapped:
            lines.extend(
                [
                    "## 未映射字段 Top 50",
                    "",
                    "| 原始列名 | 出现接口数 | 示例接口 |",
                    "|----------|-----------|----------|",
                ]
            )
            sorted_unmapped = sorted(
                self.global_unmapped.items(), key=lambda x: len(x[1]), reverse=True
            )[:50]
            for col_name, ifaces in sorted_unmapped:
                example = ifaces[0] if ifaces else ""
                lines.append(f"| {col_name} | {len(ifaces)} | {example} |")
            lines.append("")

        # 各接口详情
        lines.extend(
            [
                "## 各接口字段详情",
                "",
            ]
        )

        for r in success:
            if r.total_columns == 0:
                continue

            lines.append(f"### {r.interface_name}")
            lines.append(f"- 行数: {r.row_count}, 列数: {r.total_columns}")
            lines.append(f"- 已映射: {r.mapped_columns}, 未映射: {r.unmapped_columns}")
            lines.append(f"- 耗时: {r.exec_time:.1f}s")
            lines.append("")

            if r.output_mapping:
                lines.append("**输出映射:**")
                lines.append("```yaml")
                lines.append("output_mapping:")
                for orig, mapped in sorted(r.output_mapping.items()):
                    lines.append(f"  {orig}: {mapped}")
                lines.append("```")
                lines.append("")

            # 未映射列
            unmapped = [c for c in r.columns if not c["is_mapped"]]
            if unmapped:
                lines.append("**未映射列:**")
                lines.append("| 原始列名 | 类型 | 样本值 |")
                lines.append("|----------|------|--------|")
                for c in unmapped[:20]:  # 最多显示20个
                    lines.append(
                        f"| {c['original_name']} | {c['dtype']} | {c['sample_value'][:50]} |"
                    )
                if len(unmapped) > 20:
                    lines.append(f"| ... 还有 {len(unmapped) - 20} 列 | | |")
                lines.append("")

        # 失败的接口
        if failed:
            lines.extend(
                [
                    "## 调用失败的接口",
                    "",
                    "| 接口名 | 错误信息 |",
                    "|--------|----------|",
                ]
            )
            for r in failed:
                lines.append(f"| {r.interface_name} | {r.error_msg[:100]} |")
            lines.append("")

        report_content = "\n".join(lines)

        # 写入文件
        report_path = self.output_dir / "field_mapping_report.md"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        logger.info(f"Report saved to {report_path}")
        return report_content

    def export_unmapped_csv(self):
        """导出未映射字段 CSV"""
        csv_path = self.output_dir / "unmapped_columns.csv"

        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["original_column", "interface_count", "interfaces", "sample_value"]
            )

            sorted_unmapped = sorted(
                self.global_unmapped.items(), key=lambda x: len(x[1]), reverse=True
            )

            for col_name, ifaces in sorted_unmapped:
                # 找样本值
                sample = ""
                for r in self.results:
                    if r.status == "success":
                        for c in r.columns:
                            if c["original_name"] == col_name and c["sample_value"]:
                                sample = c["sample_value"]
                                break
                    if sample:
                        break

                writer.writerow(
                    [
                        col_name,
                        len(ifaces),
                        "; ".join(ifaces[:5]),
                        sample,
                    ]
                )

        logger.info(f"Unmapped columns saved to {csv_path}")

    def export_mappings_json(self):
        """导出映射结果为 JSON"""
        mappings = {}

        for r in self.results:
            if r.status == "success" and r.output_mapping:
                mappings[r.interface_name] = {
                    "output_mapping": r.output_mapping,
                    "total_columns": r.total_columns,
                    "mapped_columns": r.mapped_columns,
                    "unmapped_columns": r.unmapped_columns,
                }

        json_path = self.output_dir / "field_mappings.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(mappings, f, ensure_ascii=False, indent=2)

        logger.info(f"Mappings saved to {json_path}")
        return mappings

    def merge_to_registry(self, mappings: Optional[Dict] = None) -> Path:
        """将映射结果合并回注册表"""
        if mappings is None:
            mappings = self.export_mappings_json()

        # 备份原注册表
        if self.registry_path.exists():
            backup_path = self.registry_path.with_name(
                f"{self.registry_path.stem}_backup_{time.strftime('%Y%m%d_%H%M%S')}{self.registry_path.suffix}"
            )
            import shutil

            shutil.copy2(self.registry_path, backup_path)
            logger.info(f"Backed up registry to {backup_path}")

        # 重新加载
        self.load_registry()

        # 合并
        merged_count = 0
        for func_name, mapping_data in mappings.items():
            if func_name in self.registry.get("interfaces", {}):
                iface = self.registry["interfaces"][func_name]
                output_mapping = mapping_data.get("output_mapping", {})

                if not output_mapping:
                    continue

                # 确保 sources 存在
                if not iface.get("sources"):
                    iface["sources"] = [
                        {
                            "name": "akshare",
                            "func": func_name,
                            "enabled": True,
                        }
                    ]

                # 合并到第一个 source
                source = iface["sources"][0]
                existing_mapping = source.get("output_mapping", {})
                existing_mapping.update(output_mapping)
                source["output_mapping"] = existing_mapping

                merged_count += 1

        # 保存
        with open(self.registry_path, "w", encoding="utf-8") as f:
            yaml.dump(
                self.registry,
                f,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )

        logger.info(f"Merged {merged_count} interface mappings into registry")
        return self.registry_path


# ── 主入口 ───────────────────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AkShare 离线字段映射分析器")
    parser.add_argument("--sample-size", type=int, help="只分析前 N 个接口")
    parser.add_argument("--category", type=str, help="只分析指定分类的接口")
    parser.add_argument(
        "--skip-existing", action="store_true", help="跳过已有映射的接口"
    )
    parser.add_argument("--merge", action="store_true", help="分析后合并到注册表")
    parser.add_argument(
        "--report-only", action="store_true", help="仅生成报告（从已有结果）"
    )
    parser.add_argument("--output-dir", type=str, help="输出目录")
    parser.add_argument("--registry", type=str, help="注册表文件路径")
    args = parser.parse_args()

    mapper = FieldMapper(
        registry_path=Path(args.registry) if args.registry else None,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )

    if args.report_only:
        # 从已有 JSON 加载结果生成报告
        json_path = mapper.output_dir / "field_mappings.json"
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                mappings = json.load(f)

            # 重建 results
            for func_name, data in mappings.items():
                result = InterfaceFieldResult(
                    interface_name=func_name,
                    status="success",
                    total_columns=data.get("total_columns", 0),
                    mapped_columns=data.get("mapped_columns", 0),
                    unmapped_columns=data.get("unmapped_columns", 0),
                    output_mapping=data.get("output_mapping", {}),
                )
                mapper.results.append(result)

            mapper.generate_report()
        else:
            logger.error("No existing mappings found. Run without --report-only first.")
        return

    # 执行分析
    mapper.analyze_all(
        category=args.category,
        sample_size=args.sample_size,
        skip_existing=args.skip_existing,
    )

    # 生成报告
    mapper.generate_report()

    # 导出未映射 CSV
    mapper.export_unmapped_csv()

    # 导出映射 JSON
    mappings = mapper.export_mappings_json()

    # 合并到注册表
    if args.merge:
        mapper.merge_to_registry(mappings)

    # 打印摘要
    success = [r for r in mapper.results if r.status == "success"]
    total_cols = sum(r.total_columns for r in success)
    total_mapped = sum(r.mapped_columns for r in success)

    print("\n" + "=" * 60)
    print("字段映射分析完成")
    print("=" * 60)
    print(f"成功分析接口: {len(success)}")
    print(f"总列数: {total_cols}")
    print(f"已映射: {total_mapped} ({total_mapped / max(total_cols, 1) * 100:.1f}%)")
    print(f"未映射: {total_cols - total_mapped}")
    print("\n输出文件:")
    print(f"  报告: {REPORT_FILE}")
    print(f"  未映射列: {UNMAPPED_FILE}")
    print(f"  映射结果: {MAPPING_RESULT_FILE}")
    if args.merge:
        print(f"  注册表: {REGISTRY_FILE}")


if __name__ == "__main__":
    main()
