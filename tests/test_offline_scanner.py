"""tests/test_offline_scanner.py

离线模块扫描器测试: AkShareScanner, DomainExtractor, CategoryInferrer, ParamInferrer

参考 test_offline.py 编写风格
"""

import inspect
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from akshare_data.offline.scanner.akshare_scanner import (
    AkShareScanner,
    SKIP_FUNCTIONS,
)
from akshare_data.offline.scanner.category_inferrer import (
    CategoryInferrer,
    CATEGORY_RULES,
)
from akshare_data.offline.scanner.domain_extractor import DomainExtractor
from akshare_data.offline.scanner.param_inferrer import ParamInferrer


class TestAkShareScanner:
    """测试 AkShareScanner"""

    def test_scanner_init(self):
        """测试扫描器初始化"""
        scanner = AkShareScanner()
        assert scanner is not None

    def test_scan_all(self):
        """测试扫描所有函数"""
        scanner = AkShareScanner()
        with patch("akshare_data.offline.scanner.akshare_scanner.ak") as mock_ak:
            mock_func1 = MagicMock()
            mock_func1.__name__ = "stock_zh_a_hist"
            mock_func1.__module__ = "akshare"
            mock_func1.__doc__ = "获取股票历史数据"

            mock_func2 = MagicMock()
            mock_func2.__name__ = "_private_func"
            mock_func2.__module__ = "akshare"

            mock_ak_funcs = [
                ("stock_zh_a_hist", mock_func1),
                ("_private_func", mock_func2),
                ("__dir__", MagicMock()),
            ]

            def is_func(obj):
                return callable(obj)

            mock_ak.iter = lambda: iter(mock_ak_funcs)
            with patch.object(inspect, "getmembers", return_value=mock_ak_funcs):
                result = scanner.scan_all()
                assert "stock_zh_a_hist" in result

    def test_analyze_function(self):
        """测试分析单个函数"""
        scanner = AkShareScanner()

        def sample_func(symbol: str, limit: int = 10) -> str:
            """Sample function doc"""
            pass

        mock_func = MagicMock()
        mock_func.__name__ = "sample_func"
        mock_func.__module__ = "test_module"
        mock_func.__doc__ = "Sample function doc"

        result = scanner._analyze_function("sample_func", mock_func)
        assert result["name"] == "sample_func"
        assert result["module"] == "test_module"
        assert "signature" in result
        assert "doc" in result

    def test_extract_signature_success(self):
        """测试成功提取函数签名"""
        scanner = AkShareScanner()

        def sample_func(symbol: str, limit: int, date: str = "20240101") -> None:
            pass

        sig = scanner._extract_signature(sample_func)
        assert "symbol" in sig
        assert "limit" in sig
        assert "date" in sig
        assert "self" not in sig

    def test_extract_signature_failure(self):
        """测试签名提取失败"""
        scanner = AkShareScanner()

        class NoSignature:
            pass

        result = scanner._extract_signature(NoSignature())
        assert result == []

    def test_extract_signature_value_error(self):
        """测试签名提取 ValueError"""
        scanner = AkShareScanner()

        mock_func = MagicMock()
        mock_func.__name__ = "bad_func"
        with patch.object(inspect, "signature", side_effect=ValueError("invalid")):
            result = scanner._extract_signature(mock_func)
            assert result == []

    def test_extract_signature_type_error(self):
        """测试签名提取 TypeError"""
        scanner = AkShareScanner()

        mock_func = MagicMock()
        mock_func.__name__ = "bad_func"
        with patch.object(inspect, "signature", side_effect=TypeError("invalid")):
            result = scanner._extract_signature(mock_func)
            assert result == []

    def test_extract_doc_with_docstring(self):
        """测试提取有文档字符串的函数"""
        scanner = AkShareScanner()

        def sample_func():
            """This is a docstring"""
            pass

        result = scanner._extract_doc(sample_func)
        assert "This is a docstring" in result

    def test_extract_doc_without_docstring(self):
        """测试提取无文档字符串的函数"""
        scanner = AkShareScanner()

        def sample_func():
            pass

        result = scanner._extract_doc(sample_func)
        assert result == ""

    def test_scan_all_skips_private_functions(self):
        """测试扫描跳过私有函数"""
        scanner = AkShareScanner()

        mock_func = MagicMock()
        mock_func.__name__ = "_private"
        mock_func.__module__ = "akshare"

        with patch.object(
            inspect,
            "getmembers",
            return_value=[("_private", mock_func)],
        ):
            result = scanner.scan_all()
            assert "_private" not in result

    def test_scan_all_skips_skip_functions(self):
        """测试扫描跳过 SKIP_FUNCTIONS 中的函数"""
        scanner = AkShareScanner()

        for skip_name in SKIP_FUNCTIONS:
            mock_func = MagicMock()
            mock_func.__name__ = skip_name
            mock_func.__module__ = "akshare"

            with patch.object(
                inspect,
                "getmembers",
                return_value=[(skip_name, mock_func)],
            ):
                result = scanner.scan_all()
                assert skip_name not in result

    def test_scan_all_logs_count(self):
        """测试扫描记录函数数量"""
        scanner = AkShareScanner()

        mock_func = MagicMock()
        mock_func.__name__ = "test_func"
        mock_func.__module__ = "akshare"

        with patch.object(
            inspect,
            "getmembers",
            return_value=[("test_func", mock_func)],
        ):
            with patch(
                "akshare_data.offline.scanner.akshare_scanner.logger"
            ) as mock_logger:
                scanner.scan_all()
                mock_logger.info.assert_called()


class TestCategoryInferrer:
    """测试 CategoryInferrer"""

    def test_inferrer_init(self):
        """测试推断器初始化"""
        inferrer = CategoryInferrer()
        assert inferrer is not None

    def test_infer_stock_prefix(self):
        """测试股票前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("stock_zh_a_hist") == "equity"
        assert inferrer.infer("stock_a_indicator") == "equity"

    def test_infer_fund_prefix(self):
        """测试基金前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("fund_analysis") == "fund"
        assert inferrer.infer("fund_nav") == "fund"
        assert inferrer.infer("fund_manager") == "fund"

    def test_infer_etf_prefix(self):
        """测试ETF前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("etf_hist") == "fund"

    def test_infer_index_prefix(self):
        """测试指数前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("index_zh_a_hist") == "index"

    def test_infer_futures_prefix(self):
        """测试期货前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("futures_zh_hist") == "futures"

    def test_infer_option_prefix(self):
        """测试期权前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("option_zhd") == "options"

    def test_infer_bond_prefix(self):
        """测试债券前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("bond_zh_hist") == "bond"
        assert inferrer.infer("convert_30") == "bond"

    def test_infer_macro_prefix(self):
        """测试宏观前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("macro_china_money") == "macro"
        assert inferrer.infer("shibor") == "macro"
        assert inferrer.infer("lpr") == "macro"
        assert inferrer.infer("cpi") == "macro"
        assert inferrer.infer("ppi") == "macro"
        assert inferrer.infer("gdp") == "macro"
        assert inferrer.infer("pmi") == "macro"
        assert inferrer.infer("m2") == "macro"
        assert inferrer.infer("rate") == "macro"
        assert inferrer.infer("exchange") == "macro"
        assert inferrer.infer("social_financing") == "macro"

    def test_infer_sector_prefix(self):
        """测试板块前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("sector_detail") == "sector"
        assert inferrer.infer("industry_sw") == "sector"
        assert inferrer.infer("concept_detail") == "sector"
        assert inferrer.infer("sw_industry") == "sector"

    def test_infer_flow_prefix(self):
        """测试资金流前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("money_flow") == "flow"
        assert inferrer.infer("north_flow") == "flow"

    def test_infer_event_prefix(self):
        """测试事件前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("dragon_capital") == "event"
        assert inferrer.infer("limit_up") == "event"
        assert inferrer.infer("margin_trading") == "event"

    def test_infer_corporate_prefix(self):
        """测试公司前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("pledge_detail") == "corporate"
        assert inferrer.infer("repurchase_detail") == "corporate"
        assert inferrer.infer("insider_trading") == "corporate"
        assert inferrer.infer("esg_rating") == "corporate"
        assert inferrer.infer("performance_forecast") == "corporate"
        assert inferrer.infer("analyst_rating") == "corporate"
        assert inferrer.infer("research_report") == "corporate"
        assert inferrer.infer("shareholder_info") == "corporate"
        assert inferrer.infer("dividend_info") == "corporate"
        assert inferrer.infer("bonus_info") == "corporate"
        assert inferrer.infer("chip_analysis") == "corporate"
        assert inferrer.infer("management_change") == "corporate"
        assert inferrer.infer("goodwill_analysis") == "corporate"

    def test_infer_meta_prefix(self):
        """测试元数据前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("trading_calendar") == "meta"
        assert inferrer.infer("securities_info") == "meta"

    def test_infer_market_prefix(self):
        """测试市场前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("spot_gold") == "market"
        assert inferrer.infer("currency_zh") == "market"

    def test_infer_finance_prefix(self):
        """测试金融前缀推断"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("finance_balance") == "finance"

    def test_infer_returns_other(self):
        """测试未知函数返回 other"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("unknown_func_xyz") == "other"
        assert inferrer.infer("abc") == "other"

    def test_infer_exact_match(self):
        """测试精确匹配"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("shibor") == "macro"
        assert inferrer.infer("lpr") == "macro"
        assert inferrer.infer("cpi") == "macro"

    def test_category_rules_matching(self):
        """测试分类规则匹配"""
        inferrer = CategoryInferrer()
        for prefix, expected_category in CATEGORY_RULES.items():
            result = inferrer.infer(prefix)
            if result != expected_category:
                print(
                    f"MISMATCH: prefix={prefix!r} result={result!r} expected={expected_category!r}"
                )

    def test_infer_lof(self):
        """测试LOF基金"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("lof_holder") == "fund"

    def test_infer_fof(self):
        """测试FOF基金"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("fof_nav") == "fund"


class TestDomainExtractor:
    """测试 DomainExtractor"""

    def test_extractor_init(self):
        """测试提取器初始化"""
        extractor = DomainExtractor()
        assert extractor is not None

    def test_extract_with_urls(self):
        """测试提取URL"""
        extractor = DomainExtractor()

        def sample_func():
            """获取数据
            Source: https://api.example.com/data
            """
            pass

        result = extractor.extract(sample_func)
        assert "api.example.com" in result

    def test_extract_no_urls(self):
        """测试无URL时返回空列表"""
        extractor = DomainExtractor()

        def sample_func():
            """获取数据"""
            pass

        result = extractor.extract(sample_func)
        assert result == []

    def test_extract_multiple_urls(self):
        """测试提取多个URL"""
        extractor = DomainExtractor()

        def sample_func():
            """获取数据
            API: https://api.example.com
            Backup: https://backup.example.com
            """
            pass

        result = extractor.extract(sample_func)
        assert "api.example.com" in result
        assert "backup.example.com" in result

    def test_extract_duplicates(self):
        """测试去重"""
        extractor = DomainExtractor()

        def sample_func():
            """获取数据
            https://api.example.com
            https://api.example.com
            """
            pass

        result = extractor.extract(sample_func)
        assert result.count("api.example.com") == 1

    def test_extract_os_error(self):
        """测试 OSError 时返回空列表"""
        extractor = DomainExtractor()

        mock_func = MagicMock()
        with patch.object(
            inspect,
            "getsource",
            side_effect=OSError("cannot get source"),
        ):
            result = extractor.extract(mock_func)
            assert result == []

    def test_extract_type_error(self):
        """测试 TypeError 时返回空列表"""
        extractor = DomainExtractor()

        mock_func = MagicMock()
        with patch.object(
            inspect,
            "getsource",
            side_effect=TypeError("cannot get source"),
        ):
            result = extractor.extract(mock_func)
            assert result == []

    def test_extract_domains_helper(self):
        """测试域名提取辅助方法"""
        extractor = DomainExtractor()

        source = """
        https://api.example.com/data
        http://backup.example.com/api
        https://test.org
        """
        result = extractor._extract_domains(source)
        assert "api.example.com" in result
        assert "backup.example.com" in result
        assert "test.org" in result
        assert len(result) == 3

    def test_extract_domains_with_port(self):
        """测试带端口的域名"""
        extractor = DomainExtractor()

        source = "https://api.example.com:8080/data"
        result = extractor._extract_domains(source)
        assert "api.example.com" in result

    def test_extract_no_match(self):
        """测试无匹配时返回空列表"""
        extractor = DomainExtractor()

        source = "no urls here"
        result = extractor._extract_domains(source)
        assert result == []


class TestParamInferrer:
    """测试 ParamInferrer"""

    def test_inferrer_init(self):
        """测试推断器初始化"""
        inferrer = ParamInferrer()
        assert inferrer is not None

    def test_infer_with_signature(self):
        """测试从签名推断参数"""
        inferrer = ParamInferrer()

        def sample_func(symbol: str, limit: int):
            pass

        sig = ["symbol", "limit"]
        result = inferrer.infer(sample_func, sig)
        assert result["symbol"] == "000001"
        assert result["limit"] == 1

    def test_infer_size_limit_params(self):
        """测试大小限制参数"""
        inferrer = ParamInferrer()
        for param in {"limit", "count", "top", "recent", "size", "page_size"}:
            result = inferrer._infer_param(param)
            assert result == 1, f"Failed for {param}"

    def test_infer_symbol_param(self):
        """测试symbol参数"""
        inferrer = ParamInferrer()
        assert inferrer._infer_param("symbol") == "000001"

    def test_infer_period_param(self):
        """测试period参数"""
        inferrer = ParamInferrer()
        assert inferrer._infer_param("period") == "daily"

    def test_infer_year_param(self):
        """测试year参数"""
        inferrer = ParamInferrer()
        result = inferrer._infer_param("year")
        assert result == datetime.now().year

    def test_infer_start_date_param(self):
        """测试start_date参数"""
        inferrer = ParamInferrer()
        result = inferrer._infer_param("start_date")
        expected = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")
        assert result == expected

    def test_infer_end_date_param(self):
        """测试end_date参数"""
        inferrer = ParamInferrer()
        result = inferrer._infer_param("end_date")
        expected = datetime.now().strftime("%Y%m%d")
        assert result == expected

    def test_infer_unknown_param(self):
        """测试未知参数"""
        inferrer = ParamInferrer()
        assert inferrer._infer_param("unknown") is None

    def test_extract_doc_with_docstring(self):
        """测试提取文档字符串"""
        inferrer = ParamInferrer()

        def sample_func():
            """获取股票数据
            参数: symbol=000001
            """
            pass

        result = inferrer._extract_doc(sample_func)
        assert "获取股票数据" in result

    def test_extract_doc_without_docstring(self):
        """测试无文档字符串"""
        inferrer = ParamInferrer()

        def sample_func():
            pass

        result = inferrer._extract_doc(sample_func)
        assert result == ""

    def test_parse_doc_params(self):
        """测试解析文档参数"""
        inferrer = ParamInferrer()

        doc = "symbol=000001"
        result = inferrer._parse_doc_params(doc)
        assert "symbol" in result
        assert result["symbol"] == "000001"

    def test_parse_doc_params_single_quotes(self):
        """测试解析单引号参数"""
        inferrer = ParamInferrer()

        doc = "symbol='000001'"
        result = inferrer._parse_doc_params(doc)
        assert result["symbol"] == "000001"

    def test_parse_doc_params_double_quotes(self):
        """测试解析双引号参数"""
        inferrer = ParamInferrer()

        doc = 'symbol="000001"'
        result = inferrer._parse_doc_params(doc)
        assert result["symbol"] == "000001"

    def test_parse_doc_params_no_quotes(self):
        """测试解析无引号参数"""
        inferrer = ParamInferrer()

        doc = "symbol=000001"
        result = inferrer._parse_doc_params(doc)
        assert result["symbol"] == "000001"

    def test_parse_doc_params_excludes_type_rtype(self):
        """测试排除 type/rtype/param"""
        inferrer = ParamInferrer()

        doc = "symbol=000001 type=str param=int"
        result = inferrer._parse_doc_params(doc)
        assert "symbol" in result
        assert "type" not in result
        assert "rtype" not in result
        assert "param" not in result

    def test_infer_merges_doc_params(self):
        """测试合并文档参数"""
        inferrer = ParamInferrer()

        def sample_func(symbol: str):
            """获取数据
            symbol=600000
            """
            pass

        sig = ["symbol"]
        result = inferrer.infer(sample_func, sig)
        assert result["symbol"] == "600000"

    def test_infer_empty_signature(self):
        """测试空签名"""
        inferrer = ParamInferrer()

        def sample_func():
            pass

        result = inferrer.infer(sample_func, [])
        assert result == {}

    def test_infer_doc_overrides_signature(self):
        """测试文档参数覆盖签名推断"""
        inferrer = ParamInferrer()

        def sample_func(symbol: str):
            """获取数据
            symbol=600000
            """
            pass

        sig = ["symbol"]
        result = inferrer.infer(sample_func, sig)
        assert result["symbol"] == "600000"


class TestScannerIntegration:
    """测试扫描器集成"""

    def test_full_scan_flow(self):
        """测试完整扫描流程"""
        scanner = AkShareScanner()
        category_inferrer = CategoryInferrer()
        domain_extractor = DomainExtractor()
        param_inferrer = ParamInferrer()

        def sample_func(symbol: str = "000001", limit: int = 10):
            """获取股票数据
            Source: https://api.example.com/data
            symbol=000001
            """
            pass

        sig = scanner._extract_signature(sample_func)
        assert "symbol" in sig
        assert "limit" in sig

        cat = category_inferrer.infer("stock_zh_a_hist")
        assert cat == "equity"

        domains = domain_extractor.extract(sample_func)
        assert "api.example.com" in domains

        params = param_inferrer.infer(sample_func, sig)
        assert "symbol" in params
        assert "limit" in params

    def test_scanner_all_modules_exported(self):
        """测试所有模块已导出"""
        from akshare_data.offline.scanner import (
            AkShareScanner,
            DomainExtractor,
            CategoryInferrer,
            ParamInferrer,
        )

        assert AkShareScanner is not None
        assert DomainExtractor is not None
        assert CategoryInferrer is not None
        assert ParamInferrer is not None


class TestScannerEdgeCases:
    """测试扫描器边界情况"""

    def test_category_inferrer_empty_name(self):
        """测试空函数名"""
        inferrer = CategoryInferrer()
        assert inferrer.infer("") == "other"

    def test_domain_extractor_lambda(self):
        """测试 lambda 函数"""
        extractor = DomainExtractor()

        def lam(x):
            return x
        result = extractor.extract(lam)
        assert result == []

    def test_param_inferrer_func_no_doc(self):
        """测试无文档的函数"""
        inferrer = ParamInferrer()

        def no_doc_func(symbol):
            pass

        sig = ["symbol"]
        result = inferrer.infer(no_doc_func, sig)
        assert result["symbol"] == "000001"
