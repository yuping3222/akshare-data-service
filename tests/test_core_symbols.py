"""Tests for akshare_data.core.symbols module.

Covers:
- format_stock_symbol() function
- jq_code_to_ak() function
- ak_code_to_jq() function
- extract_code_num() function
- normalize_symbol() function
- get_symbol_prefix() function
- is_valid_stock_code() function
- is_gem_or_star() function
- calculate_limit_price() function
- jq_code_to_baostock() function
- baostock_to_jq() function
- ak_code_to_baostock() function
- baostock_to_ak() function
- ts_code_to_jq() function
"""

from akshare_data.core.symbols import (
    format_stock_symbol,
    jq_code_to_ak,
    ak_code_to_jq,
    extract_code_num,
    normalize_symbol,
    normalize_code,
    get_symbol_prefix,
    is_valid_stock_code,
    is_gem_or_star,
    calculate_limit_price,
    jq_code_to_baostock,
    baostock_to_jq,
    ak_code_to_baostock,
    baostock_to_ak,
    ts_code_to_jq,
    format_stock_symbol_for_akshare,
)


class TestFormatStockSymbol:
    """Test format_stock_symbol() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert format_stock_symbol(None) is None

    def test_handles_sh_prefix(self):
        """Should handle sh prefix."""
        assert format_stock_symbol("sh600000") == "600000"
        assert format_stock_symbol("sh600519") == "600519"

    def test_handles_sz_prefix(self):
        """Should handle sz prefix."""
        assert format_stock_symbol("sz000001") == "000001"

    def test_handles_bj_prefix(self):
        """Should handle bj prefix."""
        assert format_stock_symbol("bj000001") == "000001"

    def test_handles_jq_format(self):
        """Should handle JoinQuant format."""
        assert format_stock_symbol("600000.XSHG") == "600000"
        assert format_stock_symbol("000001.XSHE") == "000001"

    def test_handles_baostock_format(self):
        """Should handle BaoStock format."""
        assert format_stock_symbol("sh.600000") == "600000"
        assert format_stock_symbol("sz.000001") == "000001"

    def test_handles_pure_digits(self):
        """Should handle pure digit codes."""
        assert format_stock_symbol("600000") == "600000"
        assert format_stock_symbol("000001") == "000001"

    def test_pads_short_codes(self):
        """Should pad short codes to 6 digits."""
        assert format_stock_symbol("1") == "000001"
        assert format_stock_symbol("12") == "000012"

    def test_handles_of_suffix(self):
        """Should handle .OF suffix (fund)."""
        assert format_stock_symbol("159001.OF") == "159001"

    def test_handles_ccfx_suffix(self):
        """Should handle .CCFX suffix (futures) without zfill."""
        assert format_stock_symbol("IF2301.CCFX") == "IF2301"


class TestJqCodeToAk:
    """Test jq_code_to_ak() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert jq_code_to_ak(None) is None

    def test_converts_xshg(self):
        """Should convert .XSHG suffix to sh prefix."""
        assert jq_code_to_ak("600519.XSHG") == "sh600519"
        assert jq_code_to_ak("600000.XSHG") == "sh600000"

    def test_converts_xshe(self):
        """Should convert .XSHE suffix to sz prefix."""
        assert jq_code_to_ak("000001.XSHE") == "sz000001"

    def test_keeps_ak_format(self):
        """Should keep already prefixed codes unchanged."""
        assert jq_code_to_ak("sh600519") == "sh600519"
        assert jq_code_to_ak("sz000001") == "sz000001"

    def test_handles_pure_digits_xshg(self):
        """Should handle pure digits for XSHG."""
        result = jq_code_to_ak("600519")
        assert result.startswith("sh")

    def test_handles_pure_digits_xshe(self):
        """Should handle pure digits for XSHE."""
        result = jq_code_to_ak("000001")
        assert result.startswith("sz")


class TestAkCodeToJq:
    """Test ak_code_to_jq() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert ak_code_to_jq(None) is None

    def test_converts_sh_prefix(self):
        """Should convert sh prefix to .XSHG suffix."""
        assert ak_code_to_jq("sh600519") == "600519.XSHG"
        assert ak_code_to_jq("sh600000") == "600000.XSHG"

    def test_converts_sz_prefix(self):
        """Should convert sz prefix to .XSHE suffix."""
        assert ak_code_to_jq("sz000001") == "000001.XSHE"

    def test_keeps_jq_format(self):
        """Should keep already suffixed codes unchanged."""
        assert ak_code_to_jq("600519.XSHG") == "600519.XSHG"
        assert ak_code_to_jq("000001.XSHE") == "000001.XSHE"

    def test_handles_pure_digits_sh(self):
        """Should handle pure digits as SH."""
        result = ak_code_to_jq("600519")
        assert result == "600519.XSHG"

    def test_handles_pure_digits_sz(self):
        """Should handle pure digits as SZ."""
        result = ak_code_to_jq("000001")
        assert result == "000001.XSHE"


class TestExtractCodeNum:
    """Test extract_code_num() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert extract_code_num(None) is None

    def test_extracts_from_sh_prefix(self):
        """Should extract from sh prefix."""
        assert extract_code_num("sh600000") == "600000"

    def test_extracts_from_sz_prefix(self):
        """Should extract from sz prefix."""
        assert extract_code_num("sz000001") == "000001"

    def test_extracts_from_jq_format(self):
        """Should extract from JoinQuant format."""
        assert extract_code_num("600519.XSHG") == "600519"
        assert extract_code_num("000001.XSHE") == "000001"

    def test_extracts_from_baostock_format(self):
        """Should extract from BaoStock format."""
        assert extract_code_num("sh.600000") == "600000"

    def test_pads_short_codes(self):
        """Should pad short codes."""
        assert extract_code_num("1") == "000001"


class TestNormalizeSymbol:
    """Test normalize_symbol() function."""

    def test_is_alias_for_format_stock_symbol(self):
        """normalize_symbol should be alias for format_stock_symbol."""
        assert normalize_symbol("sh600519") == format_stock_symbol("sh600519")
        assert normalize_symbol("600519.XSHG") == format_stock_symbol("600519.XSHG")

    def test_handles_none(self):
        """Should return None for None input."""
        assert normalize_symbol(None) is None

    def test_handles_various_formats(self):
        """Should normalize various formats."""
        assert normalize_symbol("sh600519") == "600519"
        assert normalize_symbol("sz000001") == "000001"
        assert normalize_symbol("600519") == "600519"


class TestNormalizeCode:
    """Test normalize_code alias."""

    def test_is_same_as_normalize_symbol(self):
        """normalize_code should be same as normalize_symbol."""
        assert normalize_code("sh600519") == normalize_symbol("sh600519")


class TestGetSymbolPrefix:
    """Test get_symbol_prefix() function."""

    def test_returns_sh_for_shanghai(self):
        """Should return 'sh' for Shanghai codes."""
        assert get_symbol_prefix("sh600519") == "sh"
        assert get_symbol_prefix("600519.XSHG") == "sh"
        assert get_symbol_prefix("600519") == "sh"

    def test_returns_sz_for_shenzhen(self):
        """Should return 'sz' for Shenzhen codes."""
        assert get_symbol_prefix("sz000001") == "sz"
        assert get_symbol_prefix("000001.XSHE") == "sz"
        assert get_symbol_prefix("000001") == "sz"


class TestIsValidStockCode:
    """Test is_valid_stock_code() function."""

    def test_handles_none(self):
        """Should return False for None."""
        assert is_valid_stock_code(None) is False

    def test_valid_sh_prefix(self):
        """Should accept sh prefix format."""
        assert is_valid_stock_code("sh600519") is True
        assert is_valid_stock_code("sh000001") is True

    def test_valid_sz_prefix(self):
        """Should accept sz prefix format."""
        assert is_valid_stock_code("sz000001") is True

    def test_valid_pure_digits(self):
        """Should accept pure 6-digit codes."""
        assert is_valid_stock_code("600519") is True
        assert is_valid_stock_code("000001") is True

    def test_valid_jq_format(self):
        """Should accept JoinQuant format."""
        assert is_valid_stock_code("600519.XSHG") is True
        assert is_valid_stock_code("000001.XSHE") is True

    def test_valid_baostock_format(self):
        """Should accept BaoStock format."""
        assert is_valid_stock_code("sh.600519") is True
        assert is_valid_stock_code("sz.000001") is True

    def test_invalid_short_code(self):
        """Should reject short codes."""
        assert is_valid_stock_code("600") is False
        assert is_valid_stock_code("123") is False

    def test_invalid_format(self):
        """Should reject invalid formats."""
        assert is_valid_stock_code("invalid") is False
        assert is_valid_stock_code("ABC123") is False


class TestIsGemOrStar:
    """Test is_gem_or_star() function."""

    def test_创业板_codes(self):
        """Should return True for 300xxx codes."""
        assert is_gem_or_star("300001") is True
        assert is_gem_or_star("300750") is True

    def test_科创板_codes(self):
        """Should return True for 688xxx codes."""
        assert is_gem_or_star("688001") is True
        assert is_gem_or_star("688599") is True

    def test_other_codes(self):
        """Should return False for other codes."""
        assert is_gem_or_star("600519") is False
        assert is_gem_or_star("000001") is False


class TestCalculateLimitPrice:
    """Test calculate_limit_price() function."""

    def test_handles_none_prev_close(self):
        """Should return None for None prev_close."""
        assert calculate_limit_price(None, "600519") is None

    def test_handles_zero_prev_close(self):
        """Should return None for zero prev_close."""
        assert calculate_limit_price(0, "600519") is None

    def test_handles_negative_prev_close(self):
        """Should return None for negative prev_close."""
        assert calculate_limit_price(-1.0, "600519") is None

    def test_calculates_limit_up_main_board(self):
        """Should calculate 10% limit up for main board."""
        result = calculate_limit_price(100.0, "600519", direction="up")
        assert result == 110.0

    def test_calculates_limit_down_main_board(self):
        """Should calculate 10% limit down for main board."""
        result = calculate_limit_price(100.0, "600519", direction="down")
        assert result == 90.0

    def test_calculates_limit_up_gem_star(self):
        """Should calculate 20% limit up for GEM/STAR."""
        result = calculate_limit_price(100.0, "300001", direction="up")
        assert result == 120.0

    def test_calculates_limit_down_gem_star(self):
        """Should calculate 20% limit down for GEM/STAR."""
        result = calculate_limit_price(100.0, "688001", direction="down")
        assert result == 80.0


class TestJqCodeToBaostock:
    """Test jq_code_to_baostock() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert jq_code_to_baostock(None) is None

    def test_converts_xshg(self):
        """Should convert .XSHG to sh. prefix."""
        assert jq_code_to_baostock("600519.XSHG") == "sh.600519"

    def test_converts_xshe(self):
        """Should convert .XSHE to sz. prefix."""
        assert jq_code_to_baostock("000001.XSHE") == "sz.000001"

    def test_keeps_baostock_format(self):
        """Should keep BaoStock format unchanged."""
        assert jq_code_to_baostock("sh.600519") == "sh.600519"
        assert jq_code_to_baostock("sz.000001") == "sz.000001"

    def test_handles_ak_format(self):
        """Should convert AkShare format."""
        assert jq_code_to_baostock("sh600519") == "sh.600519"
        assert jq_code_to_baostock("sz000001") == "sz.000001"


class TestBaostockToJq:
    """Test baostock_to_jq() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert baostock_to_jq(None) is None

    def test_converts_sh_prefix(self):
        """Should convert sh. to .XSHG."""
        assert baostock_to_jq("sh.600519") == "600519.XSHG"

    def test_converts_sz_prefix(self):
        """Should convert sz. to .XSHE."""
        assert baostock_to_jq("sz.000001") == "000001.XSHE"

    def test_keeps_jq_format(self):
        """Should keep JoinQuant format unchanged."""
        assert baostock_to_jq("600519.XSHG") == "600519.XSHG"


class TestAkCodeToBaostock:
    """Test ak_code_to_baostock() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert ak_code_to_baostock(None) is None

    def test_converts_sh(self):
        """Should convert sh prefix to sh. format."""
        assert ak_code_to_baostock("sh600519") == "sh.600519"

    def test_converts_sz(self):
        """Should convert sz prefix to sz. format."""
        assert ak_code_to_baostock("sz000001") == "sz.000001"


class TestBaostockToAk:
    """Test baostock_to_ak() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert baostock_to_ak(None) is None

    def test_converts_sh_prefix(self):
        """Should convert sh. to sh format."""
        assert baostock_to_ak("sh.600519") == "sh600519"

    def test_converts_sz_prefix(self):
        """Should convert sz. to sz format."""
        assert baostock_to_ak("sz.000001") == "sz000001"


class TestTsCodeToJq:
    """Test ts_code_to_jq() function."""

    def test_handles_none(self):
        """Should return None for None input."""
        assert ts_code_to_jq(None) is None

    def test_converts_sz_suffix(self):
        """Should convert .SZ to .XSHE."""
        assert ts_code_to_jq("000001.SZ") == "000001.XSHE"

    def test_converts_sh_suffix(self):
        """Should convert .SH to .XSHG."""
        assert ts_code_to_jq("600000.SH") == "600000.XSHG"

    def test_converts_bj_suffix(self):
        """Should convert .BJ to .XJSE."""
        assert ts_code_to_jq("000001.BJ") == "000001.XJSE"

    def test_keeps_xshg_unchanged(self):
        """Should keep .XSHG unchanged."""
        assert ts_code_to_jq("600000.XSHG") == "600000.XSHG"

    def test_keeps_xshe_unchanged(self):
        """Should keep .XSHE unchanged."""
        assert ts_code_to_jq("000001.XSHE") == "000001.XSHE"

    def test_returns_none_for_unknown_suffix(self):
        """Should return None for unknown suffix."""
        assert ts_code_to_jq("000001.XX") is None


class TestFormatStockSymbolForAkshare:
    """Test format_stock_symbol_for_akshare alias."""

    def test_is_same_as_format_stock_symbol(self):
        """Should be same as format_stock_symbol."""
        assert format_stock_symbol_for_akshare("sh600519") == format_stock_symbol(
            "sh600519"
        )
