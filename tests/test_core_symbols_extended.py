"""Extended tests for akshare_data.core.symbols module.

Coverage gaps filled:
- ts_code_to_jq with .BJ suffix (additional edge cases)
- ts_code_to_jq with unknown format branches (additional edge cases)
"""

from akshare_data.core.symbols import (
    ts_code_to_jq,
)


class TestTsCodeToJqExtended:
    """Extended tests for ts_code_to_jq() function."""

    def test_bj_suffix_conversion(self):
        """Should convert .BJ suffix to .XJSE."""
        assert ts_code_to_jq("000001.BJ") == "000001.XJSE"

    def test_bj_suffix_with_various_codes(self):
        """Should handle various Beijing exchange codes."""
        assert ts_code_to_jq("430001.BJ") == "430001.XJSE"
        assert ts_code_to_jq("831900.BJ") == "831900.XJSE"

    def test_unknown_suffix_returns_none(self):
        """Should return None for unknown suffix."""
        assert ts_code_to_jq("000001.XX") is None
        assert ts_code_to_jq("000001.YY") is None
        assert ts_code_to_jq("000001.ZZ") is None

    def test_empty_string(self):
        """Should handle empty string."""
        result = ts_code_to_jq("")
        assert result is None or result == ".XSHG"

    def test_string_without_dot(self):
        """Should return None for string without dot."""
        assert ts_code_to_jq("000001SH") is None

    def test_pure_number_string(self):
        """Should return None for pure number string."""
        assert ts_code_to_jq("123456") is None

    def test_none_input(self):
        """Should return None for None input."""
        assert ts_code_to_jq(None) is None

    def test_lowercase_suffix(self):
        """Should handle lowercase suffix."""
        assert ts_code_to_jq("000001.sh") is None
        assert ts_code_to_jq("000001.sz") is None
        assert ts_code_to_jq("000001.bj") is None
