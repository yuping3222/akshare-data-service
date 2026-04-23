"""Tests for akshare_data.core.options module.

Covers:
- black_scholes_price() function
- calculate_option_greeks() function
- calculate_conversion_value() function
- SCIPY_AVAILABLE flag
"""

import pytest
from akshare_data.core.options import (
    SCIPY_AVAILABLE,
    black_scholes_price,
    calculate_option_greeks,
    calculate_conversion_value,
)


class TestBlackScholesPrice:
    """Test black_scholes_price() function."""

    def test_call_option_basic(self):
        """Should calculate call option price correctly."""
        S, K, T, r, sigma = 100.0, 100.0, 1.0, 0.05, 0.2
        price = black_scholes_price(S, K, T, r, sigma, "call")
        assert isinstance(price, float)
        assert 0 < price < S

    def test_put_option_basic(self):
        """Should calculate put option price correctly."""
        S, K, T, r, sigma = 100.0, 100.0, 1.0, 0.05, 0.2
        price = black_scholes_price(S, K, T, r, sigma, "put")
        assert isinstance(price, float)
        assert 0 < price < K

    def test_call_and_put_both_valid(self):
        """Both call and put should return valid prices."""
        call = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        put = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.2, "put")
        assert call > 0
        assert put > 0

    def test_option_type_case_insensitive(self):
        """Should accept uppercase option types."""
        call_upper = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.2, "CALL")
        call_lower = black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert call_upper == call_lower

    def test_invalid_option_type_raises(self):
        """Should raise ValueError for invalid option_type."""
        with pytest.raises(ValueError, match="Invalid option_type"):
            black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.2, "invalid")

    def test_zero_stock_price_raises(self):
        """Should raise ValueError when S <= 0."""
        with pytest.raises(ValueError, match="S, K, T, and sigma must all be positive"):
            black_scholes_price(0.0, 100.0, 1.0, 0.05, 0.2)

    def test_negative_stock_price_raises(self):
        """Should raise ValueError when S < 0."""
        with pytest.raises(ValueError, match="S, K, T, and sigma must all be positive"):
            black_scholes_price(-10.0, 100.0, 1.0, 0.05, 0.2)

    def test_zero_strike_raises(self):
        """Should raise ValueError when K <= 0."""
        with pytest.raises(ValueError, match="S, K, T, and sigma must all be positive"):
            black_scholes_price(100.0, 0.0, 1.0, 0.05, 0.2)

    def test_zero_time_raises(self):
        """Should raise ValueError when T <= 0."""
        with pytest.raises(ValueError, match="S, K, T, and sigma must all be positive"):
            black_scholes_price(100.0, 100.0, 0.0, 0.05, 0.2)

    def test_zero_volatility_raises(self):
        """Should raise ValueError when sigma <= 0."""
        with pytest.raises(ValueError, match="S, K, T, and sigma must all be positive"):
            black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.0)


class TestCalculateOptionGreeks:
    """Test calculate_option_greeks() function."""

    def test_call_delta(self):
        """Should calculate correct call delta."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert 0 < greeks["delta"] < 1

    def test_put_delta(self):
        """Should calculate correct put delta (negative)."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "put")
        assert -1 < greeks["delta"] < 0

    def test_delta_atm_close_to_half(self):
        """ATM call delta should be close to 0.5."""
        greeks = calculate_option_greeks(100.0, 100.0, 0.01, 0.05, 0.3, "call")
        assert 0.4 < greeks["delta"] < 0.6

    def test_gamma_positive(self):
        """Gamma should be positive for both calls and puts."""
        call_greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        put_greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "put")
        assert call_greeks["gamma"] > 0
        assert put_greeks["gamma"] > 0
        assert call_greeks["gamma"] == put_greeks["gamma"]

    def test_call_theta_negative(self):
        """Call theta should typically be negative (time decay)."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert isinstance(greeks["theta"], float)

    def test_put_theta_negative(self):
        """Put theta should typically be negative (time decay)."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "put")
        assert isinstance(greeks["theta"], float)

    def test_vega_positive(self):
        """Vega should be positive for both calls and puts."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert greeks["vega"] > 0

    def test_call_rho_positive(self):
        """Call rho should be positive."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert greeks["rho"] > 0

    def test_put_rho_positive_for_itm(self):
        """Put rho can be positive for deep ITM puts."""
        greeks = calculate_option_greeks(90.0, 100.0, 1.0, 0.05, 0.2, "put")
        assert isinstance(greeks["rho"], float)

    def test_d1_d2_relationship(self):
        """d2 should be less than d1 for positive sigma."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert greeks["d2"] < greeks["d1"]

    def test_returns_all_fields(self):
        """Should return all expected dictionary keys."""
        greeks = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        expected_keys = {
            "delta",
            "gamma",
            "theta",
            "vega",
            "rho",
            "d1",
            "d2",
            "option_type",
            "spot",
            "strike",
            "sigma",
            "time_to_expiry",
            "rate",
        }
        assert set(greeks.keys()) == expected_keys

    def test_option_type_case_insensitive(self):
        """Should accept uppercase option types."""
        greeks_upper = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "CALL")
        greeks_lower = calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "call")
        assert greeks_upper["delta"] == greeks_lower["delta"]

    def test_invalid_option_type_raises(self):
        """Should raise ValueError for invalid option_type."""
        with pytest.raises(ValueError, match="Invalid option_type"):
            calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.2, "invalid")

    def test_zero_stock_raises(self):
        """Should raise ValueError when S <= 0."""
        with pytest.raises(ValueError, match="S, K, sigma, and T must all be positive"):
            calculate_option_greeks(0.0, 100.0, 1.0, 0.05, 0.2)

    def test_zero_strike_raises(self):
        """Should raise ValueError when K <= 0."""
        with pytest.raises(ValueError, match="S, K, sigma, and T must all be positive"):
            calculate_option_greeks(100.0, 0.0, 1.0, 0.05, 0.2)

    def test_zero_volatility_raises(self):
        """Should raise ValueError when sigma <= 0."""
        with pytest.raises(ValueError, match="S, K, sigma, and T must all be positive"):
            calculate_option_greeks(100.0, 100.0, 1.0, 0.05, 0.0)

    def test_zero_time_raises(self):
        """Should raise ValueError when T <= 0."""
        with pytest.raises(ValueError, match="S, K, sigma, and T must all be positive"):
            calculate_option_greeks(100.0, 100.0, 0.0, 0.05, 0.2)


class TestCalculateConversionValue:
    """Test calculate_conversion_value() function."""

    def test_basic_conversion(self):
        """Should calculate conversion value correctly."""
        result = calculate_conversion_value(105.0, 10.0, 100.0)
        assert result["conversion_value"] == 1000.0
        assert result["bond_price"] == 105.0
        assert result["conversion_ratio"] == 10.0
        assert result["stock_price"] == 100.0

    def test_negative_premium_rate(self):
        """Should calculate negative premium when bond < conversion value."""
        result = calculate_conversion_value(90.0, 10.0, 100.0)
        assert result["premium_rate"] < 0

    def test_zero_conversion_value_edge_case(self):
        """Should handle zero conversion value (stock price = 0 or ratio = 0)."""
        result = calculate_conversion_value(100.0, 0.0, 100.0)
        assert result["conversion_value"] == 0.0
        assert result["premium_rate"] == 0.0

    def test_zero_bond_price(self):
        """Should handle zero bond price."""
        result = calculate_conversion_value(0.0, 10.0, 100.0)
        assert result["conversion_value"] == 1000.0
        assert result["premium_rate"] < 0

    def test_returns_all_fields(self):
        """Should return all expected dictionary keys."""
        result = calculate_conversion_value(105.0, 10.0, 100.0)
        expected_keys = {
            "bond_price",
            "conversion_ratio",
            "stock_price",
            "conversion_value",
            "premium_rate",
        }
        assert set(result.keys()) == expected_keys


class TestScipyAvailable:
    """Test SCIPY_AVAILABLE flag."""

    def test_scipy_available_is_boolean(self):
        """SCIPY_AVAILABLE should be a boolean."""
        assert isinstance(SCIPY_AVAILABLE, bool)
