"""Pure mathematical functions for options pricing and convertible bond valuation.

These functions have no akshare/data source dependency and implement:
- Black-Scholes option pricing model
- Option Greeks calculation (delta, gamma, theta, vega, rho)
- Convertible bond conversion value and premium rate

Uses scipy.stats.norm when available, falls back to math.erf otherwise.
"""

from __future__ import annotations

import math
from typing import Any, Dict

# ── Normal distribution helpers (scipy or math fallback) ─────────────

try:
    from scipy.stats import norm as _scipy_norm

    def _norm_cdf(x: float) -> float:
        return float(_scipy_norm.cdf(x))

    def _norm_pdf(x: float) -> float:
        return float(_scipy_norm.pdf(x))

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

    def _norm_cdf(x: float) -> float:
        """Standard normal CDF using math.erf."""
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def _norm_pdf(x: float) -> float:
        """Standard normal PDF."""
        return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


# ── Black-Scholes option pricing ─────────────────────────────────────


def black_scholes_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str = "call",
) -> float:
    """Calculate the Black-Scholes option price.

    Args:
        S: Current stock/underlying price
        K: Strike price
        T: Time to expiration in years
        r: Risk-free interest rate (annualized)
        sigma: Volatility of the underlying (annualized)
        option_type: "call" or "put"

    Returns:
        The theoretical option price

    Raises:
        ValueError: If any parameter is invalid
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        raise ValueError("S, K, T, and sigma must all be positive")

    option_type = option_type.lower()
    if option_type not in ("call", "put"):
        raise ValueError(
            f"Invalid option_type: {option_type!r}. Must be 'call' or 'put'"
        )

    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "call":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


# ── Option Greeks ────────────────────────────────────────────────────


def calculate_option_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str = "call",
) -> Dict[str, float]:
    """Calculate Black-Scholes option Greeks.

    Args:
        S: Current stock/underlying price (spot)
        K: Strike price
        T: Time to expiration in years
        r: Risk-free interest rate (annualized)
        sigma: Volatility of the underlying (annualized)
        option_type: "call" or "put"

    Returns:
        Dictionary with keys: delta, gamma, theta, vega, rho, d1, d2,
        option_type, spot, strike, sigma, time_to_expiry, rate

    Raises:
        ValueError: If any parameter is invalid
    """
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        raise ValueError("S, K, sigma, and T must all be positive")

    option_type = option_type.lower()
    if option_type not in ("call", "put"):
        raise ValueError(
            f"Invalid option_type: {option_type!r}. Must be 'call' or 'put'"
        )

    sqrt_t = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t

    norm_pdf_d1 = _norm_pdf(d1)

    # Delta
    if option_type == "call":
        delta = _norm_cdf(d1)
    else:
        delta = _norm_cdf(d1) - 1.0

    # Gamma (same for call and put)
    gamma = norm_pdf_d1 / (S * sigma * sqrt_t)

    # Theta (per calendar day, divided by 365)
    if option_type == "call":
        theta = (
            -S * norm_pdf_d1 * sigma / (2.0 * sqrt_t)
            - r * K * math.exp(-r * T) * _norm_cdf(d2)
        ) / 365.0
    else:
        theta = (
            -S * norm_pdf_d1 * sigma / (2.0 * sqrt_t)
            + r * K * math.exp(-r * T) * _norm_cdf(-d2)
        ) / 365.0

    # Vega (per 1% change in volatility)
    vega = S * norm_pdf_d1 * sqrt_t / 100.0

    # Rho (per 1% change in interest rate)
    if option_type == "call":
        rho = K * T * math.exp(-r * T) * _norm_cdf(d2) / 100.0
    else:
        rho = K * T * math.exp(-r * T) * _norm_cdf(-d2) / 100.0

    return {
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "rho": rho,
        "d1": d1,
        "d2": d2,
        "option_type": option_type,
        "spot": S,
        "strike": K,
        "sigma": sigma,
        "time_to_expiry": T,
        "rate": r,
    }


# ── Convertible bond valuation ───────────────────────────────────────


def calculate_conversion_value(
    bond_price: float,
    conversion_ratio: float,
    stock_price: float,
) -> Dict[str, Any]:
    """Calculate convertible bond conversion value and premium rate.

    Args:
        bond_price: Current market price of the convertible bond
        conversion_ratio: Number of shares per bond (conversion ratio)
        stock_price: Current price of the underlying stock

    Returns:
        Dictionary with keys: bond_price, conversion_ratio, stock_price,
        conversion_value, premium_rate
    """
    conversion_value = conversion_ratio * stock_price
    if conversion_value != 0:
        premium_rate = (bond_price - conversion_value) / conversion_value * 100.0
    else:
        premium_rate = 0.0

    return {
        "bond_price": bond_price,
        "conversion_ratio": conversion_ratio,
        "stock_price": stock_price,
        "conversion_value": conversion_value,
        "premium_rate": premium_rate,
    }


__all__ = [
    "SCIPY_AVAILABLE",
    "black_scholes_price",
    "calculate_option_greeks",
    "calculate_conversion_value",
]
