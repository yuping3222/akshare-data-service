"""Test CompletenessChecker with sample data"""

import pandas as pd
from datetime import datetime, timedelta

from akshare_data.offline.analyzer.cache_analysis import CompletenessChecker


def test_completeness_checker():
    # Generate a date range for 30 trading days (approx)
    all_dates = pd.date_range("2025-01-01", periods=30, freq="B")  # business days
    all_dates_str = [d.strftime("%Y-%m-%d") for d in all_dates]

    # Create sample data with some missing dates
    # Remove 5 dates to simulate missing data
    missing = all_dates_str[5:10]
    actual_dates = [d for d in all_dates_str if d not in missing]

    df = pd.DataFrame({
        "date": actual_dates,
        "symbol": ["AAPL"] * len(actual_dates),
        "open": [100.0 + i for i in range(len(actual_dates))],
        "close": [101.0 + i for i in range(len(actual_dates))],
        "volume": [1000000 + i * 1000 for i in range(len(actual_dates))],
    })

    print("=== Sample DataFrame ===")
    print(f"Shape: {df.shape}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Records: {len(df)}")
    print()

    # Run completeness check with expected dates
    checker = CompletenessChecker()
    result = checker.check(df, expected_dates=all_dates_str)

    print("=== Completeness Report ===")
    print(f"Has data: {result['has_data']}")
    print(f"Total records: {result['total_records']}")
    print(f"Missing dates count: {result['missing_dates_count']}")
    print(f"Completeness ratio: {result['completeness_ratio']:.2%}")
    print(f"Is complete: {result['is_complete']}")
    print(f"Missing dates: {result['missing_dates']}")
    print()

    # Test with missing fields
    result2 = checker.check(
        df,
        expected_dates=all_dates_str,
        required_fields=["date", "open", "close", "high", "low", "volume"],
    )
    print("=== Completeness Report (with required fields) ===")
    print(f"Missing fields: {result2['missing_fields']}")
    print(f"Is complete: {result2['is_complete']}")
    print()

    # Test empty DataFrame
    empty_df = pd.DataFrame()
    result3 = checker.check(empty_df)
    print("=== Empty DataFrame Test ===")
    print(f"Has data: {result3['has_data']}")
    print(f"Completeness ratio: {result3['completeness_ratio']}")


if __name__ == "__main__":
    test_completeness_checker()
