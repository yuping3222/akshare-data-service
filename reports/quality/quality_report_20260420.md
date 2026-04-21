# Data Quality Report

- **Report Time**: 2026-04-20 13:11:30
- **Table**: stock_daily
- **Symbol**: N/A

# Completeness Check

- **has_data**: True
- **total_records**: 100
- **missing_dates_count**: 0
- **missing_dates**: []
- **missing_fields**: []
- **completeness_ratio**: 1.0
- **is_complete**: True

# Anomaly Detection

- **Total Anomalies**: 44
- **Details**: [{'type': 'price', 'index': 10, 'value': 25.0, 'threshold': 20.0}, {'type': 'price', 'index': 20, 'value': -22.0, 'threshold': 20.0}, {'type': 'high_low', 'index': 0, 'high': 10.35, 'low': 15.42}, {'type': 'high_low', 'index': 3, 'high': 15.59, 'low': 17.99}, {'type': 'high_low', 'index': 10, 'high': 13.19, 'low': 14.49}, {'type': 'high_low', 'index': 11, 'high': 11.77, 'low': 15.92}, {'type': 'high_low', 'index': 17, 'high': 12.05, 'low': 16.46}, {'type': 'high_low', 'index': 19, 'high': 15.93, 'low': 17.49}, {'type': 'high_low', 'index': 23, 'high': 11.21, 'low': 12.68}, {'type': 'high_low', 'index': 28, 'high': 10.08, 'low': 17.92}, {'type': 'high_low', 'index': 30, 'high': 5.0, 'low': 15.0}, {'type': 'high_low', 'index': 31, 'high': 12.44, 'low': 14.03}, {'type': 'high_low', 'index': 32, 'high': 11.32, 'low': 14.77}, {'type': 'high_low', 'index': 33, 'high': 13.71, 'low': 13.93}, {'type': 'high_low', 'index': 35, 'high': 13.56, 'low': 16.22}, {'type': 'high_low', 'index': 38, 'high': 14.0, 'low': 15.45}, {'type': 'high_low', 'index': 41, 'high': 12.77, 'low': 18.54}, {'type': 'high_low', 'index': 42, 'high': 15.47, 'low': 18.15}, {'type': 'high_low', 'index': 45, 'high': 10.41, 'low': 18.28}, {'type': 'high_low', 'index': 47, 'high': 15.53, 'low': 18.67}]

# Summary

- **total_records**: 100
- **columns**: ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'pct_change']
- **has_data**: True
