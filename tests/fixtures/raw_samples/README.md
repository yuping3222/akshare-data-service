# Raw Sample Fixtures

This directory stores representative Raw (L0) sample data for testing.

## Purpose

Raw samples serve as stable, version-controlled evidence for:

- **Field mapping tests**: Verify that source columns map correctly to standard fields.
- **Schema drift tests**: Detect when source schemas change between versions.
- **Replay tests**: Re-run normalization pipelines without depending on live source stations.
- **Contract tests**: Validate that reader/manifest/schema structures conform to `20-raw-spec.md`.

## Directory Structure

```
tests/fixtures/raw_samples/
├── README.md                          # This file
├── cn/
│   ├── market_quote_daily/
│   │   ├── extract_date=2026-04-22/
│   │   │   └── batch_id=20260422_001/
│   │   │       ├── part-000.parquet    # Raw data with system fields
│   │   │       ├── _manifest.json      # Batch metadata
│   │   │       └── _schema.json        # Schema snapshot
│   │   └── extract_date=2026-04-21/
│   │       └── batch_id=20260421_001/
│   │           ├── part-000.parquet
│   │           ├── _manifest.json
│   │           └── _schema.json
│   ├── financial_indicator/
│   └── macro_indicator/
└── system/
```

## Sample Selection Criteria

Each dataset should have at least one sample batch that:

1. **Represents a typical response**: Not edge cases, not error responses.
2. **Includes system fields**: All 10 system columns must be present.
3. **Has a valid manifest**: Conforms to `20-raw-spec.md` §6.
4. **Has a schema snapshot**: `_schema.json` matches the parquet file columns.
5. **Is anonymized**: No real tokens, credentials, or sensitive identifiers.

## Naming Conventions

- **Domain**: Use standard domain names (`cn`, `us`, `system`).
- **Dataset**: Use canonical dataset names (e.g., `market_quote_daily`, not `stock_daily`).
- **extract_date**: Use `extract_date=YYYY-MM-DD` directory format.
- **batch_id**: Use `batch_id=YYYYMMDD_NNN` directory format.
- **Parquet files**: Use `part-NNN.parquet` naming.

## Required Metadata

Each `_manifest.json` must include:

| Field | Required | Description |
|-------|----------|-------------|
| `manifest_version` | Yes | Always "1.0" |
| `dataset` | Yes | Canonical dataset name |
| `domain` | Yes | Data domain |
| `batch_id` | Yes | Batch identifier |
| `extract_date` | Yes | Extraction date (ISO format) |
| `source_name` | Yes | Source adapter (e.g., "akshare") |
| `interface_name` | Yes | Source interface name |
| `request_params` | Yes | Request parameters as JSON object |
| `record_count` | Yes | Number of records in the batch |
| `file_count` | Yes | Number of parquet files |
| `schema_fingerprint` | Yes | SHA-256 fingerprint of schema |
| `extract_version` | Yes | Extraction version tag |
| `status` | Yes | Batch status ("success", "partial", etc.) |

## Using Samples in Tests

```python
from pathlib import Path
from akshare_data.raw.reader import RawReader
from akshare_data.raw.replay import ReplayEngine

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "raw_samples"

# Read a specific batch
reader = RawReader(base_dir=str(FIXTURES_DIR))
df = reader.read_by_batch_id("20260422_001", domain="cn", dataset="market_quote_daily")

# Replay with a processor
engine = ReplayEngine(base_dir=str(FIXTURES_DIR))
result = engine.replay_by_batch_id("20260422_001")
assert result.schema_compatible
assert result.record_count > 0
```

## Adding New Samples

1. Capture a representative batch from the live source (or use an existing Raw batch).
2. Copy the batch directory into the appropriate `domain/dataset/extract_date=batch_id/` path.
3. Verify the manifest is valid and all system fields are present.
4. Anonymize any sensitive data (tokens, internal IDs, etc.).
5. Add a note in this README about what the sample represents.

## Current Samples

| Domain | Dataset | Batch ID | Extract Date | Source | Interface | Description |
|--------|---------|----------|--------------|--------|-----------|-------------|
| cn | market_quote_daily | 20260422_001 | 2026-04-22 | akshare | stock_zh_a_hist | Typical A-share daily quote for 600519 |
| cn | financial_indicator | 20260422_001 | 2026-04-22 | akshare | stock_financial_analysis_indicator | Typical financial indicator data |
| cn | macro_indicator | 20260422_001 | 2026-04-22 | akshare | macro_china_gdp | Typical macro GDP data |

> Note: Samples marked as "TODO" need to be populated. The directory structure is reserved.
