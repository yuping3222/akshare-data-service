# System Raw Samples

This directory stores canonical Raw (L0) sample archives for the system.

## Purpose

`data/system/raw_samples/` is the authoritative archive of representative Raw
batches that serve as:

- **Golden references** for field mapping and normalization tests.
- **Schema drift baselines** — each sample represents a known-good schema version.
- **Replay anchors** — tests can replay from these samples without network access.
- **Contract test fixtures** — verify that the reader/writer/manifest pipeline
  produces conformant output.

## Difference from `tests/fixtures/raw_samples/`

| Aspect | `tests/fixtures/raw_samples/` | `data/system/raw_samples/` |
|--------|-------------------------------|----------------------------|
| Location | Inside test tree | Inside data tree |
| Purpose | Test fixtures loaded by pytest | System-level reference archive |
| Lifecycle | Version-controlled with code | May be generated/updated by offline tools |
| Size | Small, minimal samples | Can include larger representative batches |

Both directories follow the same naming conventions and structure.

## Directory Structure

```
data/system/raw_samples/
├── README.md                          # This file
├── cn/
│   ├── market_quote_daily/
│   │   └── extract_date=2026-04-22/
│   │       └── batch_id=20260422_001/
│   │           ├── part-000.parquet
│   │           ├── _manifest.json
│   │           └── _schema.json
│   ├── financial_indicator/
│   └── macro_indicator/
└── system/
```

## Naming Conventions

Same as `tests/fixtures/raw_samples/README.md`:

- Domain: `cn`, `us`, `system`
- Dataset: canonical names (`market_quote_daily`, `financial_indicator`, `macro_indicator`)
- Partitions: `extract_date=YYYY-MM-DD/batch_id=YYYYMMDD_NNN/`
- Files: `part-NNN.parquet`, `_manifest.json`, `_schema.json`

## Required Metadata

Each batch must have a valid `_manifest.json` with all required fields per
`20-raw-spec.md` §6. The `source_name` and `interface_name` fields must be
preserved to enable source-specific replay.

## Adding Samples

Samples can be added by:

1. **Manual copy**: Copy a batch from `data/raw/` into the appropriate path.
2. **Offline tools**: Use the downloader or scanner to capture representative batches.
3. **Test generation**: Generate synthetic samples for edge cases.

After adding, verify:
- `RawReader` can read the batch back.
- `ReplayEngine` can replay it.
- Schema fingerprint matches `_schema.json`.
- All 10 system fields are present.

## Retention

System raw samples are retained indefinitely as reference data. They should be
committed to version control (if small) or tracked in a separate artifact store
(if large).
