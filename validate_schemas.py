"""Comprehensive schema validation: schema.py tables vs interface configs vs akShare output.

Usage: python validate_schemas.py
"""
from __future__ import annotations

import sys
import os
import warnings
warnings.filterwarnings("ignore")

# ── Load all schema sources ────────────────────────────────
from akshare_data.core.schema import _DEFAULT_TABLES, get_table_schema, list_tables
from akshare_data.core.config_cache import ConfigCache
import yaml

# Load schemas.yaml for comparison
schemas_yaml_path = os.path.join(os.path.dirname(__file__), "config", "schemas.yaml")
with open(schemas_yaml_path) as f:
    schemas_yaml_data = yaml.safe_load(f)

# Load all interface configs
interfaces = ConfigCache.load_interfaces()

# Build interface_name -> table_name mapping (reverse lookup)
from akshare_data.offline.downloader.task_builder import TaskBuilder
builder = TaskBuilder()

schema_tables = {table.name: table for table in _DEFAULT_TABLES}

print("=" * 80)
print("SCHEMA VALIDATION REPORT")
print(f"Tables in schema.py _DEFAULT_TABLES: {len(_DEFAULT_TABLES)}")
print(f"Tables in schemas.yaml: {len(schemas_yaml_data.get('tables', {}))}")
print(f"Interfaces in config: {len(interfaces)}")
print("=" * 80)

# ── Part 1: Check which schema.py tables have matching interfaces ──
print("\n1. SCHEMA.PY TABLES vs INTERFACE CONFIGS\n")
print(f"{'Table':30s} {'Interface':35s} {'akShare Func':35s} {'Mapped?'}")
print("-" * 120)

tables_with_interface = {}
tables_without_interface = []

for table_name, table_def in schema_tables.items():
    found_iface = None
    found_func = None
    
    # Direct match: table name == interface name
    if table_name in interfaces:
        found_iface = table_name
        for src in interfaces[table_name].get("sources", []):
            if src.get("enabled", True):
                found_func = src.get("func", "")
                break
    else:
        # Check aliases (reverse mapping)
        for alias, target in builder.INTERFACE_TABLE_ALIASES.items():
            if target == table_name and alias in interfaces:
                found_iface = alias
                for src in interfaces[alias].get("sources", []):
                    if src.get("enabled", True):
                        found_func = src.get("func", "")
                        break
                break
    
    if found_iface:
        tables_with_interface[table_name] = (found_iface, found_func)
        print(f"{table_name:30s} {found_iface:35s} {found_func or 'multisource':35s} YES")
    else:
        tables_without_interface.append(table_name)
        print(f"{table_name:30s} {'N/A':35s} {'N/A':35s} NO")

# ── Part 2: Interfaces WITHOUT schema.py tables ──
print(f"\n\n2. INTERFACES WITHOUT SCHEMA.PY TABLE ({len(interfaces)} total)\n")
print(f"{'Interface':35s} {'Cache Table':35s} {'akShare Func':35s}")
print("-" * 105)

interfaces_without_schema = []
for iface_name, iface_def in sorted(interfaces.items()):
    table = builder._resolve_cache_table(iface_name)
    schema = get_table_schema(table)
    if schema is None:
        func = ""
        for src in iface_def.get("sources", []):
            if src.get("enabled", True):
                func = src.get("func", "")
                break
        interfaces_without_schema.append((iface_name, table, func))
        print(f"{iface_name:35s} {table:35s} {func or 'multisource':35s}")

print(f"\nTotal interfaces without schema: {len(interfaces_without_schema)}")

# ── Part 3: Check schema fields vs interface output_mapping ──
print(f"\n\n3. FIELD-LEVEL COMPARISON (schema fields vs interface output_mapping)\n")
print(f"{'Table':30s} {'Schema Fields':>5s} {'Iface Outputs':>5s} {'Mismatches':>10s}")
print("-" * 80)

for table_name, (iface_name, func) in sorted(tables_with_interface.items()):
    table_def = schema_tables[table_name]
    iface_def = interfaces.get(iface_name)
    if not iface_def:
        continue
    
    schema_fields = set(table_def.schema.keys())
    
    # Collect all output fields from all enabled sources
    all_output_fields = set()
    output_mappings = []
    for src in iface_def.get("sources", []):
        if not src.get("enabled", True):
            continue
        om = src.get("output_mapping", {})
        if om:
            output_mappings.append(om)
            all_output_fields.update(om.values())
    
    # Also check the top-level output definition
    top_output = iface_def.get("output", [])
    top_output_fields = set(item["name"] for item in top_output if isinstance(item, dict))
    all_output_fields = all_output_fields | top_output_fields
    
    # Find schema fields NOT in any output
    missing_from_akshare = schema_fields - all_output_fields
    extra_in_iface = all_output_fields - schema_fields
    
    mismatches = ""
    if missing_from_akshare:
        mismatches += f"MISSING:{','.join(sorted(missing_from_akshare)[:5])}"
    if extra_in_iface:
        if mismatches:
            mismatches += " | "
        mismatches += f"EXTRA:{','.join(sorted(extra_in_iface)[:5])}"
    
    print(f"{table_name:30s} {len(schema_fields):>5d} {len(all_output_fields):>5d} {mismatches or 'OK':>10s}")

# ── Part 4: Compare schema.py vs schemas.yaml differences ──
print(f"\n\n4. SCHEMA.PY vs SCHEMAS.YAML COMPARISON\n")
print(f"{'Table':30s} {'In .py':>6s} {'In .yaml':>6s} {'Py Fields':>10s} {'Yaml Fields':>10s} {'Diff?'}")
print("-" * 100)

for table_name in sorted(set(list(schema_tables.keys()) + list(schemas_yaml_data.get("tables", {}).keys()))):
    py_def = schema_tables.get(table_name)
    yaml_def = schemas_yaml_data.get("tables", {}).get(table_name)
    
    in_py = "YES" if py_def else "NO"
    in_yaml = "YES" if yaml_def else "NO"
    
    py_fields = len(py_def.schema) if py_def else 0
    yaml_fields = len(yaml_def.get("schema", {})) if yaml_def else 0
    
    diff = ""
    if py_def and yaml_def:
        py_set = set(py_def.schema.keys())
        yaml_set = set(yaml_def.get("schema", {}).keys())
        if py_set != yaml_set:
            only_py = py_set - yaml_set
            only_yaml = yaml_set - py_set
            parts = []
            if only_py:
                parts.append(f"PY_ONLY:{','.join(sorted(only_py)[:5])}")
            if only_yaml:
                parts.append(f"YAML_ONLY:{','.join(sorted(only_yaml)[:5])}")
            diff = " | ".join(parts)
        elif py_def.ttl_hours != yaml_def.get("ttl_hours"):
            diff = f"TTL: {py_def.ttl_hours} vs {yaml_def.get('ttl_hours')}"
        elif py_def.primary_key != yaml_def.get("primary_key"):
            diff = f"PK: {py_def.primary_key} vs {yaml_def.get('primary_key')}"
    
    print(f"{table_name:30s} {in_py:>6s} {in_yaml:>6s} {py_fields:>10d} {yaml_fields:>10d} {diff or 'MATCH':>10s}")

# ── Part 5: Verify actual akShare output for key tables ──
print(f"\n\n5. ACTUAL AKSHARE OUTPUT VERIFICATION (first 20 tables)\n")
print(f"Calling akShare functions to verify actual column output...\n")

import akshare as ak

# Map tables to sample akShare calls
VERIFY_MAP = {
    "stock_daily": lambda: ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20260101", end_date="20260110"),
    "etf_daily": lambda: ak.fund_etf_hist_em(symbol="510050", period="daily", start_date="20260101", end_date="20260110"),
    "index_daily": lambda: ak.index_zh_a_hist(symbol="000001", period="daily", start_date="20260101", end_date="20260110"),
    "spot_snapshot": lambda: ak.stock_zh_a_spot_em(),
    "securities": lambda: ak.stock_info_a_code_name(),
    "money_flow": lambda: ak.stock_individual_fund_flow(stock="000001", market="sh"),
    "trade_calendar": lambda: ak.tool_trade_date_hist_sina(),
    "industry_list": lambda: ak.sw_index_first_info(),
    "north_flow": lambda: ak.stock_hsgt_fund_flow_summary_em(),
    "northbound_holdings": lambda: ak.stock_hsgt_hist_em(symbol="北向资金"),
    "sector_flow_snapshot": lambda: ak.stock_fund_flow_industry(symbol="即时"),
    "futures_daily": lambda: ak.futures_zh_daily_sina(symbol="V0"),
    "conversion_bond_daily": lambda: ak.bond_zh_hs_cov_daily(symbol="sh110044"),
    "block_deal": lambda: ak.stock_fund_flow_big_deal(),
    "shibor_rate": lambda: ak.macro_china_shibor_all(),
    "industry_components": lambda: ak.sw_index_third_cons(symbol="801120.SI"),
    "company_info": lambda: ak.stock_individual_info_em(symbol="000001"),
}

for table_name, fn in VERIFY_MAP.items():
    try:
        df = fn()
        actual_cols = sorted(df.columns.tolist())
        table_def = schema_tables.get(table_name)
        if table_def:
            schema_cols = sorted(table_def.schema.keys())
            missing_from_schema = set(actual_cols) - set(schema_cols)
            missing_from_actual = set(schema_cols) - set(actual_cols)
            
            status = "OK"
            details = []
            if missing_from_schema:
                status = "???"
                details.append(f"NOT_IN_SCHEMA:{sorted(missing_from_schema)[:8]}")
            if missing_from_actual:
                # Some fields are computed (e.g. week, period, adjust) - that's OK
                computed = {"week", "period", "adjust", "is_trading_day", "market_cap", 
                           "circulating_cap", "pe", "pb", "ps", "turnover_rate"}
                real_missing = missing_from_actual - computed
                if real_missing:
                    status = "MISMATCH"
                    details.append(f"NOT_IN_ACTUAL:{sorted(real_missing)[:8]}")
            
            detail_str = " | ".join(details) if details else ""
            print(f"  {table_name:30s} [{status}] actual={len(actual_cols)} cols schema={len(schema_cols)} cols {detail_str}")
        else:
            print(f"  {table_name:30s} [NO_SCHEMA] actual cols: {actual_cols[:10]}")
    except Exception as e:
        print(f"  {table_name:30s} [ERROR] {str(e)[:80]}")

# ── Part 6: Test the 4 problematic tables ──
print(f"\n\n6. PROBLEMATIC TABLES (no schema, actual column output)\n")

PROBLEM_TESTS = {
    "dragon_tiger_summary": lambda: ak.stock_lhb_detail_em(start_date="20260101", end_date="20260123"),
    "limit_up_pool": lambda: ak.stock_zt_pool_em(date="20260123"),
    "limit_down_pool": lambda: ak.stock_zt_pool_dtgc_em(date="20260123"),
    "margin_summary": lambda: ak.stock_margin_sse(start_date="20260101", end_date="20260123"),
    "suspended_stocks": lambda: ak.stock_tfp_em(),
}

for name, fn in PROBLEM_TESTS.items():
    try:
        df = fn()
        print(f"  {name}: cols={sorted(df.columns.tolist())[:15]}, rows={len(df)}")
    except Exception as e:
        print(f"  {name}: [ERROR] {str(e)[:80]}")

print("\n" + "=" * 80)
print("VALIDATION COMPLETE")
print("=" * 80)
