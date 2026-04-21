import yaml
from pathlib import Path

# Check interfaces from config/interfaces/*.yaml
interfaces_dir = Path('config/interfaces')
manual_interfaces = {}
for yf in interfaces_dir.glob('*.yaml'):
    with open(yf) as f:
        data = yaml.safe_load(f) or {}
        manual_interfaces.update(data)

print('=== config/interfaces/*.yaml ===')
print(f'Total interfaces: {len(manual_interfaces)}')
for name, cfg in manual_interfaces.items():
    sources = cfg.get('sources', [])
    source_names = [s.get('name','?') for s in sources]
    rate_key = cfg.get('rate_limit_key', 'default')
    print(f'  {name}: sources={source_names}, rate_key={rate_key}')

# Check akshare_registry
with open('config/akshare_registry.yaml') as f:
    registry = yaml.safe_load(f) or {}
interfaces_reg = registry.get('interfaces', {})
count_with_sources = sum(1 for v in interfaces_reg.values() if v.get('sources'))
print()
print(f'=== akshare_registry.yaml ===')
print(f'Total in registry: {len(interfaces_reg)}')
print(f'With sources: {count_with_sources}')
print(f'Without sources (raw akshare): {len(interfaces_reg) - count_with_sources}')

# Check rate_limits.yaml
print()
print('=== config/rate_limits.yaml ===')
with open('config/rate_limits.yaml') as f:
    rate_limits = yaml.safe_load(f) or {}
for k, v in rate_limits.items():
    print(f'  {k}: {v}')

# Check domains.yaml
print()
print('=== config/sources/domains.yaml ===')
with open('config/sources/domains.yaml') as f:
    domains = yaml.safe_load(f) or {}
for k, v in domains.get('domains', {}).items():
    print(f'  {k}: {v.get("url_pattern", "N/A")}')

# Check source names used in interfaces/*.yaml
print()
print('=== Source names used in interfaces/*.yaml ===')
source_names = set()
for cfg in manual_interfaces.values():
    for s in cfg.get('sources', []):
        source_names.add(s.get('name', '?'))
print(f'  {sorted(source_names)}')