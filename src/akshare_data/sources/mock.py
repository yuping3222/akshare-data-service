"""Mock data source — backward compatibility shim.

Re-exports from ``ingestion.adapters.mock``.  New code should import
from ``akshare_data.ingestion.adapters.mock``.
"""

from akshare_data.ingestion.adapters.mock import MockAdapter

# Also expose as MockSource for legacy callers
MockSource = MockAdapter

__all__ = ["MockAdapter", "MockSource"]
