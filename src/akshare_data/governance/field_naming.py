"""Field naming governance helpers (T4-002).

Provides a single place to validate canonical field names and detect
legacy aliases that should be normalized before entering standardized/served.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")

LEGACY_CANONICAL_FORBIDDEN = {
    "symbol",
    "code",
    "ts_code",
    "date",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "amount",
    "turnover",
    "pct_chg",
    "pct_change",
    "vol",
}

RECOMMENDED_SUFFIXES = {
    "_id",
    "_code",
    "_name",
    "_date",
    "_time",
    "_pct",
    "_amount",
    "_volume",
    "_flag",
    "_status",
    "_version",
}


@dataclass(frozen=True)
class FieldNameValidation:
    field_name: str
    valid: bool
    reasons: list[str]


class FieldNamingStandard:
    """Validation/normalization helper for canonical field naming."""

    def validate(self, field_name: str) -> FieldNameValidation:
        reasons: list[str] = []

        if not _SNAKE_CASE_RE.match(field_name):
            reasons.append("field must be snake_case")

        if field_name in LEGACY_CANONICAL_FORBIDDEN:
            reasons.append("legacy name cannot be canonical")

        return FieldNameValidation(
            field_name=field_name,
            valid=(len(reasons) == 0),
            reasons=reasons,
        )

    def is_recommended_suffix(self, field_name: str) -> bool:
        """Whether the field ends with one of recommended business suffixes."""
        return any(field_name.endswith(suffix) for suffix in RECOMMENDED_SUFFIXES)
