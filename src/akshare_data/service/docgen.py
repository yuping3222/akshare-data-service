"""Documentation generator bound to field dictionary and entity schemas.

Generates structured API documentation from:
- config/standards/entities/*.yaml
- config/standards/field_dictionary.yaml

No hand-written field comments; all descriptions come from the standard
field dictionary and entity schema definitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

from akshare_data.service.query_contract import (
    SYSTEM_FIELDS,
    DatasetContract,
    get_contract,
    list_contracts,
)


# ---------------------------------------------------------------------------
# Field dictionary loader
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldDictEntry:
    name: str
    field_type: str
    description: str
    unit: Optional[str] = None
    aliases: List[str] = field(default_factory=list)


def _load_field_dictionary() -> Dict[str, FieldDictEntry]:
    import os

    pkg_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    project_root = os.path.dirname(pkg_dir)
    filepath = os.path.join(
        project_root, "config", "standards", "field_dictionary.yaml"
    )

    if not os.path.exists(filepath):
        return {}

    with open(filepath, encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    entries: Dict[str, FieldDictEntry] = {}

    for fname, fdef in (doc.get("fields") or {}).items():
        entries[fname] = FieldDictEntry(
            name=fname,
            field_type=fdef.get("type", "string"),
            description=fdef.get("description", ""),
            unit=fdef.get("unit"),
            aliases=fdef.get("aliases", []) or [],
        )

    return entries


FIELD_DICT: Dict[str, FieldDictEntry] = _load_field_dictionary()


# ---------------------------------------------------------------------------
# Document models
# ---------------------------------------------------------------------------


@dataclass
class FieldDoc:
    name: str
    field_type: str
    description: str
    unit: Optional[str]
    required: bool
    is_system: bool


@dataclass
class ParamDoc:
    name: str
    param_type: str
    required: bool
    default: Any
    description: str


@dataclass
class DatasetDoc:
    dataset: str
    entity: str
    description: str
    primary_key: List[str]
    partition_by: List[str]
    fields: List[FieldDoc]
    query_params: List[ParamDoc]
    default_sort_field: str


# ---------------------------------------------------------------------------
# Document generator
# ---------------------------------------------------------------------------


class DocGenerator:
    """Generates documentation bound to field dictionary and entity schemas."""

    def generate_dataset_doc(self, dataset: str) -> DatasetDoc:
        contract = get_contract(dataset)
        return self._build_dataset_doc(contract)

    def generate_all_docs(self) -> List[DatasetDoc]:
        return [self.generate_dataset_doc(ds) for ds in list_contracts()]

    def generate_markdown(self, dataset: str) -> str:
        doc = self.generate_dataset_doc(dataset)
        return self._render_markdown(doc)

    def generate_all_markdown(self) -> str:
        docs = self.generate_all_docs()
        sections = [self._render_markdown(d) for d in docs]
        return "\n\n---\n\n".join(sections)

    def _build_dataset_doc(self, contract: DatasetContract) -> DatasetDoc:
        schema = contract.schema
        fields = []

        for fname, fdef in schema.fields.items():
            dict_entry = FIELD_DICT.get(fname)
            fields.append(
                FieldDoc(
                    name=fname,
                    field_type=fdef.field_type,
                    description=dict_entry.description
                    if dict_entry
                    else fdef.description,
                    unit=dict_entry.unit if dict_entry else fdef.unit,
                    required=fname in schema.required_fields,
                    is_system=fname in SYSTEM_FIELDS,
                )
            )

        params = self._build_param_docs(contract)

        return DatasetDoc(
            dataset=contract.dataset,
            entity=contract.entity,
            description=schema.description,
            primary_key=schema.primary_key,
            partition_by=schema.partition_by,
            fields=fields,
            query_params=params,
            default_sort_field=schema.default_sort_field,
        )

    def _build_param_docs(self, contract: DatasetContract) -> List[ParamDoc]:
        params_cls = contract.params_class
        param_docs: List[ParamDoc] = []

        for fname, f in params_cls.__dataclass_fields__.items():
            if fname in (
                "start_date",
                "end_date",
                "fields",
                "sort_by",
                "sort_order",
                "limit",
                "offset",
                "release_version",
            ):
                continue

            is_required = fname in contract.required_params
            default = f.default if f.default is not f.default_factory else None
            if isinstance(default, type) and hasattr(default, "__name__"):
                default = None

            param_docs.append(
                ParamDoc(
                    name=fname,
                    param_type=f.type if isinstance(f.type, str) else str(f.type),
                    required=is_required,
                    default=default,
                    description=self._param_description(fname),
                )
            )

        base_params = [
            ParamDoc(
                "start_date", "str", False, None, "Start date (inclusive), YYYY-MM-DD"
            ),
            ParamDoc(
                "end_date", "str", False, None, "End date (inclusive), YYYY-MM-DD"
            ),
            ParamDoc(
                "fields",
                "list[str]",
                False,
                None,
                "Field projection; None = all business fields",
            ),
            ParamDoc(
                "sort_by",
                "str",
                False,
                contract.schema.default_sort_field,
                "Sort field name",
            ),
            ParamDoc("sort_order", "str", False, "asc", "Sort direction: asc/desc"),
            ParamDoc("limit", "int", False, None, "Max rows to return (max 10000)"),
            ParamDoc("offset", "int", False, 0, "Pagination offset"),
            ParamDoc(
                "release_version",
                "str",
                False,
                None,
                "Release version; None = latest stable",
            ),
        ]

        return param_docs + base_params

    def _param_description(self, param_name: str) -> str:
        descriptions = {
            "security_id": "Security identifier",
            "adjust_type": "Adjustment type: qfq/hfq/none",
            "report_type": "Report type: Q1/H1/Q3/A; None = all",
            "indicator_code": "Macro indicator code",
            "region": "Region code (default: CN)",
        }
        return descriptions.get(param_name, "")

    def _render_markdown(self, doc: DatasetDoc) -> str:
        lines: List[str] = []

        lines.append(f"## `{doc.dataset}`")
        lines.append("")
        lines.append(f"**Entity**: `{doc.entity}`")
        lines.append("")
        lines.append(f"{doc.description}")
        lines.append("")

        lines.append("### Primary Key")
        lines.append("")
        for pk in doc.primary_key:
            lines.append(f"- `{pk}`")
        lines.append("")

        lines.append("### Partition")
        lines.append("")
        for p in doc.partition_by:
            lines.append(f"- `{p}`")
        lines.append("")

        lines.append("### Query Parameters")
        lines.append("")
        lines.append("| Parameter | Type | Required | Default | Description |")
        lines.append("|-----------|------|----------|---------|-------------|")
        for p in doc.query_params:
            req = "Yes" if p.required else "No"
            default = repr(p.default) if p.default is not None else "None"
            lines.append(
                f"| `{p.name}` | {p.param_type} | {req} | {default} | {p.description} |"
            )
        lines.append("")

        lines.append("### Fields")
        lines.append("")
        lines.append("| Field | Type | Required | Unit | Description |")
        lines.append("|-------|------|----------|------|-------------|")
        for f in doc.fields:
            if f.is_system:
                continue
            req = "Yes" if f.required else "No"
            unit = f.unit or "-"
            lines.append(
                f"| `{f.name}` | {f.field_type} | {req} | {unit} | {f.description} |"
            )
        lines.append("")

        lines.append("### System Fields")
        lines.append("")
        lines.append(
            "System fields are not returned by default. Request them explicitly via `fields`."
        )
        lines.append("")
        lines.append("| Field | Type | Description |")
        lines.append("|-------|------|-------------|")
        for f in doc.fields:
            if not f.is_system:
                continue
            lines.append(f"| `{f.name}` | {f.field_type} | {f.description} |")
        lines.append("")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------


def generate_doc(dataset: str) -> DatasetDoc:
    return DocGenerator().generate_dataset_doc(dataset)


def generate_markdown(dataset: str) -> str:
    return DocGenerator().generate_markdown(dataset)


def generate_all_markdown() -> str:
    return DocGenerator().generate_all_markdown()
