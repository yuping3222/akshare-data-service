"""Field lineage tracking for source-to-standard field mappings.

Provides the ability to trace how source fields map to standard entity fields,
with full version tracking (batch_id, schema_version, normalize_version,
release_version).

Integration points reserved for:
- Task 15: mapping configuration loading
- Task 07: release manifest generation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FieldLineage:
    """Lineage record for a single field transformation.

    Attributes:
        dataset: Target dataset name (standard entity name).
        standard_field: Standard field name in the target entity.
        source_name: Source system name (e.g. lixinger, akshare).
        source_field: Original field name in the source.
        interface_name: Source interface/endpoint name.
        transform: Transformation description (e.g. 'direct', 'rename', 'cast').
        batch_id: Batch identifier for this lineage record.
        schema_version: Entity schema version at time of mapping.
        normalize_version: Normalization rule version applied.
        release_version: Release version if published, else None.
        created_at: Timestamp when this lineage was recorded.
    """

    dataset: str
    standard_field: str
    source_name: str
    source_field: str
    interface_name: str
    transform: str = "direct"
    batch_id: str = ""
    schema_version: str = ""
    normalize_version: str = ""
    release_version: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class LineageEdge:
    """A single edge in the lineage graph.

    Represents one source field contributing to one standard field.
    """

    source_node: str
    target_node: str
    lineage: FieldLineage


class LineageGraph:
    """Directed graph of field-level lineage.

    Nodes are field identifiers (e.g. 'akshare:stock_zh_a_daily:close',
    'market_quote_daily:close_price'). Edges carry FieldLineage metadata.
    """

    def __init__(self) -> None:
        self._edges: list[LineageEdge] = []
        self._forward_index: dict[str, list[LineageEdge]] = {}
        self._reverse_index: dict[str, list[LineageEdge]] = {}

    def add(self, lineage: FieldLineage) -> None:
        """Add a lineage edge to the graph.

        Args:
            lineage: FieldLineage record to add.
        """
        source_node = (
            f"{lineage.source_name}:{lineage.interface_name}:{lineage.source_field}"
        )
        target_node = f"{lineage.dataset}:{lineage.standard_field}"

        edge = LineageEdge(
            source_node=source_node, target_node=target_node, lineage=lineage
        )
        self._edges.append(edge)

        self._forward_index.setdefault(source_node, []).append(edge)
        self._reverse_index.setdefault(target_node, []).append(edge)

    def get_sources_for(self, dataset: str, standard_field: str) -> list[FieldLineage]:
        """Find all source fields that contribute to a standard field.

        Args:
            dataset: Target dataset name.
            standard_field: Standard field name.

        Returns:
            List of FieldLineage records, ordered by creation time.
        """
        target_node = f"{dataset}:{standard_field}"
        edges = self._reverse_index.get(target_node, [])
        return sorted(
            [e.lineage for e in edges],
            key=lambda x: x.created_at,
        )

    def get_targets_for(
        self, source_name: str, source_field: str
    ) -> list[FieldLineage]:
        """Find all standard fields that a source field feeds into.

        Args:
            source_name: Source system name.
            source_field: Source field name.

        Returns:
            List of FieldLineage records.
        """
        edges = []
        for edge_list in self._forward_index.values():
            for edge in edge_list:
                if (
                    edge.lineage.source_name == source_name
                    and edge.lineage.source_field == source_field
                ):
                    edges.append(edge.lineage)
        return edges

    def get_all_for_dataset(self, dataset: str) -> list[FieldLineage]:
        """Get all lineage records for a dataset.

        Args:
            dataset: Target dataset name.

        Returns:
            List of FieldLineage records for the dataset.
        """
        return [edge.lineage for edge in self._edges if edge.lineage.dataset == dataset]

    def get_all_for_batch(self, batch_id: str) -> list[FieldLineage]:
        """Get all lineage records for a specific batch.

        Args:
            batch_id: Batch identifier.

        Returns:
            List of FieldLineage records for the batch.
        """
        return [
            edge.lineage for edge in self._edges if edge.lineage.batch_id == batch_id
        ]

    def list_datasets(self) -> list[str]:
        """List all datasets that have lineage records."""
        return sorted({edge.lineage.dataset for edge in self._edges})

    def list_sources(self) -> list[str]:
        """List all source systems that have lineage records."""
        return sorted({edge.lineage.source_name for edge in self._edges})

    def edge_count(self) -> int:
        """Return total number of lineage edges."""
        return len(self._edges)


class LineageTracker:
    """High-level tracker for field lineage across datasets.

    Manages a LineageGraph and provides convenience methods for
    recording and querying lineage with version tracking.

    Integration points:
    - load_mapping_config(): reserved for task 15 mapping configuration
    - build_release_manifest(): reserved for task 07 release manifest
    """

    def __init__(self) -> None:
        self._graph = LineageGraph()

    @property
    def graph(self) -> LineageGraph:
        """Access the underlying lineage graph."""
        return self._graph

    def record(
        self,
        dataset: str,
        standard_field: str,
        source_name: str,
        source_field: str,
        interface_name: str,
        transform: str = "direct",
        batch_id: str = "",
        schema_version: str = "",
        normalize_version: str = "",
        release_version: str | None = None,
    ) -> FieldLineage:
        """Record a field lineage mapping.

        Args:
            dataset: Target dataset name.
            standard_field: Standard field name.
            source_name: Source system name.
            source_field: Source field name.
            interface_name: Source interface name.
            transform: Transformation type.
            batch_id: Batch identifier.
            schema_version: Schema version.
            normalize_version: Normalize version.
            release_version: Release version if published.

        Returns:
            The created FieldLineage record.
        """
        lineage = FieldLineage(
            dataset=dataset,
            standard_field=standard_field,
            source_name=source_name,
            source_field=source_field,
            interface_name=interface_name,
            transform=transform,
            batch_id=batch_id,
            schema_version=schema_version,
            normalize_version=normalize_version,
            release_version=release_version,
        )
        self._graph.add(lineage)
        return lineage

    def record_batch(
        self,
        dataset: str,
        mappings: list[dict[str, str]],
        batch_id: str = "",
        schema_version: str = "",
        normalize_version: str = "",
        release_version: str | None = None,
    ) -> list[FieldLineage]:
        """Record multiple field lineage mappings at once.

        Args:
            dataset: Target dataset name.
            mappings: List of dicts with keys: standard_field, source_field,
                      source_name, interface_name, transform (optional).
            batch_id: Batch identifier.
            schema_version: Schema version.
            normalize_version: Normalize version.
            release_version: Release version if published.

        Returns:
            List of created FieldLineage records.
        """
        results = []
        for m in mappings:
            lineage = self.record(
                dataset=dataset,
                standard_field=m["standard_field"],
                source_name=m.get("source_name", "unknown"),
                source_field=m["source_field"],
                interface_name=m.get("interface_name", ""),
                transform=m.get("transform", "direct"),
                batch_id=batch_id,
                schema_version=schema_version,
                normalize_version=normalize_version,
                release_version=release_version,
            )
            results.append(lineage)
        return results

    def get_field_sources(
        self, dataset: str, standard_field: str
    ) -> list[FieldLineage]:
        """Get all source fields for a standard field.

        Args:
            dataset: Target dataset name.
            standard_field: Standard field name.

        Returns:
            List of FieldLineage records showing field origins.
        """
        return self._graph.get_sources_for(dataset, standard_field)

    def get_dataset_lineage(self, dataset: str) -> list[FieldLineage]:
        """Get all lineage records for a dataset."""
        return self._graph.get_all_for_dataset(dataset)

    def get_batch_lineage(self, batch_id: str) -> list[FieldLineage]:
        """Get all lineage records for a batch."""
        return self._graph.get_all_for_batch(batch_id)

    def get_field_versions(
        self, dataset: str, standard_field: str
    ) -> list[dict[str, Any]]:
        """Get version history for a specific field.

        Returns a list of dicts with batch_id, schema_version,
        normalize_version, release_version, and source info.
        """
        sources = self._graph.get_sources_for(dataset, standard_field)
        return [
            {
                "batch_id": s.batch_id,
                "schema_version": s.schema_version,
                "normalize_version": s.normalize_version,
                "release_version": s.release_version,
                "source_name": s.source_name,
                "source_field": s.source_field,
                "transform": s.transform,
                "created_at": s.created_at.isoformat(),
            }
            for s in sources
        ]

    # ------------------------------------------------------------------
    # Integration points for future tasks
    # ------------------------------------------------------------------

    def load_mapping_config(self, config_path: str) -> int:
        """Load field mappings from a YAML configuration file.

        Reserved for task 15: mapping configuration.

        Expected YAML structure:
            dataset: market_quote_daily
            batch_id: "20260422_001"
            mappings:
              - standard_field: close_price
                source_name: akshare
                source_field: close
                interface_name: stock_zh_a_daily
                transform: rename

        Args:
            config_path: Path to the YAML mapping config file.

        Returns:
            Number of mappings loaded.

        Raises:
            NotImplementedError: Until task 15 is implemented.
        """
        raise NotImplementedError(
            "load_mapping_config is reserved for task 15 (mapping configuration)"
        )

    def build_release_manifest(
        self,
        dataset: str,
        release_version: str,
        batch_id: str,
    ) -> dict[str, Any]:
        """Build a release manifest for a dataset version.

        Reserved for task 07: release manifest.

        The manifest will include:
        - release_version, batch_id, schema_version
        - Field-level lineage summary
        - Quality gate status (from task 06)
        - Record counts and date ranges

        Args:
            dataset: Target dataset name.
            release_version: Release version string.
            batch_id: Batch identifier.

        Returns:
            Manifest dictionary.

        Raises:
            NotImplementedError: Until task 07 is implemented.
        """
        raise NotImplementedError(
            "build_release_manifest is reserved for task 07 (release manifest)"
        )
