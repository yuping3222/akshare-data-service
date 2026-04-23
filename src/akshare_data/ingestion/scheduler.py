"""Task scheduler for ingestion.

Generates ``ExtractTask`` and ``BatchContext`` instances from YAML schedule
definitions, supporting:

- Calendar-based scheduling (daily, weekly, monthly, custom cron-like)
- Priority levels (P0–P3)
- Partition-aware generation (by symbol, by date range)
- Trade-calendar awareness (skip non-trading days)

The scheduler does **not** execute tasks; it only produces task lists that
conform to the ``ExtractTask`` / ``BatchContext`` models defined in
``ingestion.models``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

from .models import BatchContext, ExtractTask


# ---------------------------------------------------------------------------
# Config paths
# ---------------------------------------------------------------------------

_DEFAULT_SCHEDULE_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "config"
    / "ingestion"
    / "schedules.yaml"
)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


# ---------------------------------------------------------------------------
# Priority
# ---------------------------------------------------------------------------


class Priority(str, Enum):
    P0 = "p0"
    P1 = "p1"
    P2 = "p2"
    P3 = "p3"


# ---------------------------------------------------------------------------
# Schedule frequency
# ---------------------------------------------------------------------------


class Frequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ONCE = "once"
    CUSTOM = "custom"


# ---------------------------------------------------------------------------
# Schedule definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScheduleDef:
    """Parsed schedule entry from YAML config.

    Attributes
    ----------
    name : str
        Schedule identifier.
    dataset : str
        Canonical dataset name (e.g. ``"market_quote_daily"``).
    domain : str
        Data domain (e.g. ``"cn"``, ``"us"``).
    source_name : str
        Preferred source adapter name.
    interface_name : str
        Source-specific interface / function name.
    frequency : Frequency
        How often to generate tasks.
    priority : Priority
        Task priority level.
    partitions : list[str]
        Partition keys to generate tasks for (e.g. symbol lists).
    partition_mode : str
        ``"symbol"`` | ``"date"`` | ``"all"``.
    trading_calendar : bool
        If True, skip non-trading days (requires trade_calendar table).
    time_of_day : str | None
        HH:MM string for daily/weekly schedules.
    day_of_week : int | None
        0=Monday … 6=Sunday (for weekly schedules).
    day_of_month : int | None
        1–31 (for monthly schedules).
    params_template : dict
        Default parameters merged into every generated task.
    enabled : bool
        Whether the schedule is active.
    """

    name: str
    dataset: str
    domain: str
    source_name: str
    interface_name: str
    frequency: Frequency = Frequency.DAILY
    priority: Priority = Priority.P1
    partitions: List[str] = field(default_factory=list)
    partition_mode: str = "symbol"
    trading_calendar: bool = False
    time_of_day: Optional[str] = None
    day_of_week: Optional[int] = None
    day_of_month: Optional[int] = None
    params_template: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class Scheduler:
    """Generates ingestion tasks from schedule configuration.

    Usage::

        scheduler = Scheduler()
        batch = scheduler.generate_batch(extract_date=date.today())
        for task in batch.tasks:
            ...
    """

    def __init__(
        self,
        schedules: Optional[List[ScheduleDef]] = None,
        config_path: Optional[str] = None,
        trade_calendar: Optional[Set[date]] = None,
    ) -> None:
        self._schedules: List[ScheduleDef] = schedules or []
        self._trade_calendar: Optional[Set[date]] = trade_calendar
        self._config_path = config_path

        if not self._schedules:
            self._load_from_config()

    # -- config loading --------------------------------------------------

    def _load_from_config(self) -> None:
        path = Path(self._config_path) if self._config_path else _DEFAULT_SCHEDULE_PATH
        raw = _load_yaml(path)
        schedules_section = raw.get("schedules", raw)
        for name, cfg in schedules_section.items():
            if not isinstance(cfg, dict):
                continue
            freq_str = cfg.get("frequency", "daily")
            try:
                freq = Frequency(freq_str)
            except ValueError:
                freq = Frequency.CUSTOM

            pri_str = cfg.get("priority", "p1")
            try:
                pri = Priority(pri_str)
            except ValueError:
                pri = Priority.P1

            partitions = cfg.get("partitions", [])
            if isinstance(partitions, str):
                partitions = [partitions]

            self._schedules.append(
                ScheduleDef(
                    name=name,
                    dataset=cfg.get("dataset", name),
                    domain=cfg.get("domain", "cn"),
                    source_name=cfg.get("source_name", "akshare"),
                    interface_name=cfg.get("interface_name", ""),
                    frequency=freq,
                    priority=pri,
                    partitions=partitions,
                    partition_mode=cfg.get("partition_mode", "symbol"),
                    trading_calendar=cfg.get("trading_calendar", False),
                    time_of_day=cfg.get("time_of_day"),
                    day_of_week=cfg.get("day_of_week"),
                    day_of_month=cfg.get("day_of_month"),
                    params_template=cfg.get("params_template", {}),
                    enabled=cfg.get("enabled", True),
                )
            )

    # -- batch generation ------------------------------------------------

    def generate_batch(
        self,
        extract_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain_filter: Optional[str] = None,
        priority_filter: Optional[Priority] = None,
        schedule_filter: Optional[Set[str]] = None,
        batch_sequence: int = 1,
    ) -> BatchContext:
        """Generate a batch of tasks for the given date or date range.

        Args
        ----
        extract_date : date
            Single extraction date (used for daily schedules).
        start_date / end_date : date
            Date range for backfill or multi-day generation.
        domain_filter : str
            Only include schedules matching this domain.
        priority_filter : Priority
            Only include schedules at or above this priority.
        schedule_filter : set[str]
            Only include schedules whose names are in this set.
        batch_sequence : int
            Sequence number for batch ID generation.

        Returns
        -------
        BatchContext with all generated tasks.
        """
        target_date = extract_date or date.today()
        dates = self._resolve_dates(target_date, start_date, end_date)

        tasks: List[ExtractTask] = []
        domain = domain_filter or ""

        for sched in self._schedules:
            if not sched.enabled:
                continue
            if domain_filter and sched.domain != domain_filter:
                continue
            if priority_filter and _priority_value(sched.priority) > _priority_value(
                priority_filter
            ):
                continue
            if schedule_filter and sched.name not in schedule_filter:
                continue
            if not self._should_run(sched, target_date):
                continue

            sched_domain = domain or sched.domain
            for d in dates:
                if sched.trading_calendar and not self._is_trading_day(d):
                    continue
                tasks.extend(self._tasks_for_schedule(sched, d, sched_domain))

        batch = BatchContext.new(tasks=tasks, domain=domain or "mixed")
        if not domain and tasks:
            batch = BatchContext.new(tasks=tasks, domain=tasks[0].domain)
        return batch

    def generate_tasks_for_date(
        self,
        extract_date: date,
        domain_filter: Optional[str] = None,
    ) -> List[ExtractTask]:
        """Convenience: return a flat list of tasks for a single date."""
        batch = self.generate_batch(
            extract_date=extract_date,
            domain_filter=domain_filter,
        )
        return batch.tasks

    def generate_backfill_tasks(
        self,
        start_date: date,
        end_date: date,
        dataset: Optional[str] = None,
        domain_filter: Optional[str] = None,
    ) -> BatchContext:
        """Generate tasks for a backfill date range."""
        return self.generate_batch(
            start_date=start_date,
            end_date=end_date,
            domain_filter=domain_filter,
            schedule_filter={s.name for s in self._schedules if s.dataset == dataset}
            if dataset
            else None,
        )

    # -- schedule access -------------------------------------------------

    def list_schedules(self) -> List[ScheduleDef]:
        return list(self._schedules)

    def get_schedule(self, name: str) -> Optional[ScheduleDef]:
        for s in self._schedules:
            if s.name == name:
                return s
        return None

    def set_trade_calendar(self, trading_days: Set[date]) -> None:
        self._trade_calendar = trading_days

    # -- internal --------------------------------------------------------

    def _resolve_dates(
        self,
        target_date: date,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> List[date]:
        if start_date and end_date:
            return _date_range(start_date, end_date)
        return [target_date]

    def _should_run(self, sched: ScheduleDef, target_date: date) -> bool:
        if sched.frequency == Frequency.DAILY:
            return True
        if sched.frequency == Frequency.WEEKLY:
            if sched.day_of_week is not None:
                return target_date.weekday() == sched.day_of_week
            return target_date.weekday() == 0  # default Monday
        if sched.frequency == Frequency.MONTHLY:
            dom = sched.day_of_month or 1
            return target_date.day == dom
        if sched.frequency == Frequency.ONCE:
            return True
        if sched.frequency == Frequency.CUSTOM:
            return True
        return True

    def _is_trading_day(self, d: date) -> bool:
        if self._trade_calendar is None:
            return d.weekday() < 5  # Mon-Fri as fallback
        return d in self._trade_calendar

    def _tasks_for_schedule(
        self,
        sched: ScheduleDef,
        extract_date: date,
        domain: str,
    ) -> List[ExtractTask]:
        if not sched.partitions or sched.partition_mode == "all":
            params = dict(sched.params_template)
            params["extract_date"] = extract_date.isoformat()
            return [
                ExtractTask(
                    domain=domain,
                    dataset=sched.dataset,
                    source_name=sched.source_name,
                    interface_name=sched.interface_name,
                    request_params=params,
                    extract_date=extract_date,
                )
            ]

        tasks: List[ExtractTask] = []
        for partition in sched.partitions:
            params = dict(sched.params_template)
            if sched.partition_mode == "symbol":
                params["symbol"] = partition
            params["extract_date"] = extract_date.isoformat()
            tasks.append(
                ExtractTask(
                    domain=domain,
                    dataset=sched.dataset,
                    source_name=sched.source_name,
                    interface_name=sched.interface_name,
                    request_params=params,
                    extract_date=extract_date,
                )
            )
        return tasks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _priority_value(p: Priority) -> int:
    mapping = {Priority.P0: 0, Priority.P1: 1, Priority.P2: 2, Priority.P3: 3}
    return mapping.get(p, 3)


def _date_range(start: date, end: date) -> List[date]:
    result: List[date] = []
    current = start
    while current <= end:
        result.append(current)
        current += timedelta(days=1)
    return result
