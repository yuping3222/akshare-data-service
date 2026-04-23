"""调用统计分析器 - 读取访问日志，生成下载优先级配置"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from akshare_data.offline.core.paths import paths

logger = logging.getLogger("akshare_data")


class CallStatsAnalyzer:
    """日志分析器，生成下载优先级配置"""

    def __init__(
        self,
        log_dir: Optional[str] = None,
        output_path: Optional[str] = None,
    ):
        self._log_dir = Path(log_dir) if log_dir else paths.logs_dir
        self._output_path = Path(output_path) if output_path else paths.priority_file
        self._output_path.parent.mkdir(parents=True, exist_ok=True)

    def analyze(self, window_days: int = 7) -> Dict:
        """分析最近 N 天的日志"""
        entries = self._read_logs(window_days)
        if not entries:
            logger.warning("No log entries found for the last %d days", window_days)
            return {}

        aggregated = self._aggregate(entries)
        scored = self._score(aggregated)
        ranked = self._rank(scored)
        config = self._build_config(ranked, entries, window_days)
        self._save(config)
        return config

    def _read_logs(self, window_days: int) -> List[Dict]:
        """读取最近 N 天的日志文件"""
        entries = []
        cutoff = datetime.now() - timedelta(days=window_days)

        log_files = list(self._log_dir.glob("access.log*"))
        for log_file in log_files:
            try:
                if log_file.name != "access.log":
                    date_str = log_file.name.replace("access.log.", "")
                    try:
                        file_date = datetime.strptime(date_str, "%Y-%m-%d")
                        if file_date < cutoff:
                            continue
                    except ValueError:
                        pass

                with open(log_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            entry_ts = datetime.fromisoformat(entry.get("ts", ""))
                            if entry_ts >= cutoff:
                                entries.append(entry)
                        except (json.JSONDecodeError, ValueError):
                            continue
            except OSError:
                continue

        return entries

    def _aggregate(self, entries: List[Dict]) -> Dict[str, Dict]:
        """按 interface + symbol 聚合统计"""
        aggregated = {}
        for entry in entries:
            interface = entry.get("interface", "unknown")
            symbol = entry.get("symbol", "unknown")
            key = f"{interface}:{symbol}"

            if key not in aggregated:
                aggregated[key] = {
                    "interface": interface,
                    "symbol": symbol,
                    "call_count": 0,
                    "miss_count": 0,
                    "total_latency": 0.0,
                    "timestamps": [],
                }

            agg = aggregated[key]
            agg["call_count"] += 1
            if not entry.get("cache_hit", False):
                agg["miss_count"] += 1
            agg["total_latency"] += entry.get("latency_ms", 0)
            try:
                agg["timestamps"].append(datetime.fromisoformat(entry["ts"]))
            except (ValueError, KeyError):
                pass

        return aggregated

    def _score(self, aggregated: Dict[str, Dict]) -> Dict[str, Dict]:
        """计算优先级分数"""
        if not aggregated:
            return {}

        max_calls = max(a["call_count"] for a in aggregated.values())
        scored = {}

        for key, agg in aggregated.items():
            call_count_norm = agg["call_count"] / max_calls if max_calls > 0 else 0
            miss_rate = (
                agg["miss_count"] / agg["call_count"] if agg["call_count"] > 0 else 0
            )
            recency = self._calc_recency(agg["timestamps"])

            score = call_count_norm * 0.4 + miss_rate * 0.3 + recency * 0.3
            scored[key] = {
                **agg,
                "score": round(score * 100, 2),
                "call_count_norm": call_count_norm,
                "miss_rate": miss_rate,
                "recency": recency,
                "avg_latency": agg["total_latency"] / agg["call_count"]
                if agg["call_count"] > 0
                else 0,
            }

        return scored

    def _calc_recency(self, timestamps: List[datetime]) -> float:
        """计算时间衰减因子"""
        if not timestamps:
            return 0.0

        now = datetime.now()
        decay_sum = 0.0
        for ts in timestamps:
            days_ago = (now - ts).total_seconds() / 86400
            decay_sum += math.exp(-0.5 * days_ago)

        return decay_sum / len(timestamps)

    def _rank(self, scored: Dict[str, Dict]) -> List[Dict]:
        """按分数排序生成排名"""
        ranked = sorted(scored.values(), key=lambda x: x["score"], reverse=True)
        for i, item in enumerate(ranked):
            item["rank"] = i + 1
        return ranked

    def _build_config(
        self, ranked: List[Dict], entries: List[Dict], window_days: int
    ) -> Dict:
        """构建最终配置"""
        # First pass: collect all symbols per interface and aggregate stats
        iface_data = {}
        for item in ranked:
            interface = item["interface"]
            if interface not in iface_data:
                iface_data[interface] = {
                    "symbols": [],
                    "total_calls": 0,
                    "total_misses": 0,
                    "total_latency": 0.0,
                    "max_score": item["score"],
                }
            data = iface_data[interface]
            data["symbols"].append(
                {
                    "code": item["symbol"],
                    "calls": item["call_count"],
                    "misses": item["miss_count"],
                }
            )
            data["total_calls"] += item["call_count"]
            data["total_misses"] += item["miss_count"]
            data["total_latency"] += item["avg_latency"] * item["call_count"]
            data["max_score"] = max(data["max_score"], item["score"])

        # Build interface-level priorities with aggregated stats
        priorities = {}
        for interface, data in iface_data.items():
            miss_rate = (
                data["total_misses"] / data["total_calls"]
                if data["total_calls"] > 0
                else 0
            )
            avg_latency = (
                data["total_latency"] / data["total_calls"]
                if data["total_calls"] > 0
                else 0
            )
            priorities[interface] = {
                "score": round(data["max_score"], 2),
                "call_count_7d": data["total_calls"],
                "miss_rate_7d": round(miss_rate, 2),
                "avg_latency_ms": round(avg_latency, 2),
                "symbols": data["symbols"],
                "recommendation": self._recommend_strategy(
                    {
                        "miss_rate": miss_rate,
                        "call_count": data["total_calls"],
                        "score": data["max_score"],
                    }
                ),
            }

        # Re-rank interfaces by score
        ranked_ifaces = sorted(
            priorities.items(), key=lambda x: x[1]["score"], reverse=True
        )
        for rank, (iface, data) in enumerate(ranked_ifaces, 1):
            data["rank"] = rank

        total_calls = len(entries)
        total_misses = sum(1 for e in entries if not e.get("cache_hit", False))

        return {
            "generated_at": datetime.now().isoformat(),
            "window": f"{window_days}d",
            "priorities": priorities,
            "global": {
                "total_calls_7d": total_calls,
                "total_misses_7d": total_misses,
                "overall_miss_rate": round(total_misses / total_calls, 2)
                if total_calls > 0
                else 0,
            },
        }

    def _recommend_strategy(self, item: Dict) -> Dict:
        """推荐下载策略"""
        if item["miss_rate"] > 0.5 and item["call_count"] > 50:
            return {"mode": "incremental", "frequency": "hourly"}
        if item["miss_rate"] > 0.3 or item["score"] > 60:
            return {"mode": "incremental", "frequency": "daily", "time": "15:30"}
        if item["score"] > 30:
            return {"mode": "full", "frequency": "weekly"}
        return {"mode": "full", "frequency": "monthly"}

    def _save(self, config: Dict):
        """保存为 YAML 文件"""
        with open(self._output_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        logger.info(f"Saved priority config to {self._output_path}")
