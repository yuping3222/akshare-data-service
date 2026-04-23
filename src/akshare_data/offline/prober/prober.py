"""接口探测器 - 主调度器"""

from __future__ import annotations

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import yaml

from akshare_data.offline.core.paths import paths
from akshare_data.offline.prober.checkpoint import CheckpointManager, ProbeResult
from akshare_data.offline.prober.samples import SampleManager
from akshare_data.offline.prober.task_builder import TaskBuilder, ValidationResult
from akshare_data.offline.prober.executor import TaskExecutor

logger = logging.getLogger("akshare_data")

MAX_WORKERS = 64
DOMAIN_CONCURRENCY_DEFAULT = 3
DELAY_BETWEEN_CALLS = 1.0
DEFAULT_STABLE_TTL = 30 * 24 * 3600
TIMEOUT_LIMIT = 20


class APIProber:
    """接口探测器"""

    def __init__(self, mode: str = "run"):
        paths.ensure_dirs()
        self.mode = mode
        self.results: Dict[str, ValidationResult] = {}
        self.domain_semaphores: Dict[str, threading.Semaphore] = {}
        self.total_elapsed = 0.0

        self.checkpoint_mgr = CheckpointManager()
        self.sample_mgr = SampleManager()
        self.task_builder = TaskBuilder()
        self.executor = TaskExecutor()

        self._load_config()
        self._init_semaphores()

    def _load_config(self):
        """加载探测配置"""
        self.config = {}
        registry_file = paths.legacy_registry_file
        if not registry_file.exists():
            return

        try:
            with open(registry_file, "r", encoding="utf-8") as f:
                registry = yaml.safe_load(f) or {}

            for func_name, iface in registry.get("interfaces", {}).items():
                probe = iface.get("probe", {})
                if probe:
                    self.config[func_name] = {
                        "params": probe.get("params", {}),
                        "skip": probe.get("skip", False),
                        "check_interval": probe.get(
                            "check_interval", DEFAULT_STABLE_TTL
                        ),
                    }
            logger.info(f"Loaded probe config: {len(self.config)} entries")
        except Exception as e:
            logger.error(f"Config load error: {e}")

    def _init_semaphores(self):
        """初始化域名信号量"""
        if self.config:
            domains = set()
            for func_name, cfg in self.config.items():
                domains.add(func_name.split("_")[0] if "_" in func_name else "default")
            for domain in domains:
                self.domain_semaphores[domain] = threading.Semaphore(
                    DOMAIN_CONCURRENCY_DEFAULT
                )

    def run_check(self) -> Dict[str, ValidationResult]:
        """运行健康检查"""
        start_time = time.time()
        tasks = self.task_builder.build_tasks(self.config)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            for task in tasks:
                future = executor.submit(self._run_single_task, task)
                futures[future] = task

            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    if result:
                        self.results[task.func_name] = result
                        self.checkpoint_mgr.set_result(
                            ProbeResult(
                                func_name=result.func_name,
                                domain_group=result.domain_group,
                                status=result.status,
                                error_msg=result.error_msg,
                                exec_time=result.exec_time,
                                data_size=result.data_size,
                                last_check=time.time(),
                                check_count=1,
                            )
                        )
                except Exception as e:
                    logger.error(f"Task {task.func_name} failed: {e}")

        self.total_elapsed = time.time() - start_time
        self.checkpoint_mgr.save()
        logger.info(f"Probe completed in {self.total_elapsed:.2f}s")
        return self.results

    def _run_single_task(self, task) -> Optional[ValidationResult]:
        """运行单个探测任务"""
        if self.checkpoint_mgr.should_skip(task.func_name, task.ttl):
            logger.debug(f"Skipping {task.func_name} (TTL fresh)")
            return None

        domain = task.func_name.split("_")[0] if "_" in task.func_name else "default"
        sem = self.domain_semaphores.get(
            domain, threading.Semaphore(DOMAIN_CONCURRENCY_DEFAULT)
        )

        with sem:
            result = self.executor.execute(task)
            if result and result.data_size > 0:
                self.sample_mgr.save_sample(task.func_name, result.data)

        time.sleep(DELAY_BETWEEN_CALLS)
        return result

    def get_results(self) -> Dict[str, ValidationResult]:
        """获取所有结果"""
        return self.results

    def get_summary(self) -> Dict[str, Any]:
        """获取摘要统计"""
        checkpoint_results = self.checkpoint_mgr.get_all_results()
        if not self.results and not checkpoint_results:
            return {"total": 0, "success": 0, "failed": 0, "rate": 0.0}

        if self.results:
            total = len(self.results)
            success = sum(
                1 for r in self.results.values() if r.status.startswith("Success")
            )
        else:
            total = len(checkpoint_results)
            success = sum(
                1
                for r in checkpoint_results.values()
                if r.get("status", "").startswith("Success")
            )

        return {
            "total": total,
            "success": success,
            "failed": total - success,
            "rate": (success / total * 100) if total > 0 else 0.0,
            "elapsed": self.total_elapsed,
        }

    def get_rolling_date_range(self, days: int = 30) -> Tuple[str, str]:
        """获取滚动日期范围"""
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        return start, end

    def parse_params_from_doc(self, func: Callable) -> Dict[str, Any]:
        """从文档字符串解析参数"""
        import re

        params = {}
        doc = func.__doc__ or ""
        for match in re.finditer(r"(\w+)=['\"]([^'\"]*)['\"]", doc):
            params[match.group(1)] = match.group(2)
        return params

    def get_smart_kwargs(self, func: Callable) -> Dict[str, Any]:
        """从配置获取智能参数"""
        from akshare_data.offline.scanner.param_inferrer import SIZE_LIMIT_PARAMS

        func_name = func.__name__
        kwargs = {}

        if func_name in self.config:
            cfg = self.config[func_name]
            kwargs.update(cfg.get("params", {}))

        sig_params = {}
        try:
            import inspect

            sig = inspect.signature(func)
            sig_params = {
                p.name: p.default
                for p in sig.parameters.values()
                if p.default is not inspect.Parameter.empty
            }
        except (ValueError, TypeError):
            pass

        for param_name, default_value in sig_params.items():
            if param_name not in kwargs:
                if param_name in SIZE_LIMIT_PARAMS:
                    kwargs[param_name] = 1
                else:
                    kwargs[param_name] = default_value

        return kwargs

    def get_website_group(self, func: Callable) -> str:
        """从源码提取域名"""
        import re

        source = ""
        try:
            import inspect

            source = inspect.getsource(func)
        except (OSError, TypeError):
            pass

        urls = re.findall(r"https?://([^/]+)", source)
        return urls[0] if urls else "unknown"

    def discover_interfaces(self) -> List[Callable]:
        """发现所有接口函数"""
        funcs = []
        for name in dir(ak):
            if not name.startswith("_"):
                obj = getattr(ak, name, None)
                if callable(obj):
                    funcs.append(obj)
        return funcs

    def should_skip(self, func_name: str) -> Tuple[bool, str]:
        """检查是否应跳过"""
        if func_name in self.config and self.config[func_name].get("skip"):
            return True, "Manual Skip"

        if func_name in self.results:
            result = self.results[func_name]
            if result.status.startswith("Success"):
                last_check = result.last_check
                if last_check > 0 and (time.time() - last_check) < DEFAULT_STABLE_TTL:
                    return True, "TTL Fresh"

        return False, ""

    def run_single_task(self, func: Callable, domain: str = "unknown"):
        """运行单个探测任务"""
        self.domain_semaphores[domain] = self.domain_semaphores.get(
            domain, threading.Semaphore(DOMAIN_CONCURRENCY_DEFAULT)
        )
        from akshare_data.offline.prober.task_builder import ProbeTask

        task = ProbeTask(
            func_name=func.__name__,
            func=func,
            params=self.get_smart_kwargs(func),
            ttl=DEFAULT_STABLE_TTL,
            skip=False,
        )
        result = self.executor.execute(task)
        if result:
            self.results[func.__name__] = result

    def generate_report(self):
        """生成健康报告"""
        from akshare_data.offline.prober import BASE_DIR

        report_path = Path(BASE_DIR) / "reports" / "health_report.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.results:
            report_path.write_text("# Health Report\n\nNo results available.\n")
            return

        lines = ["# Akshare Health Audit", "", f"Total: {len(self.results)}", ""]
        lines.append("| Func | Domain | Status | Time |")
        lines.append("|------|--------|--------|------|")
        for name, result in self.results.items():
            lines.append(
                f"| {name} | {result.domain_group} | {result.status} | {result.exec_time:.2f}s |"
            )

        report_path.write_text("\n".join(lines))

    def integrate_with_summary(self, total: int, success: int, elapsed: float):
        """集成摘要"""
        pass

    def to_md(self, df: pd.DataFrame) -> str:
        """DataFrame转Markdown"""
        md = df.to_markdown()
        return md.replace("|    | ", "|")

    def _save_checkpoint(self):
        """保存检查点"""
        self.checkpoint_mgr.save()

    def call_with_retry(
        self, func: Callable, kwargs: Dict[str, Any]
    ) -> Tuple[Optional[Any], str]:
        """带重试的调用"""
        return self.executor._call_with_retry(func, kwargs)

    def generate_full_config(self):
        """生成完整探测配置"""
        from akshare_data.offline.prober import BASE_DIR

        config_path = Path(BASE_DIR) / "config" / "health_config_generated.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)

        funcs = self.discover_interfaces()
        config = {}
        for func in funcs[:10]:
            func_name = getattr(func, "__name__", str(func))
            config[func_name] = {
                "params": self.get_smart_kwargs(func),
                "skip": False,
            }

        import json

        config_path.write_text(json.dumps(config, indent=2))
