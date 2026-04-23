"""统一 CLI 入口"""

from __future__ import annotations

import argparse
import logging
import sys

from akshare_data.offline.core.paths import paths

logger = logging.getLogger("akshare_data")


def main():
    """CLI 主入口"""
    parser = argparse.ArgumentParser(
        prog="offline",
        description="AkShare Data Service - Offline Tools",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    _add_download_parser(subparsers)
    _add_probe_parser(subparsers)
    _add_analyze_parser(subparsers)
    _add_report_parser(subparsers)
    _add_config_parser(subparsers)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)s] - %(message)s",
    )

    paths.ensure_dirs()

    if args.command == "download":
        _handle_download(args)
    elif args.command == "probe":
        _handle_probe(args)
    elif args.command == "analyze":
        _handle_analyze(args)
    elif args.command == "report":
        _handle_report(args)
    elif args.command == "config":
        _handle_config(args)


def _add_download_parser(subparsers):
    parser = subparsers.add_parser("download", help="Download data")
    parser.add_argument("--interface", type=str, help="Interface name")
    parser.add_argument(
        "--mode", choices=["incremental", "full"], default="incremental"
    )
    parser.add_argument("--days", type=int, default=1, help="Days back")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=4, help="Max workers")
    parser.add_argument("--schedule", action="store_true", help="Start scheduler")


def _add_probe_parser(subparsers):
    parser = subparsers.add_parser("probe", help="Probe interface health")
    parser.add_argument("--all", action="store_true", help="Probe all interfaces")
    parser.add_argument("--interface", type=str, help="Specific interface")
    parser.add_argument("--status", action="store_true", help="Show probe status")


def _add_analyze_parser(subparsers):
    parser = subparsers.add_parser("analyze", help="Analyze data")
    parser.add_argument("type", choices=["logs", "cache", "fields"])
    parser.add_argument("--window", type=int, default=7, help="Days window")
    parser.add_argument("--table", type=str, help="Table name")
    parser.add_argument("--category", type=str, help="Interface category")


def _add_report_parser(subparsers):
    parser = subparsers.add_parser("report", help="Generate reports")
    parser.add_argument("type", choices=["health", "quality", "dashboard"])
    parser.add_argument("--table", type=str, help="Table name")


def _add_config_parser(subparsers):
    parser = subparsers.add_parser("config", help="Manage configuration")
    parser.add_argument("action", choices=["generate", "validate", "merge"])


def _handle_download(args):
    """处理下载命令"""
    from akshare_data.offline.downloader import BatchDownloader
    from akshare_data.offline.core.data_loader import get_cache_manager_instance

    cache_manager = get_cache_manager_instance()
    downloader = BatchDownloader(cache_manager=cache_manager, max_workers=args.workers)

    if args.schedule:
        from akshare_data.offline.scheduler import Scheduler

        scheduler = Scheduler()
        scheduler.set_downloader(downloader)
        scheduler.start()
        print("Scheduler started. Press Ctrl+C to stop.")
        try:
            while True:
                import time

                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.stop()
        return

    if args.mode == "incremental":
        result = downloader.download_incremental(days_back=args.days)
    else:
        interfaces = [args.interface] if args.interface else None
        result = downloader.download_full(
            interfaces=interfaces,
            start=args.start or "2020-01-01",
            end=args.end,
        )

    print(f"Download completed: {result}")


def _handle_probe(args):
    """处理探测命令"""
    from akshare_data.offline.prober import APIProber

    prober = APIProber()

    if args.status:
        results = prober.checkpoint_mgr.get_all_results()
        if results:
            print(f"Last probe results ({len(results)} interfaces):")
            for func_name, r in results.items():
                print(
                    f"  {func_name}: {r['status']} ({r['exec_time']:.2f}s, {r['data_size']} rows)"
                )
        print(f"Summary: {prober.get_summary()}")
        return

    if args.interface:
        prober.config = {args.interface: prober.config.get(args.interface, {})}
        if not prober.config:
            print(f"Interface {args.interface} not found in registry")
            return

    prober.run_check()
    summary = prober.get_summary()
    print(f"Probe completed: {summary}")

    results = prober.get_results()
    for func_name, result in results.items():
        print(
            f"  {func_name}: {result.status} ({result.exec_time:.2f}s, {result.data_size} rows)"
        )
        if result.error_msg:
            print(f"    Error: {result.error_msg}")

    from akshare_data.offline.report import HealthReportGenerator

    report_data = {}
    for func_name, result in results.items():
        report_data[func_name] = {
            "func_name": result.func_name,
            "domain_group": result.domain_group,
            "status": result.status,
            "error_msg": result.error_msg,
            "exec_time": result.exec_time,
            "data_size": result.data_size,
        }
    generator = HealthReportGenerator()
    generator.generate(report_data, total_elapsed=prober.total_elapsed)
    print(f"Health report generated at {paths.health_reports_dir}")


def _handle_analyze(args):
    """处理分析命令"""
    if args.type == "logs":
        from akshare_data.offline.analyzer import CallStatsAnalyzer

        analyzer = CallStatsAnalyzer()
        result = analyzer.analyze(window_days=args.window)
        print(f"Analysis completed: {len(result.get('priorities', {}))} interfaces")

    elif args.type == "cache":
        if not args.table:
            print("Error: --table required for cache analysis")
            sys.exit(1)
        from akshare_data.offline.analyzer import CompletenessChecker
        from akshare_data.offline.core.data_loader import load_table

        df = load_table(args.table)
        if df.empty:
            print(f"No data found for table: {args.table}")
            sys.exit(0)

        date_col = CompletenessChecker()._find_date_column(df)
        expected_dates = []
        if date_col and date_col in df.columns:
            import pandas as pd
            from datetime import timedelta

            dates = pd.to_datetime(df[date_col]).dropna().sort_values()
            if len(dates) > 0:
                min_date = dates.min()
                max_date = dates.max()
                current = min_date
                while current <= max_date:
                    if current.weekday() < 5:
                        expected_dates.append(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)

        checker = CompletenessChecker()
        result = checker.check(
            df, expected_dates=expected_dates if expected_dates else None
        )

        print(f"=== Cache Completeness Analysis: {args.table} ===")
        print(f"Total records: {result['total_records']}")
        print(f"Completeness ratio: {result['completeness_ratio']:.2%}")
        print(f"Is complete: {result['is_complete']}")
        if result.get("missing_dates_count"):
            print(f"Missing dates count: {result['missing_dates_count']}")
            if result["missing_dates"]:
                print(f"Missing dates (first 10): {result['missing_dates'][:10]}")
        if result.get("missing_fields"):
            print(f"Missing fields: {result['missing_fields']}")

    elif args.type == "fields":
        from akshare_data.offline.analyzer import FieldMapper

        mapper = FieldMapper()
        mapper.analyze_all(category=args.category, sample_size=50)
        report = mapper.generate_report()
        print(report)
        json_path = mapper.export_mappings_json()
        print(f"\nField mappings exported to: {json_path}")


def _handle_report(args):
    """处理报告命令"""
    if args.type == "health":
        import json
        from akshare_data.offline.report import HealthReportGenerator

        state_file = paths.prober_state_file
        if not state_file.exists():
            print(f"No probe data found at {state_file}")
            print("Run 'python -m akshare_data.offline.cli probe --all' first")
            sys.exit(1)

        with open(state_file, "r") as f:
            probe_results = json.load(f)

        generator = HealthReportGenerator()
        content = generator.generate(results=probe_results)
        if content:
            print("Health report generated successfully")
            print(content)
        else:
            print("Health report is empty (no probe data)")

    elif args.type == "quality":
        if not args.table:
            print("Error: --table required for quality report")
            sys.exit(1)
        from akshare_data.offline.report import QualityReportGenerator
        from akshare_data.offline.analyzer.cache_analysis import (
            CompletenessChecker,
            AnomalyDetector,
        )

        df = _load_table_data(args.table)
        checker = CompletenessChecker()
        detector = AnomalyDetector()

        completeness = checker.check(df)
        anomalies = detector.detect(df)

        quality_results = {
            "table": args.table,
            "symbol": "N/A",
            "checks": {
                "completeness": completeness,
                "anomalies": anomalies,
            },
            "summary": {
                "total_records": len(df) if df is not None and not df.empty else 0,
                "columns": df.columns.tolist()
                if df is not None and not df.empty
                else [],
                "has_data": df is not None and not df.empty,
            },
        }

        generator = QualityReportGenerator()
        content = generator.generate(quality_results=quality_results)
        if content:
            print(f"Quality report for {args.table} generated successfully")
            print(content)
        else:
            print("Quality report is empty")

    elif args.type == "dashboard":
        import json
        from akshare_data.offline.report.renderer import ReportRenderer
        from datetime import datetime

        renderer = ReportRenderer()
        output_dir = paths.dashboard_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"dashboard_{datetime.now().strftime('%Y%m%d')}.md"

        sections = {
            "AkShare Data Service Dashboard": {
                "Report Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Project Root": str(paths.project_root),
            },
        }

        state_file = paths.prober_state_file
        if state_file.exists():
            with open(state_file, "r") as f:
                probe_results = json.load(f)
            total = len(probe_results)
            success = sum(1 for r in probe_results if "Success" in r.get("status", ""))
            failed = total - success
            rate = (success / total * 100) if total > 0 else 0
            sections["API Health Overview"] = {
                "Total APIs": total,
                "Available": success,
                "Failed": failed,
                "Health Rate": f"{rate:.1f}%",
            }
            if total > 0:
                import pandas as pd

                df = pd.DataFrame(probe_results)
                if "exec_time" in df.columns:
                    slowest = df.sort_values("exec_time", ascending=False).head(10)
                    sections["Top 10 Slowest APIs"] = slowest[
                        ["func_name", "exec_time", "status"]
                    ]
        else:
            sections["API Health Overview"] = {
                "Status": "No probe data available",
            }

        content = renderer.render_markdown(sections)
        renderer.save(content, output_file)
        print(f"Dashboard report generated: {output_file}")
        print(content)


def _load_table_data(table_name: str):
    """尝试从缓存加载表数据，如果没有则返回空 DataFrame"""
    try:
        from akshare_data.offline.core.data_loader import load_table

        return load_table(table_name)
    except Exception:
        pass

    import pandas as pd

    logger.warning(f"No cached data found for table: {table_name}, using empty dataset")
    return pd.DataFrame()


def _handle_config(args):
    """处理配置命令"""
    if args.action == "generate":
        from akshare_data.offline.registry import RegistryBuilder, RegistryExporter

        builder = RegistryBuilder()
        registry = builder.build()
        exporter = RegistryExporter()
        output = exporter.export_yaml(registry)
        print(f"Config generated: {output}")

    elif args.action == "validate":
        from akshare_data.offline.registry import RegistryValidator

        validator = RegistryValidator()
        import yaml
        from akshare_data.offline.core.paths import paths

        try:
            with open(paths.legacy_registry_file, "r") as f:
                registry = yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Registry has YAML syntax errors: {e}")
            print("Validation skipped due to file corruption.")
            return
        errors = validator.validate(registry)
        if errors:
            print(f"Validation found {len(errors)} issues:")
            for e in errors:
                print(f"  - {e}")
        else:
            print("Validation passed")

    elif args.action == "merge":
        from akshare_data.offline.registry import (
            RegistryBuilder,
            RegistryMerger,
            RegistryExporter,
        )

        builder = RegistryBuilder()
        registry = builder.build()
        merger = RegistryMerger()
        merger.merge_interfaces(registry)
        merger.merge_rate_limits(registry)
        exporter = RegistryExporter()
        output = exporter.export_yaml(registry)
        print(f"Config merged: {output}")


if __name__ == "__main__":
    main()
