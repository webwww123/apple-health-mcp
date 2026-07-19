#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from apple_health_mcp.storage import (
    DEFAULT_TIMEZONE,
    audit_metric_duplicates,
    repair_historical_metric_duplicates,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit or repair duplicate normalized Apple Health metric rows."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("/var/lib/apple-health-mcp/health.db"),
        help="SQLite database path",
    )
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Archive candidate duplicates and rebuild active rows from retained raw ingestions",
    )
    parser.add_argument(
        "--backup",
        type=Path,
        help="Required with --repair; SQLite backup created before any historical rewrite",
    )
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    args = parser.parse_args()

    if args.repair and args.backup is None:
        parser.error("--repair requires --backup")
    if args.repair:
        args.backup.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with sqlite3.connect(args.db) as source, sqlite3.connect(args.backup) as target:
            source.backup(target)
        args.backup.chmod(0o600)
        report = repair_historical_metric_duplicates(
            args.db,
            args.timezone,
            dry_run=False,
        )
        report["backup_path"] = str(args.backup)
    else:
        report = audit_metric_duplicates(args.db, args.timezone)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        args.output.chmod(0o600)
    print(rendered)


if __name__ == "__main__":
    main()
