#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from build_interconnectors import (
    API_URL,
    CODES,
    DEFAULT_INTERVAL_HOURS,
    MAX_INFERRED_INTERVAL_HOURS,
    METHOD_VERSION,
    ROOT,
    SOURCE,
    build_rollups,
    fetch_month,
    latest_complete_month,
    month_iter,
    next_month,
    normalise_month,
    parse_month,
    reconcile,
    update_changelog,
    utcnow,
    verify_output,
    write_month,
    write_reports,
)

BOOTSTRAP_START_MONTH = "2020-12"


def month_text(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def shift_month(year: int, month: int, offset: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + offset
    return idx // 12, idx % 12 + 1


def existing_flow_files() -> list[Path]:
    return list((ROOT / "flows" / "dataset=fuelinst_interconnector").glob("year=*/month=*/*.parquet"))


def resolve_month_range(start: str, end: str, refetch_months: int, bootstrap_start: str) -> tuple[str, str, str]:
    end_month = latest_complete_month() if not end or end == "latest-complete" else end
    if refetch_months < 1:
        raise SystemExit("refetch-months must be >= 1")

    if start and start not in {"auto", "latest-complete"}:
        return start, end_month, "explicit_month_range"

    if not existing_flow_files():
        return bootstrap_start, end_month, "bootstrap_full_history_no_existing_parquet"

    ey, em = parse_month(end_month)
    sy, sm = shift_month(ey, em, -(refetch_months - 1))
    return month_text(sy, sm), end_month, f"monthly_refetch_last_{refetch_months}_complete_months"


def main() -> int:
    ap = argparse.ArgumentParser(description="Monthly interconnector updater for data-interconnectors")
    ap.add_argument("--start", default="auto", help="YYYY-MM or auto. Auto means trailing refetch, or bootstrap if no Parquet exists.")
    ap.add_argument("--end", default="latest-complete", help="YYYY-MM or latest-complete")
    ap.add_argument("--refetch-months", type=int, default=3, help="Recent complete months to rewrite when start is auto.")
    ap.add_argument("--bootstrap-start", default=BOOTSTRAP_START_MONTH)
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--monolith-dir", default="_monolith/uk_energy_tracking_v6/generation_history/interconnectors")
    ap.add_argument("--fail-on-reconciliation-mismatch", dest="fail_on_reconciliation_mismatch", action="store_true", default=True)
    ap.add_argument("--allow-reconciliation-mismatch", dest="fail_on_reconciliation_mismatch", action="store_false")
    args = ap.parse_args()

    start_month, end_month, selection_mode = resolve_month_range(args.start, args.end, args.refetch_months, args.bootstrap_start)
    months = list(month_iter(start_month, end_month))
    if not months:
        raise RuntimeError("no months selected")

    month_reports = []
    removed_partitions = []
    for year, month in months:
        raw = fetch_month(year, month, args.timeout)
        rows, meta = normalise_month(year, month, raw)
        partition_dir = ROOT / "flows" / "dataset=fuelinst_interconnector" / f"year={year}" / f"month={month}"
        if partition_dir.exists():
            removed_partitions.append(str(partition_dir))
        write_month(rows, year, month)
        month_reports.append(meta)
        print(f"{year:04d}-{month:02d}: api={meta['apiRows']} interconnector={meta['interconnectorRows']} codes={','.join(meta['codesPresent'])} intervals={meta['intervalSourceCounts']}")

    rollups = build_rollups()
    verification = verify_output(end_month)
    reconciliation = reconcile(ROOT / args.monolith_dir)

    report = {
        "generatedUTC": utcnow(),
        "mode": "monthly_update",
        "selectionMode": selection_mode,
        "startMonth": start_month,
        "endMonth": end_month,
        "targetMonths": [month_text(y, m) for y, m in months],
        "removedPartitionsBeforeRewrite": removed_partitions,
        "methodVersion": METHOD_VERSION,
        "source": SOURCE,
        "apiUrl": API_URL,
        "signConvention": "positive signed MW is import to GB; negative signed MW is export from GB",
        "intervalMethod": "inferred per BMRS code from neighbouring readings; max one hour; default five minutes",
        "defaultIntervalHours": DEFAULT_INTERVAL_HOURS,
        "maxInferredIntervalHours": MAX_INFERRED_INTERVAL_HOURS,
        "operationalCodes": sorted(CODES),
        "months": month_reports,
        "verification": verification,
        "rollups": rollups,
        "reconciliation": reconciliation,
    }

    write_reports(report)
    if args.fail_on_reconciliation_mismatch and not reconciliation.get("accuracyProven"):
        raise RuntimeError("monolith reconciliation did not prove accuracy; see reports/INTERCONNECTOR_BUILD_LATEST.md")

    update_changelog(report)
    print(json.dumps(report["verification"], indent=2))
    print(json.dumps(report["reconciliation"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
