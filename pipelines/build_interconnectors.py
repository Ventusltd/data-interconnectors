#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = Path(__file__).resolve().parents[1]
API_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST"
METHOD_VERSION = "fuelinst_interconnector_v2_inferred_interval_20260630"
SOURCE = "Elexon BMRS FUELINST"
DEFAULT_INTERVAL_HOURS = 5 / 60
MAX_INFERRED_INTERVAL_HOURS = 1.0

INTERCONNECTORS = [
    {"bmrsCode": "INTFR", "country": "France", "interconnectorName": "IFA", "capacityGW": 2.0},
    {"bmrsCode": "INTIFA2", "country": "France", "interconnectorName": "IFA2", "capacityGW": 1.0},
    {"bmrsCode": "INTELEC", "country": "France", "interconnectorName": "ElecLink", "capacityGW": 1.0},
    {"bmrsCode": "INTNED", "country": "Netherlands", "interconnectorName": "BritNed", "capacityGW": 1.0},
    {"bmrsCode": "INTNEM", "country": "Belgium", "interconnectorName": "Nemo Link", "capacityGW": 1.0},
    {"bmrsCode": "INTNSL", "country": "Norway", "interconnectorName": "North Sea Link", "capacityGW": 1.4},
    {"bmrsCode": "INTVKL", "country": "Denmark", "interconnectorName": "Viking Link", "capacityGW": 1.4},
    {"bmrsCode": "INTEW", "country": "Ireland", "interconnectorName": "East West Interconnector", "capacityGW": 0.5},
    {"bmrsCode": "INTGRNL", "country": "Ireland", "interconnectorName": "Greenlink", "capacityGW": 0.5},
    {"bmrsCode": "INTIRL", "country": "Northern Ireland", "interconnectorName": "Moyle", "capacityGW": 0.5},
]
SPEC = {x["bmrsCode"]: x for x in INTERCONNECTORS}
CODES = set(SPEC)

TIME_KEYS = [
    "periodStartUTC", "periodStartUtc", "periodStart", "publishDateTime", "publishDateTimeUTC",
    "publishedDateTime", "publishTime", "publishTimeUTC", "startTime", "startTimeUTC",
    "settlementPeriodStartUTC", "settlementPeriodStart",
]
CODE_KEYS = ["fuelType", "fuelTypeCode", "bmrsCode", "fuel", "psrType"]
MW_KEYS = ["generationMW", "generationMw", "generation", "currentUsage", "currentUsageMW", "quantity", "value", "mw"]


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_month(value: str) -> tuple[int, int]:
    y, m = value.split("-", 1)
    return int(y), int(m)


def month_start(year: int, month: int) -> dt.datetime:
    return dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)


def next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def month_iter(start: str, end: str):
    y, m = parse_month(start)
    ey, em = parse_month(end)
    while (y, m) <= (ey, em):
        yield y, m
        y, m = next_month(y, m)


def latest_complete_month() -> str:
    today = dt.datetime.now(dt.timezone.utc).date()
    first = today.replace(day=1)
    last_prev = first - dt.timedelta(days=1)
    return f"{last_prev.year:04d}-{last_prev.month:02d}"


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def api_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "Data", "items", "Items", "results", "Results", "records", "Records"):
            if isinstance(payload.get(key), list):
                return [x for x in payload[key] if isinstance(x, dict)]
        if any(k in payload for k in CODE_KEYS + MW_KEYS):
            return [payload]
    raise ValueError("API JSON schema mismatch: no row list found")


def pick(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    lowered = {str(k).lower(): k for k in row.keys()}
    for key in keys:
        real = lowered.get(key.lower())
        if real is not None and row[real] not in (None, ""):
            return row[real]
    return None


def parse_time(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        out = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if out.tzinfo is None:
        out = out.replace(tzinfo=dt.timezone.utc)
    return out.astimezone(dt.timezone.utc)


def parse_float(value: Any) -> float | None:
    try:
        out = float(value)
        return out if math.isfinite(out) else None
    except Exception:
        return None


def fetch_month(year: int, month: int, timeout: int) -> list[dict[str, Any]]:
    start = month_start(year, month)
    ny, nm = next_month(year, month)
    end = month_start(ny, nm)
    params = {
        "publishDateTimeFrom": iso_z(start),
        "publishDateTimeTo": iso_z(end),
        "format": "json",
    }
    response = requests.get(API_URL, params=params, timeout=timeout, headers={"Accept": "application/json"})
    if not response.ok and response.status_code == 400:
        params["publishDateTimeFrom"] = params["publishDateTimeFrom"].replace("Z", "")
        params["publishDateTimeTo"] = params["publishDateTimeTo"].replace("Z", "")
        response = requests.get(API_URL, params=params, timeout=timeout, headers={"Accept": "application/json"})
    if not response.ok:
        raise RuntimeError(f"FUELINST API error {response.status_code} for {year:04d}-{month:02d}: {response.text[:500]}")
    rows = api_rows(response.json())
    if not rows:
        raise RuntimeError(f"FUELINST API returned no rows for {year:04d}-{month:02d}")
    return rows


def infer_interval(items: list[dict[str, Any]], index: int) -> tuple[float, str]:
    t = items[index]["_time"]
    if index + 1 < len(items):
        gap = (items[index + 1]["_time"] - t).total_seconds() / 3600
        if 0 < gap <= MAX_INFERRED_INTERVAL_HOURS:
            return gap, "next_gap"
    if index > 0:
        gap = (t - items[index - 1]["_time"]).total_seconds() / 3600
        if 0 < gap <= MAX_INFERRED_INTERVAL_HOURS:
            return gap, "previous_gap"
    return DEFAULT_INTERVAL_HOURS, "default_5min"


def normalise_month(year: int, month: int, raw_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    start = month_start(year, month)
    ny, nm = next_month(year, month)
    end = month_start(ny, nm)
    fetched = utcnow()
    raw_dedup: dict[tuple[str, str], dict[str, Any]] = {}
    schema_errors = 0
    seen_candidate = 0
    for row in raw_rows:
        code = pick(row, CODE_KEYS)
        if code is None:
            continue
        code = str(code).strip().upper()
        if code not in CODES:
            continue
        seen_candidate += 1
        t = parse_time(pick(row, TIME_KEYS))
        mw = parse_float(pick(row, MW_KEYS))
        if t is None or mw is None:
            schema_errors += 1
            continue
        if not (start <= t < end):
            continue
        period = iso_z(t)
        raw_dedup[(period, code)] = {"_time": t, "periodStartUTC": period, "bmrsCode": code, "signedMW": float(mw)}

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in raw_dedup.values():
        grouped[rec["bmrsCode"]].append(rec)
    for code in grouped:
        grouped[code].sort(key=lambda r: r["_time"])

    rows: list[dict[str, Any]] = []
    interval_sources: Counter[str] = Counter()
    intervals: list[float] = []
    for code in sorted(grouped):
        items = grouped[code]
        for i, rec in enumerate(items):
            interval_hours, interval_source = infer_interval(items, i)
            interval_sources[interval_source] += 1
            intervals.append(interval_hours)
            spec = SPEC[code]
            mw = rec["signedMW"]
            direction = "import" if mw >= 0 else "export"
            rows.append({
                "periodStartUTC": rec["periodStartUTC"],
                "bmrsCode": code,
                "interconnectorName": spec["interconnectorName"],
                "country": spec["country"],
                "flowDirection": direction,
                "signedMW": round(float(mw), 6),
                "grossMWh": round(abs(float(mw)) * interval_hours, 9),
                "signedMWh": round(float(mw) * interval_hours, 9),
                "intervalHours": round(interval_hours, 9),
                "intervalSource": interval_source,
                "source": SOURCE,
                "methodVersion": METHOD_VERSION,
                "fetchedAtUTC": fetched,
                "year": year,
                "month": month,
            })
    rows.sort(key=lambda r: (r["periodStartUTC"], r["bmrsCode"]))
    if not rows:
        raise RuntimeError(f"No interconnector rows after filtering for {year:04d}-{month:02d}; schema_errors={schema_errors}; candidate={seen_candidate}")
    meta = {
        "month": f"{year:04d}-{month:02d}",
        "apiRows": len(raw_rows),
        "candidateInterconnectorRows": seen_candidate,
        "interconnectorRows": len(rows),
        "schemaErrors": schema_errors,
        "codesPresent": sorted(grouped.keys()),
        "dedupedDroppedRows": max(0, seen_candidate - len(raw_dedup) - schema_errors),
        "intervalSourceCounts": dict(sorted(interval_sources.items())),
        "intervalHoursMin": round(min(intervals), 9) if intervals else None,
        "intervalHoursMax": round(max(intervals), 9) if intervals else None,
    }
    return rows, meta


def write_month(rows: list[dict[str, Any]], year: int, month: int) -> None:
    out_dir = ROOT / "flows" / "dataset=fuelinst_interconnector" / f"year={year}" / f"month={month}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_dir / "data_0.parquet", compression="zstd")


def parquet_path() -> str:
    return str(ROOT / "flows" / "dataset=fuelinst_interconnector" / "year=*" / "month=*" / "*.parquet")


def build_rollups() -> dict[str, Any]:
    (ROOT / "rollups").mkdir(exist_ok=True)
    con = duckdb.connect()
    p = parquet_path()
    monthly_sql = f"""
      SELECT year, month, bmrsCode, country, interconnectorName, flowDirection,
             sum(grossMWh) AS grossMWh,
             sum(signedMWh) AS signedMWh,
             count(*) AS rowCount,
             min(periodStartUTC) AS firstPeriodStartUTC,
             max(periodStartUTC) AS lastPeriodStartUTC,
             '{METHOD_VERSION}' AS methodVersion
      FROM read_parquet('{p}')
      GROUP BY 1,2,3,4,5,6
      ORDER BY year, month, bmrsCode, flowDirection
    """
    annual_sql = f"""
      SELECT year, bmrsCode, country, interconnectorName, flowDirection,
             sum(grossMWh) AS grossMWh,
             sum(signedMWh) AS signedMWh,
             count(*) AS rowCount,
             min(periodStartUTC) AS firstPeriodStartUTC,
             max(periodStartUTC) AS lastPeriodStartUTC,
             '{METHOD_VERSION}' AS methodVersion
      FROM read_parquet('{p}')
      GROUP BY 1,2,3,4,5
      ORDER BY year, bmrsCode, flowDirection
    """
    con.execute(f"COPY ({monthly_sql}) TO '{ROOT / 'rollups' / 'monthly_by_link_direction.parquet'}' (FORMAT parquet, COMPRESSION zstd)")
    con.execute(f"COPY ({annual_sql}) TO '{ROOT / 'rollups' / 'annual_by_link_direction.parquet'}' (FORMAT parquet, COMPRESSION zstd)")
    monthly_rows = con.execute(f"SELECT count(*) FROM read_parquet('{ROOT / 'rollups' / 'monthly_by_link_direction.parquet'}')").fetchone()[0]
    annual_rows = con.execute(f"SELECT count(*) FROM read_parquet('{ROOT / 'rollups' / 'annual_by_link_direction.parquet'}')").fetchone()[0]
    return {"monthlyRows": int(monthly_rows), "annualRows": int(annual_rows)}


def verify_output(latest_month: str) -> dict[str, Any]:
    con = duckdb.connect()
    p = parquet_path()
    total_rows, distinct_keys = con.execute(f"""
        SELECT count(*) AS rows, count(DISTINCT periodStartUTC || '|' || bmrsCode) AS keys
        FROM read_parquet('{p}')
    """).fetchone()
    duplicate_groups = con.execute(f"""
        SELECT count(*) FROM (
          SELECT periodStartUTC, bmrsCode, count(*) AS c
          FROM read_parquet('{p}')
          GROUP BY 1,2
          HAVING count(*) > 1
        )
    """).fetchone()[0]
    null_keys = con.execute(f"""
        SELECT count(*) FROM read_parquet('{p}')
        WHERE periodStartUTC IS NULL OR bmrsCode IS NULL OR periodStartUTC = '' OR bmrsCode = ''
    """).fetchone()[0]
    latest_y, latest_m = parse_month(latest_month)
    latest_codes = [r[0] for r in con.execute(f"""
        SELECT DISTINCT bmrsCode FROM read_parquet('{p}')
        WHERE year={latest_y} AND month={latest_m}
        ORDER BY 1
    """).fetchall()]
    missing_latest = sorted(CODES - set(latest_codes))
    if total_rows != distinct_keys:
        raise RuntimeError(f"duplicate key breach: rows {total_rows} != distinct keys {distinct_keys}")
    if duplicate_groups:
        raise RuntimeError(f"duplicate key groups found: {duplicate_groups}")
    if null_keys:
        raise RuntimeError(f"null key rows found: {null_keys}")
    if missing_latest:
        raise RuntimeError(f"latest complete month missing operational codes: {missing_latest}")
    parquet_files = [x for x in (ROOT / "flows").rglob("*.parquet")]
    return {
        "rows": int(total_rows),
        "distinctKeys": int(distinct_keys),
        "duplicateKeyGroups": int(duplicate_groups),
        "nullKeyRows": int(null_keys),
        "latestCompleteMonth": latest_month,
        "latestMonthCodes": latest_codes,
        "parquetFiles": len(parquet_files),
        "flowsMb": round(sum(x.stat().st_size for x in parquet_files) / 1048576, 3),
    }


def number(value: Any) -> float | None:
    try:
        out = float(value)
        return out if math.isfinite(out) else None
    except Exception:
        return None


def extract_year_month(row: dict[str, Any]) -> tuple[int, int] | None:
    y = row.get("year") or row.get("Year")
    m = row.get("month") or row.get("Month")
    if y and m:
        return int(y), int(m)
    for key in ("monthStart", "monthStartUTC", "date", "periodStartUTC"):
        if row.get(key):
            t = parse_time(row.get(key)) or parse_time(str(row.get(key)) + "T00:00:00Z")
            if t:
                return t.year, t.month
    return None


def monolith_monthly_map(monolith_dir: Path) -> tuple[dict[tuple[int, int, str, str], float], dict[str, Any]]:
    out: dict[tuple[int, int, str, str], float] = {}
    meta = {"found": False, "files": 0, "rows": 0, "notes": []}
    if not monolith_dir.exists():
        meta["notes"].append(f"monolith dir not found: {monolith_dir}")
        return out, meta
    for path in sorted(monolith_dir.glob("*.json")):
        meta["files"] += 1
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            meta["notes"].append(f"{path.name}: json read failed {exc}")
            continue
        rows = None
        if isinstance(data, dict):
            rows = data.get("monthlyRows") or data.get("monthly_rows") or data.get("rows")
            default_code = data.get("bmrsCode") or data.get("code")
        else:
            rows = data if isinstance(data, list) else None
            default_code = None
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            ym = extract_year_month(row)
            if not ym:
                continue
            code = row.get("bmrsCode") or row.get("code") or row.get("interconnectorCode") or default_code
            if not code:
                continue
            code = str(code).strip().upper()
            if code not in CODES:
                continue
            made = False
            imp = number(row.get("importMWh") or row.get("importsMWh"))
            exp = number(row.get("exportMWh") or row.get("exportsMWh"))
            if imp is not None:
                out[(ym[0], ym[1], code, "import")] = out.get((ym[0], ym[1], code, "import"), 0.0) + abs(imp)
                made = True
            if exp is not None:
                out[(ym[0], ym[1], code, "export")] = out.get((ym[0], ym[1], code, "export"), 0.0) - abs(exp)
                made = True
            if made:
                meta["rows"] += 1
                continue
            signed = number(row.get("signedMWh") or row.get("netMWh") or row.get("totalSignedMWh"))
            if signed is None:
                continue
            direction = row.get("flowDirection") or row.get("direction") or ("import" if signed >= 0 else "export")
            direction = str(direction).strip().lower()
            if direction.startswith("imp"):
                direction = "import"
            elif direction.startswith("exp"):
                direction = "export"
            else:
                direction = "import" if signed >= 0 else "export"
            out[(ym[0], ym[1], code, direction)] = out.get((ym[0], ym[1], code, direction), 0.0) + signed
            meta["rows"] += 1
    meta["found"] = bool(out)
    return out, meta


def fresh_monthly_map() -> dict[tuple[int, int, str, str], float]:
    con = duckdb.connect()
    rows = con.execute(f"""
      SELECT year, month, bmrsCode, flowDirection, sum(signedMWh) AS signedMWh
      FROM read_parquet('{ROOT / 'rollups' / 'monthly_by_link_direction.parquet'}')
      GROUP BY 1,2,3,4
    """).fetchall()
    return {(int(y), int(m), str(code), str(direction)): float(val or 0) for y, m, code, direction, val in rows}


def reconcile(monolith_dir: Path) -> dict[str, Any]:
    mono, meta = monolith_monthly_map(monolith_dir)
    fresh = fresh_monthly_map()
    checked = matched = missing = mismatched = 0
    examples = []
    for key, mono_val in mono.items():
        if key not in fresh:
            missing += 1
            if len(examples) < 20:
                examples.append({"key": list(key), "issue": "missing in fresh", "monolithSignedMWh": round(mono_val, 3)})
            continue
        checked += 1
        fresh_val = fresh[key]
        diff = abs(fresh_val - mono_val)
        tolerance = max(1.0, abs(mono_val) * 0.005)
        if diff <= tolerance:
            matched += 1
        else:
            mismatched += 1
            if len(examples) < 20:
                examples.append({"key": list(key), "monolithSignedMWh": round(mono_val, 3), "freshSignedMWh": round(fresh_val, 3), "diffMWh": round(diff, 3), "toleranceMWh": round(tolerance, 3)})
    return {
        "monolith": meta,
        "freshKeys": len(fresh),
        "monolithKeys": len(mono),
        "checkedOverlapKeys": checked,
        "matchedWithinTolerance": matched,
        "missingInFresh": missing,
        "mismatched": mismatched,
        "accuracyProven": bool(checked and checked == matched and not missing and not mismatched),
        "examples": examples,
    }


def write_reports(report: dict[str, Any]) -> None:
    report_dir = ROOT / "reports"
    json_dir = report_dir / "json"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / "INTERCONNECTOR_BUILD_LATEST.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    md = [
        "# Interconnector Build Latest",
        "",
        f"Generated UTC: `{report['generatedUTC']}`",
        f"Range: `{report['startMonth']}` to `{report['endMonth']}`",
        "",
        "## Output",
        "",
        f"- Flow rows: `{report['verification']['rows']}`",
        f"- Distinct keys: `{report['verification']['distinctKeys']}`",
        f"- Duplicate key groups: `{report['verification']['duplicateKeyGroups']}`",
        f"- Null key rows: `{report['verification']['nullKeyRows']}`",
        f"- Flow parquet files: `{report['verification']['parquetFiles']}`",
        f"- Flow parquet MB: `{report['verification']['flowsMb']}`",
        f"- Monthly rollup rows: `{report['rollups']['monthlyRows']}`",
        f"- Annual rollup rows: `{report['rollups']['annualRows']}`",
        "",
        "## Interval method",
        "",
        f"- Default interval hours: `{DEFAULT_INTERVAL_HOURS}`",
        f"- Max inferred interval hours: `{MAX_INFERRED_INTERVAL_HOURS}`",
        "- Interval hours are inferred per BMRS code from neighbouring readings, with previous-gap/default fallback.",
        "",
        "## Latest complete month code check",
        "",
        "```text",
        "\n".join(report['verification']['latestMonthCodes']),
        "```",
        "",
        "## Monolith reconciliation",
        "",
        f"- Monolith keys: `{report['reconciliation']['monolithKeys']}`",
        f"- Fresh keys: `{report['reconciliation']['freshKeys']}`",
        f"- Checked overlap keys: `{report['reconciliation']['checkedOverlapKeys']}`",
        f"- Matched within tolerance: `{report['reconciliation']['matchedWithinTolerance']}`",
        f"- Missing in fresh: `{report['reconciliation']['missingInFresh']}`",
        f"- Mismatched: `{report['reconciliation']['mismatched']}`",
        f"- Accuracy proven: `{report['reconciliation']['accuracyProven']}`",
        "",
        "## Rule",
        "",
        "Positive signed MW is import to GB. Negative signed MW is export from GB. Interconnectors are flows, never domestic generation.",
    ]
    if report["reconciliation"].get("examples"):
        md += ["", "## Reconciliation examples", "", "```json", json.dumps(report["reconciliation"]["examples"], indent=2), "```"]
    (report_dir / "INTERCONNECTOR_BUILD_LATEST.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def update_changelog(report: dict[str, Any]) -> None:
    path = ROOT / "CHANGELOG.md"
    old = path.read_text(encoding="utf-8") if path.exists() else "# CHANGELOG.md\n\n"
    entry = f"""\n---\n\n## {dt.date.today().isoformat()} — UK interconnector Parquet build result\n\nBuilt the UK interconnector flow data product from fresh Elexon BMRS FUELINST API windows.\n\nRange: `{report['startMonth']}` to `{report['endMonth']}`.\n\nFlow rows: `{report['verification']['rows']}`. Distinct declared keys: `{report['verification']['distinctKeys']}`. Duplicate key groups: `{report['verification']['duplicateKeyGroups']}`. Null key rows: `{report['verification']['nullKeyRows']}`.\n\nFlow parquet files: `{report['verification']['parquetFiles']}`. Flow parquet MB: `{report['verification']['flowsMb']}`. Monthly rollup rows: `{report['rollups']['monthlyRows']}`. Annual rollup rows: `{report['rollups']['annualRows']}`.\n\nInterval method: inferred per BMRS code from actual reading spacing, with a one-hour cap and default five-minute fallback.\n\nMonolith reconciliation checked `{report['reconciliation']['checkedOverlapKeys']}` overlapping keys, matched `{report['reconciliation']['matchedWithinTolerance']}` within tolerance, missing `{report['reconciliation']['missingInFresh']}`, mismatched `{report['reconciliation']['mismatched']}`. Accuracy proven: `{report['reconciliation']['accuracyProven']}`.\n\n"""
    if "---\n" in old:
        head, tail = old.split("---\n", 1)
        path.write_text(head + entry + tail, encoding="utf-8")
    else:
        path.write_text(old.rstrip() + entry, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2020-12")
    ap.add_argument("--end", default="latest-complete")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--monolith-dir", default="_monolith/uk_energy_tracking_v6/generation_history/interconnectors")
    ap.add_argument("--fail-on-reconciliation-mismatch", dest="fail_on_reconciliation_mismatch", action="store_true", default=True)
    ap.add_argument("--allow-reconciliation-mismatch", dest="fail_on_reconciliation_mismatch", action="store_false")
    args = ap.parse_args()

    end = latest_complete_month() if args.end == "latest-complete" else args.end
    months = list(month_iter(args.start, end))
    if not months:
        raise RuntimeError("no months selected")

    flow_root = ROOT / "flows" / "dataset=fuelinst_interconnector"
    if flow_root.exists():
        shutil.rmtree(flow_root)

    month_reports = []
    for y, m in months:
        raw = fetch_month(y, m, args.timeout)
        rows, meta = normalise_month(y, m, raw)
        write_month(rows, y, m)
        month_reports.append(meta)
        print(f"{y:04d}-{m:02d}: api={meta['apiRows']} interconnector={meta['interconnectorRows']} codes={','.join(meta['codesPresent'])} intervals={meta['intervalSourceCounts']}")

    rollups = build_rollups()
    verification = verify_output(end)
    reconciliation = reconcile(ROOT / args.monolith_dir)
    if args.fail_on_reconciliation_mismatch and not reconciliation.get("accuracyProven"):
        report = {
            "generatedUTC": utcnow(),
            "startMonth": args.start,
            "endMonth": end,
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
        raise RuntimeError("monolith reconciliation did not prove accuracy; see reports/INTERCONNECTOR_BUILD_LATEST.md")

    report = {
        "generatedUTC": utcnow(),
        "startMonth": args.start,
        "endMonth": end,
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
    update_changelog(report)
    print(json.dumps(report["verification"], indent=2))
    print(json.dumps(report["reconciliation"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
