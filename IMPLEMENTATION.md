# IMPLEMENTATION.md

Status: direct UK interconnector build method.

## Method

1. Fetch Elexon BMRS FUELINST one complete calendar month at a time.
2. Filter to the ten current operational GB interconnector BMRS codes.
3. Normalise timestamp, fuel code and signed MW fields.
4. Deduplicate on `periodStartUTC + bmrsCode`.
5. Apply sign convention: positive MW is import to GB, negative MW is export from GB.
6. Calculate `grossMWh = abs(signedMW) * intervalHours`.
7. Calculate `signedMWh = signedMW * intervalHours`.
8. Use `intervalHours = 5 / 60` for the current UK FUELINST signal.
9. Write monthly zstd Parquet partitions.
10. Read back all Parquet and check keys, nulls and latest-month code presence.
11. Write monthly and annual rollups for chart consumption.
12. Reconcile against the monolith interconnector JSON oracle and record the result.

## Declared key

```text
periodStartUTC + bmrsCode
```

## Output fields

```text
periodStartUTC
bmrsCode
interconnectorName
country
flowDirection
signedMW
grossMWh
signedMWh
intervalHours
source
methodVersion
fetchedAtUTC
year
month
```

## Output layout

```text
flows/dataset=fuelinst_interconnector/year=YYYY/month=M/data_0.parquet
rollups/monthly_by_link_direction.parquet
rollups/annual_by_link_direction.parquet
reports/INTERCONNECTOR_BUILD_LATEST.md
reports/json/INTERCONNECTOR_BUILD_LATEST.json
```

## Reconciliation

The monolith JSON under `uk_energy_tracking_v6/generation_history/interconnectors/` is used as an oracle for monthly totals. The build records the number of overlapping keys, matched keys and mismatches.

If reconciliation fields cannot be parsed from the monolith files, the build records that clearly rather than copying the JSON as data.

## Direct build note

This UK build is direct because the source endpoint is the same FUELINST endpoint already used by the GB electricity data repo and because the monolith has an existing oracle for the UK ten. The future global build should use the full scope, audit and approval gate.
