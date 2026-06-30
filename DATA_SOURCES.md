# DATA_SOURCES.md

Status: active source register for the UK interconnector build.

Repository: `Ventusltd/data-interconnectors`

## Scope

This repository stores GB interconnector border-flow data. It does not store domestic GB generation and it does not redefine generation totals.

## Source 1 — Elexon BMRS FUELINST interconnector rows

Name: Elexon BMRS FUELINST.

Endpoint: `https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST`

Query pattern: `publishDateTimeFrom`, `publishDateTimeTo`, `format=json`.

Fetch method: one complete calendar month per request, starting `2020-12` and ending at the latest complete calendar month by default.

Source grain: signed MW by fuel type timestamp.

Rows used here: the ten operational INT-coded fuel types only.

Operational BMRS codes:

```text
INTFR
INTIFA2
INTELEC
INTNED
INTNEM
INTNSL
INTVKL
INTEW
INTGRNL
INTIRL
```

## Source interpretation

Positive signed MW is import to GB.

Negative signed MW is export from GB.

Interconnectors are flows. They must never be summed into domestic generation.

## Normalisation

The build normalises API rows into:

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
```

The canonical key is:

```text
periodStartUTC + bmrsCode
```

## Data output

Granular derived flow rows:

```text
flows/dataset=fuelinst_interconnector/year=YYYY/month=M/data_0.parquet
```

Chart rollups:

```text
rollups/monthly_by_link_direction.parquet
rollups/annual_by_link_direction.parquet
```

## Reference oracle

The old monolith already produced monthly interconnector JSON under:

```text
uk_energy_tracking_v6/generation_history/interconnectors/
```

The build clones the monolith during the workflow and reconciles against those JSON files. That monolith JSON is an accuracy oracle only. It is not copied as the primary data product.

## Failure rules

The build fails on:

```text
API error
empty API response for a requested month
schema mismatch that prevents period, code or MW extraction
zero filtered interconnector rows for a requested month
duplicate periodStartUTC + bmrsCode keys after dedup/write
null periodStartUTC or bmrsCode
missing all current operational codes in the latest complete month
```
