# data-interconnectors

GlobalGrid2050 interconnector data repository.

This repo holds border-flow data separately from GB domestic generation data. Interconnectors are edges between systems. They are flows, not generation.

## Current build scope

Status: direct UK build.

Scope: current operational GB electricity interconnectors only.

Source: Elexon BMRS FUELINST signed INT-coded rows.

Endpoint: `https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST`

Query pattern: `publishDateTimeFrom`, `publishDateTimeTo`, `format=json`.

Reference method: `Ventusltd/globalgrid2050` GridBot interconnector split logic.

Reference oracle: monolith JSON files under `uk_energy_tracking_v6/generation_history/interconnectors/`, used only for reconciliation. They are not copied as the primary source.

## Operational cable list

| BMRS code | Country | Interconnector | Capacity | Status |
|---|---:|---|---:|---|
| `INTFR` | France | IFA | 2.0 GW | operational |
| `INTIFA2` | France | IFA2 | 1.0 GW | operational |
| `INTELEC` | France | ElecLink | 1.0 GW | operational |
| `INTNED` | Netherlands | BritNed | 1.0 GW | operational |
| `INTNEM` | Belgium | Nemo Link | 1.0 GW | operational |
| `INTNSL` | Norway | North Sea Link | 1.4 GW | operational |
| `INTVKL` | Denmark | Viking Link | 1.4 GW | operational |
| `INTEW` | Ireland | East West Interconnector | 0.5 GW | operational |
| `INTGRNL` | Ireland | Greenlink | 0.5 GW | operational |
| `INTIRL` | Northern Ireland | Moyle | 0.5 GW | operational |

## Future placeholders

Future cables are recorded as reference entries only. They carry no fake values, no BMRS code and no data wiring. They remain `DATA NOT WIRED` until Elexon issues an operational BMRS code and the code is explicitly added to the fetch list.

| Project | Country | Route | Capacity | Target | Status |
|---|---:|---|---:|---:|---|
| NeuConnect | Germany | Isle of Grain to Wilhelmshaven | 1.4 GW | 2028 | DATA NOT WIRED |
| Tarchon Energy | Germany | East Anglia to Niederlangen | 1.4 GW | 2032 | DATA NOT WIRED |
| LionLink | Netherlands | offshore hybrid to Suffolk | up to 1.8-2.0 GW | 2032 | DATA NOT WIRED |
| Nautilus | Belgium | offshore hybrid to Isle of Grain | 1.4 GW | 2032 | DATA NOT WIRED |
| MaresConnect | Ireland | Bodelwyddan North Wales to Republic of Ireland | 0.75 GW | 2032 | DATA NOT WIRED |
| LirIC | Northern Ireland | Kilroot to Hunterston | 0.7 GW | 2032 | DATA NOT WIRED |

## Sign convention

Positive signed MW is import to GB.

Negative signed MW is export from GB.

`grossMWh` is the absolute energy movement.

`signedMWh` keeps the import/export sign.

## Output layout

Granular derived flow rows:

```text
flows/dataset=fuelinst_interconnector/year=YYYY/month=M/data_0.parquet
```

Rollups for charts:

```text
rollups/monthly_by_link_direction.parquet
rollups/annual_by_link_direction.parquet
```

Format: Parquet.

Compression: zstd.

Raw CSV committed here: no.

Copied monolith JSON as source: no.

## Declared key

Compound key: `periodStartUTC` plus `bmrsCode`.

Build checks:

```text
total rows equal distinct periodStartUTC plus bmrsCode
zero null keys
all operational codes present in the latest complete month
monolith reconciliation recorded in reports and CHANGELOG
```

## Governance

Read the federation data discipline manual before editing this repo:

`Ventusltd/globalgrid2050-hompage/docs/DATA_DISCIPLINE_MANUAL.md`
