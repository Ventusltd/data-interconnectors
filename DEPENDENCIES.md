# DEPENDENCIES.md

Status: active dependency register.

## Upstream

`Elexon BMRS FUELINST` is the primary source for this repo.

`Ventusltd/data-gb-electricity` is the sibling reference for the monthly API plus Parquet method. It owns GB generation and price data. This repo owns only interconnector flow data.

`Ventusltd/globalgrid2050` is the monolith reference and reconciliation oracle. Its generated interconnector JSON files are used for comparison only and are not copied as the source of truth.

`Ventusltd/globalgrid2050-hompage` holds the federation governance and data discipline material.

## Downstream

`Ventusltd/gb-electricity-ui` consumes this repo for named interconnector import/export flow views.

The UI must not sum interconnector flows into domestic generation.

## Out of scope for this repo

This repo does not own domestic generation, system prices, solar PVLive, frequency, commodity, road fuel, EV charging or carbon feeds.
