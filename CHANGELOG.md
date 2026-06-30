# CHANGELOG.md

Plain-language project change log for `data-interconnectors`. Newest entries first.

---

## 2026-06-30 — UK interconnector direct build assets added

Added the source register, dependency register, implementation method and operational/future cable reference table for the direct UK interconnector build.

The build target is Elexon BMRS FUELINST signed INT-coded rows for the ten current operational GB interconnectors.

The output target is compact zstd Parquet plus monthly and annual rollups. Interconnectors remain flows and are not domestic generation.

The workflow will fetch fresh data month by month, write the Parquet product, read it back, test the declared key, and reconcile against the monolith interconnector JSON oracle.

Actual build row counts, reconciliation numbers and output file details will be inserted by the build script after the workflow runs.
