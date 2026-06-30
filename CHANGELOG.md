# CHANGELOG.md

Plain-language project change log for `data-interconnectors`. Newest entries first.

---

## 2026-06-30 — Claudit corrections before first build trigger

Pre-trigger review found one important accuracy issue and one fail-loud issue.

The builder no longer values every FUELINST row as a fixed five-minute block. It now infers `intervalHours` per BMRS code from actual neighbouring reading spacing, with previous-gap/default fallback and a one-hour cap.

The workflow now fails reconciliation by default. A run can only allow reconciliation mismatch if explicitly triggered with `fail_on_reconciliation_mismatch: false`, which passes `--allow-reconciliation-mismatch` to the script.

These corrections were made before trusting the first data build.

---

## 2026-06-30 — UK interconnector direct build assets added

Added the source register, dependency register, implementation method and operational/future cable reference table for the direct UK interconnector build.

The build target is Elexon BMRS FUELINST signed INT-coded rows for the ten current operational GB interconnectors.

The output target is compact zstd Parquet plus monthly and annual rollups. Interconnectors remain flows and are not domestic generation.

The workflow will fetch fresh data month by month, write the Parquet product, read it back, test the declared key, and reconcile against the monolith interconnector JSON oracle.

Actual build row counts, reconciliation numbers and output file details will be inserted by the build script after the workflow runs.
