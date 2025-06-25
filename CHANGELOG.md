## [3.0.0] - 2025-06-25

### ⚠️ Breaking Changes
- Migrated all stored data files from CSV to **Parquet** format.
  - All systems expecting `.csv` files must be updated to read `.parquet`.

### ✨ Improvements
- `DatalakeClient` and related utilities now read/write `.parquet` files instead of `.csv`.
- Added a migration script to convert all existing CSV files to Parquet and verify their integrity.
- Automatically removes old CSV files after successful conversion to avoid redundancy.

## [2.0.0] - 2025-06-01

### ⚠️ Breaking Changes
- Changed the naming convention of stored trading data files.
- Old code expecting the v1.x format will no longer work.

### ✨ Improvements
- Introduced a more consistent schema for symbol-date versioning (e.g., `FUT_ES_2025-06-01_historical_data.csv` vs `STK_ES_2025-06-01_historical_data.csv`)