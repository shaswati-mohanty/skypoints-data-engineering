# SkyPoints Data Engineering Assessment

Reference implementation for validated member-profile and partner-redemption ingestion using Python and Snowflake SQL.

## Architecture

`raw landing -> validation/quarantine -> normalized staging -> latest member state -> country serving tables`

Redemptions follow `raw VARIANT -> LATERAL FLATTEN -> transaction fact`; transactions join profiles by stable `MEMBER_ID`. Orphans remain visible through a left join and should be monitored. The Python reference is a local correctness harness, not a billions-of-rows single-process engine. At production scale, land immutable files in object storage, process distributed micro-batches with Snowflake COPY/Streams/Tasks or Spark, partition by ingestion date, cluster on member/transaction keys where measured useful, and make batch IDs idempotent. Preserve raw source and reject records for replay/audit.

## Input contract

CSV headers are mapped by aliases, or pipe records use `|H|...` followed by `|D|...`. Dates accepted by Python: `YYYYMMDD`, ISO, US slash dates and `MMDDYYYY`. The assessment's 8-character date fields are ambiguous (`03051985`): this solution interprets DOB as `MMDDYYYY`, and enrollment/flight dates as `YYYYMMDD`. Change the configured contract if the source confirms another format. Country aliases normalize US/IN/AU/PHIL; unknown values are preserved and should be reviewed. Missing optional DOB/flight is allowed; invalid supplied dates are rejected. Key is `member_id`; duplicate rows in a batch resolve in source order (last valid row wins). Across batches, use a source effective timestamp/sequence in production, not arrival order. The sample has no source update timestamp, so batch business date and file/row order are documented tie-breakers.

## Run

```powershell
$env:PYTHONPATH = 'src'
python -m skypoints_etl.cli --members path\members.csv --redemptions path\redemptions.json --as-of 2024-01-15 --out out
python -m pytest
```

### Country-coded member CSV files

The `skypoints-ingest` command recognizes a country code at the start of a filename or as a delimited filename token. `US`, `IN`, and `AU` normalize to `USA`, `IND`, and `AUS`. It maps common header variants into `ID, MEMBER_NAME, DOB, TIER_CODE, ENROLLMENT_DATE, IND_CORP, FLIGHT_DATE`; missing optional columns are emitted as blank values and recorded in a JSON schema manifest. Dates in the output CSVs use ISO `YYYY-MM-DD`; Snowflake columns use `DATE`.

Rows with missing required IDs/names, invalid supplied dates, unsupported tier values, conflicting row/file countries, or duplicate IDs within one file are written to the dated reject CSV. Accepted rows are written to `COUNTRY_members_YYYYMMDD.csv`. The command also generates Snowflake SQL to persist raw rows, normalized staging rows, schema inspection, rejects, and per-country member tables. It computes age and the `> 90` day stale flag using the ingestion date.

```powershell
$env:PYTHONPATH = 'src'
python -m skypoints_etl.ingest_cli `
  --input-files 'C:\data\USA(in).csv' 'C:\data\IND(in).csv' 'C:\data\AUS(Sheet1).csv' `
  --output-dir outputs\ingested `
  --database SNOWFLAKE_LEARNING_DB --schema SKYPOINTS
```

Review the generated manifest and rejects before running the generated `snowflake_load_YYYYMMDD.sql` in the intended Snowflake development schema. Filename country mapping is used only when the member row has no country column; if a country field is present, it must agree with the filename. The sample CSV IDs repeat across countries, so country tables keep IDs scoped to their table. Global country-move resolution requires a globally unique member ID and an authoritative source update timestamp/sequence.

No database credentials are needed to run the local Python reference. Apply `sql/01_ddl.sql` and `sql/02_transform.sql` in a Snowflake development schema after setting batch/date variables and adapting country target creation to the approved country catalog. Target routing must be allow-listed and generated with safely quoted identifiers; do not interpolate untrusted source values into SQL.

## Data quality coverage

Required member name, ID, country, enrollment date; declared lengths; valid dates; future dates; DOB after enrollment; allowed active flags; optional-field nulls; exact stale cutoff (`>90`); stable member ID; transaction required fields, positive integer miles, known status, feed/transaction date relationship, duplicate IDs, unknown country and unmatched transactions. Keep rejects with reason and raw input. Production monitoring should add volume drift, duplicate rate, null/invalid rate, country distribution, referential orphan rate, late-arriving data, SLA and reconciliation (input = accepted + rejected).

## Assessment / interpretation notes

The assessment layout calls Post Code optional, while its sample header omits that column; code treats it as optional. Some sample member IDs are shorter than six digits despite the prose/table length definition, so they are preserved (maximum 18, not padded). `PHIL` and `AU` are mapped to `PHL` and `AUS`; production should use an approved ISO/country crosswalk. Snowflake standard-table primary keys are informational, so deduplication and DQ checks enforce uniqueness operationally. SQL contains an India target example; generate the other country targets from a controlled deployment-time country list, not arbitrary data values.
