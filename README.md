# SkyPoints Data Engineering Assessment

Reference implementation for validated member-profile and partner-redemption ingestion using Python and Snowflake SQL.

## Architecture

`raw landing -> validation/quarantine -> normalized staging -> latest member state -> country serving tables`

Redemptions follow `raw VARIANT -> LATERAL FLATTEN -> transaction fact`; transactions join profiles by stable `MEMBER_ID`. Orphans remain visible through a left join and should be monitored. The Python reference is a local correctness harness, not a billions-of-rows single-process engine. At production scale, land immutable files in object storage, process distributed micro-batches with Snowflake COPY/Streams/Tasks or Spark, partition by ingestion date, cluster on member/transaction keys where measured useful, and make batch IDs idempotent. Preserve raw source and reject records for replay/audit.

## Input contract

CSV headers are mapped by aliases, or pipe records use `|H|...` followed by `|D|...`. Empty pipe fields are preserved and rows with the wrong number of fields are rejected. Dates accepted by Python: `YYYYMMDD`, unpadded `MMDDYYYY` (such as `6152022`), ISO, and US slash dates. The assessment's date fields are ambiguous (`03051985`): DOB is interpreted as `MMDDYYYY`; enrollment and flight dates prefer `YYYYMMDD`. Country aliases normalize US/IN/AU/PHIL; unknown values require an approved country mapping. Missing optional DOB/flight is allowed; invalid supplied dates are rejected. Key is `member_id`; duplicate rows in a batch resolve in source order (last valid row wins) and the superseded row is recorded in rejects. Cross-batch ordering requires a source effective timestamp/sequence.

## Run

```powershell
$env:PYTHONPATH = 'src'
python -m skypoints_etl.cli --members path\members.csv --redemptions path\redemptions.json --as-of 2024-01-15 --out out
python -m pytest
```

### Country-coded member CSV files

The `skypoints-ingest` command recognizes a country code at the start of a filename or as a delimited filename token. `US`, `IN`, and `AU` normalize to `USA`, `IND`, and `AUS`. It maps common header variants into `ID, MEMBER_NAME, DOB, TIER_CODE, ENROLLMENT_DATE, IND_CORP, FLIGHT_DATE`; missing optional columns are emitted as blank values and recorded in a JSON schema manifest. Source `Agent_Name` and `Post Code` are retained in the Snowflake raw and typed staging/target tables, while the canonical CSV remains the requested seven-column schema. Dates in the output CSVs use ISO `YYYY-MM-DD`; Snowflake columns use `DATE`.

Rows with missing required ID/name/enrollment date, invalid supplied dates, unsupported tier values, out-of-range post codes, conflicting row/file countries, or duplicate IDs within one file are written to the dated reject CSV. Accepted rows are written to `COUNTRY_members_YYYYMMDD.csv`. The command also generates a content-hash batch ID, a batch audit row, and Snowflake SQL to persist raw rows, normalized staging rows, schema inspection, rejects, and per-country member tables. It computes age and the `> 90` day stale flag using the ingestion date. The generated row-literal SQL is for small local assessment files; use the staged `COPY INTO` pattern for production-sized feeds.

```powershell
$env:PYTHONPATH = 'src'
python -m skypoints_etl.ingest_cli `
  --input-files 'C:\data\USA(in).csv' 'C:\data\IND(in).csv' 'C:\data\AUS(Sheet1).csv' `
  --output-dir outputs\ingested `
  --database SNOWFLAKE_LEARNING_DB --schema SKYPOINTS
```

Review the generated manifest and rejects before running the generated `snowflake_load_YYYYMMDD.sql` in the intended Snowflake development schema. Its files and row-level operations are idempotent for an identical content-hash batch, assuming serialized execution. Filename country mapping is used only when the member row has no country column; if a country field is present, it must agree with the filename. The supplied sample CSV IDs repeat across countries, so this file loader keeps IDs scoped to each country table. Global country-move resolution in `sql/02_transform.sql` requires a globally unique member ID and an authoritative source update timestamp/sequence.

No database credentials are needed to run the local Python reference. Run `python -m skypoints_etl.ingest_cli ...` and inspect `outputs/ingested/`: accepted country CSVs, `rejects_YYYYMMDD.csv`, `ingestion_manifest_YYYYMMDD.json`, and `snowflake_load_YYYYMMDD.sql`. The generic CLI writes `members.csv`, `redemptions.csv`, and `rejects.csv` to the directory passed through `--out`. Apply `sql/01_ddl.sql` and `sql/02_transform.sql` in a Snowflake development schema after setting the content-hash batch and business date. The generic SQL country allow-list currently contains USA/IND/AUS and must be expanded from an approved country catalog before global deployment.

## Data quality coverage

Required member name, ID, country, enrollment date; declared lengths; valid dates; future dates; DOB after enrollment; allowed active flags; optional-field nulls; exact stale cutoff (`>90`); stable member ID; transaction required fields, positive integer miles, known status, feed/transaction date relationship, duplicate IDs, unknown country and unmatched transactions. Keep rejects with reason and raw input. Production monitoring should add volume drift, duplicate rate, null/invalid rate, country distribution, referential orphan rate, late-arriving data, SLA and reconciliation (input = accepted + rejected).

## Assessment / interpretation notes

The assessment layout calls Post Code optional, while its sample header omits that column; code treats it as optional. Some sample member IDs are shorter than six digits despite the prose/table length definition, so they are preserved (maximum 18, not padded). `PHIL` and `AU` are mapped to `PHL` and `AUS`; production should use an approved ISO/country crosswalk. Snowflake standard-table primary keys are informational, so deduplication and DQ checks enforce uniqueness operationally. SQL contains an India target example; generate the other country targets from a controlled deployment-time country list, not arbitrary data values.

## Country migration, pipe feeds, and operating limits

The country-file SQL generator accepts either a country-coded CSV or the assessment's `|H|...` header followed by `|D|...` detail records. Pipe rows are checked against the header width; unexpected record types and malformed rows enter the dated reject output. In Snowflake, the generated SQL writes raw and normalized records to landing, inserts staging rows idempotently, and computes `AGE` and `STALE_MEMBER`.

For country moves, `MEMBER_FILE_CURRENT_OWNER` records the last known country. Before applying the current batch, the SQL records IDs already present in more than one country table and IDs repeated across countries in the batch in `MEMBER_FILE_ID_CONFLICT`. Those IDs are held for manual resolution. For unambiguous IDs, the SQL updates ownership, merges into the new country table, and deletes the old-country row in the same transaction. This assumes a serialized writer; Snowflake does not enforce the declared primary keys as uniqueness constraints. Do not run two country-loader batches concurrently against the same members without an external single-writer lock or a stream/task orchestration policy.

The generated script uses `QUALIFY ROW_NUMBER()` to select one source row per member and `CLUSTER BY` on batch/country/member keys for pruning. Clustering is a maintenance/pruning aid, not a partition or a substitute for warehouse sizing. The local generator embeds row literals for review-scale assessment files; do not use that SQL shape for billion-row ingestion. At production scale, stage compressed files in cloud storage, use Snowflake `COPY INTO` into append-only landing, transform with set-based SQL, and tune warehouse size, file sizing, concurrency, and clustering from query profiles. Configure an orchestration gate that stops promotion when the rejected-row rate exceeds the CLI's `--max-reject-rate` (default 20%); the CLI records a circuit-breaker status and does not emit a load script when the threshold is exceeded.
