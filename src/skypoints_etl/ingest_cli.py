"""CLI for normalizing country-coded member CSV files and preparing Snowflake SQL."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .file_ingestion import generate_snowflake_load_sql, ingest_csv_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize country-coded SkyPoints member CSV files")
    parser.add_argument("--input-files", nargs="+", type=Path, required=True, help="CSV paths with a country code in each filename")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for dated canonical CSV, reject CSV, manifest, and Snowflake SQL")
    parser.add_argument("--ingested-on", type=date.fromisoformat, default=date.today(), help="Ingestion date YYYY-MM-DD; defaults to today")
    parser.add_argument("--database", default="SNOWFLAKE_LEARNING_DB")
    parser.add_argument("--schema", default="SKYPOINTS")
    args = parser.parse_args()

    result = ingest_csv_files(args.input_files, args.output_dir, args.ingested_on)
    sql_path = args.output_dir / f"snowflake_load_{args.ingested_on:%Y%m%d}.sql"
    generate_snowflake_load_sql(result, sql_path, args.database, args.schema)
    print(json.dumps({
        "ingested_on": args.ingested_on.isoformat(),
        "accepted_rows": sum(len(rows) for rows in result.accepted_by_country.values()),
        "rejected_rows": len(result.rejects),
        "targets": {f"MEMBER_{country}": len(rows) for country, rows in sorted(result.accepted_by_country.items())},
        "outputs": [str(path) for path in sorted(args.output_dir.iterdir())],
        "snowflake_sql": str(sql_path),
    }, indent=2))


if __name__ == "__main__":
    main()
