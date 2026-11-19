"""CLI for normalizing country-coded member CSV files and preparing Snowflake SQL."""

from __future__ import annotations

import argparse
import json
import sys
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
    parser.add_argument("--max-reject-rate", type=float, default=0.20,
                        help="Circuit-breaker limit from 0 to 1; SQL is not generated when exceeded")
    args = parser.parse_args()

    if not 0 <= args.max_reject_rate <= 1:
        parser.error("--max-reject-rate must be between 0 and 1")

    result = ingest_csv_files(args.input_files, args.output_dir, args.ingested_on)
    sql_path = args.output_dir / f"snowflake_load_{args.ingested_on:%Y%m%d}.sql"
    rows_read = sum(item.rows_read for item in result.files)
    reject_rate = len(result.rejects) / rows_read if rows_read else 0.0
    manifest_path = args.output_dir / f"ingestion_manifest_{args.ingested_on:%Y%m%d}.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["reject_rate"] = reject_rate
    manifest["max_reject_rate"] = args.max_reject_rate
    manifest["circuit_breaker"] = "TRIPPED" if reject_rate > args.max_reject_rate else "CLEAR"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if reject_rate > args.max_reject_rate:
        print(json.dumps({
            "status": "CIRCUIT_BREAKER_TRIPPED",
            "rows_read": rows_read,
            "rows_rejected": len(result.rejects),
            "reject_rate": reject_rate,
            "max_reject_rate": args.max_reject_rate,
            "manifest": str(manifest_path),
            "reject_file": str(args.output_dir / f"rejects_{args.ingested_on:%Y%m%d}.csv"),
            "snowflake_sql_generated": False,
        }, indent=2))
        sys.exit(2)
    generate_snowflake_load_sql(result, sql_path, args.database, args.schema)
    print(json.dumps({
        "ingested_on": args.ingested_on.isoformat(),
        "batch_id": result.batch_id,
        "accepted_rows": sum(len(rows) for rows in result.accepted_by_country.values()),
        "rejected_rows": len(result.rejects),
        "reject_rate": reject_rate,
        "circuit_breaker": "CLEAR",
        "targets": {f"MEMBER_{country}": len(rows) for country, rows in sorted(result.accepted_by_country.items())},
        "outputs": [str(path) for path in sorted(args.output_dir.iterdir())],
        "snowflake_sql": str(sql_path),
    }, indent=2))


if __name__ == "__main__":
    main()
