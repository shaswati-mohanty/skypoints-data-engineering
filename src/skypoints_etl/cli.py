from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from .pipeline import process_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and normalize SkyPoints feeds")
    parser.add_argument("--members", type=Path, required=True)
    parser.add_argument("--redemptions", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--as-of", required=True, help="Business date YYYY-MM-DD")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    result = process_files(args.members, args.redemptions, args.as_of)
    outputs = (
        ("members.csv", result.members, ("member_name", "member_id", "enrollment_date", "last_flight_date", "tier_code", "agent_name", "state", "country", "postcode", "dob", "is_active", "age", "stale_member", "source_updated_at", "source_sequence")),
        ("redemptions.csv", result.redemptions, ("txn_id", "member_id", "feed_date", "txn_date", "partner", "miles_redeemed", "status")),
        ("rejects.csv", result.rejects, ("feed", "row", "reason", "record")),
    )
    for name, rows, empty_fields in outputs:
        with (args.out / name).open("w", newline="", encoding="utf-8") as f:
            fieldnames = list(dict.fromkeys(key for row in rows for key in row)) or list(empty_fields)
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    print(json.dumps({"members": len(result.members), "redemptions": len(result.redemptions), "rejects": len(result.rejects)}))

if __name__ == "__main__":
    main()
