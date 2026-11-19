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
    for name, rows in (("members.csv", result.members), ("redemptions.csv", result.redemptions), ("rejects.csv", result.rejects)):
        with (args.out / name).open("w", newline="", encoding="utf-8") as f:
            if rows:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader(); writer.writerows(rows)
    print(json.dumps({"members": len(result.members), "redemptions": len(result.redemptions), "rejects": len(result.rejects)}))

if __name__ == "__main__":
    main()
