from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

DATE_FORMATS = ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%m%d%Y")
ALIASES = {
    "member_name": ("member_name", "name"), "member_id": ("member_id", "mem_id", "id", "unique_id"),
    "enrollment_date": ("enrollment_date", "enroll_dt", "date_of_enrollment"),
    "last_flight_date": ("last_flight_date", "flight_dt", "flight_date", "date_of_flight"),
    "tier_code": ("tier_code", "tier", "tier_type"), "agent_name": ("agent_name",),
    "state": ("state",), "country": ("country", "county"), "postcode": ("post_code", "postcode"),
    "dob": ("dob", "date_of_birth"), "is_active": ("is_active", "flag", "active_member"),
}
COUNTRY_CODES = {"USA": "USA", "US": "USA", "IND": "IND", "IN": "IND", "AU": "AUS", "AUS": "AUS", "PHIL": "PHL", "CAN": "CAN"}

@dataclass
class Result:
    members: list[dict[str, Any]]
    redemptions: list[dict[str, Any]]
    rejects: list[dict[str, Any]]

def parse_date(value: Any) -> date | None:
    if value is None or str(value).strip().upper() in ("", "NULL", "NONE"):
        return None
    raw = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"invalid date: {raw}")

def _key(s: str) -> str:
    return "".join(c.lower() for c in s if c.isalnum())

def _normalize_header(header: Iterable[str]) -> dict[str, str]:
    lookup = {_key(alias): canonical for canonical, aliases in ALIASES.items() for alias in aliases}
    return {lookup[_key(h)]: h for h in header if _key(h) in lookup}

def _records(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        first = f.readline(); f.seek(0)
        if first.startswith("|H|") or first.startswith("|D|"):
            header = None; out = []
            for line_no, line in enumerate(f, 1):
                parts = line.rstrip("\r\n").split("|")
                parts = [x for x in parts if x != ""]
                if not parts: continue
                kind, *values = parts
                if kind == "H": header = values; continue
                if kind != "D" or header is None:
                    out.append({"__error": f"line {line_no}: detail before header or unknown record type"}); continue
                out.append(dict(zip(header, values)))
            return out
        return list(csv.DictReader(f))

def normalize_member(raw: dict[str, str], as_of: date) -> dict[str, Any]:
    if "__error" in raw: raise ValueError(raw["__error"])
    cols = _normalize_header(raw)
    def get(name: str) -> str | None:
        actual = cols.get(name)
        return raw.get(actual, "").strip() if actual else None
    name, member_id, country = get("member_name"), get("member_id"), get("country")
    enroll = parse_date(get("enrollment_date")); flight = parse_date(get("last_flight_date")); dob = parse_date(get("dob"))
    if not name or not member_id or not country or enroll is None:
        raise ValueError("missing mandatory member_name, member_id, country, or enrollment_date")
    if len(name) > 255 or len(member_id) > 18 or len(country) > 5:
        raise ValueError("field exceeds declared length")
    if enroll > as_of or (flight and flight > as_of) or (dob and dob > as_of):
        raise ValueError("date is in the future")
    if dob and dob > enroll: raise ValueError("DOB is after enrollment date")
    active = get("is_active")
    if active and active.upper() not in ("A", "I", "Y", "N", "1", "0"): raise ValueError("invalid active flag")
    age = as_of.year - dob.year - ((as_of.month, as_of.day) < (dob.month, dob.day)) if dob else None
    return {"member_name": name, "member_id": member_id, "enrollment_date": enroll.isoformat(),
            "last_flight_date": flight.isoformat() if flight else None, "tier_code": get("tier_code"),
            "agent_name": get("agent_name"), "state": get("state"), "country": COUNTRY_CODES.get(country.upper(), country.upper()),
            "postcode": get("postcode"), "dob": dob.isoformat() if dob else None, "is_active": active.upper() if active else None,
            "age": age, "stale_member": bool(flight and (as_of-flight).days > 90), "source_updated_at": as_of.isoformat()}

def normalize_redemption_payload(payload: Any, as_of: date) -> list[dict[str, Any]]:
    if isinstance(payload, dict): payload = [payload]
    if not isinstance(payload, list): raise ValueError("redemption feed must be an object or array")
    out = []
    for feed in payload:
        if not isinstance(feed, dict): raise ValueError("feed entry must be an object")
        member_id = str(feed.get("member_id", "")).strip()
        feed_date = parse_date(feed.get("feed_date"))
        if not member_id or feed_date is None: raise ValueError("missing member_id or feed_date")
        if feed_date > as_of: raise ValueError("feed_date is in the future")
        txns = feed.get("redemptions", [])
        if not isinstance(txns, list): raise ValueError("redemptions must be an array")
        for txn in txns:
            if not isinstance(txn, dict): raise ValueError("transaction must be an object")
            tid = str(txn.get("txn_id", "")).strip(); partner = str(txn.get("partner", "")).strip()
            txn_date = parse_date(txn.get("txn_date")); miles = txn.get("miles_redeemed"); status = str(txn.get("status", "")).upper()
            if not tid or not partner or txn_date is None or miles is None or not status: raise ValueError("transaction missing required field")
            if txn_date > feed_date or txn_date > as_of: raise ValueError("transaction date after feed date/as-of date")
            if isinstance(miles, bool) or not isinstance(miles, int) or miles <= 0: raise ValueError("miles_redeemed must be a positive integer")
            if status not in ("COMPLETED", "PENDING", "REJECTED", "CANCELLED"): raise ValueError("unknown transaction status")
            out.append({"txn_id": tid, "member_id": member_id, "feed_date": feed_date.isoformat(), "txn_date": txn_date.isoformat(), "partner": partner, "miles_redeemed": miles, "status": status})
    return out

def run_pipeline(member_rows: Iterable[dict[str, str]], redemption_payload: Any, as_of: str) -> Result:
    business_date = date.fromisoformat(as_of)
    rejects: list[dict[str, Any]] = []; by_id: dict[str, dict[str, Any]] = {}
    for row_num, raw in enumerate(member_rows, 1):
        try:
            record = normalize_member(raw, business_date)
            record["source_sequence"] = row_num
            existing = by_id.get(record["member_id"])
            if existing:
                # Feed order is source order. Equal-date conflicting records are ambiguous and rejected.
                if record["source_updated_at"] == existing["source_updated_at"] and record == existing:
                    continue
            by_id[record["member_id"]] = record
        except (ValueError, TypeError) as exc:
            rejects.append({"feed": "member", "row": row_num, "reason": str(exc), "record": json.dumps(raw, default=str)})
    try:
        transactions = normalize_redemption_payload(redemption_payload, business_date)
    except (ValueError, TypeError) as exc:
        transactions = []; rejects.append({"feed": "redemption", "row": None, "reason": str(exc), "record": ""})
    seen = set(); unique = []
    for txn in transactions:
        if txn["txn_id"] in seen:
            rejects.append({"feed": "redemption", "row": txn["txn_id"], "reason": "duplicate txn_id", "record": json.dumps(txn)})
        else: seen.add(txn["txn_id"]); unique.append(txn)
    return Result(list(by_id.values()), unique, rejects)

def process_files(member_path: Path, redemption_path: Path, as_of: str) -> Result:
    rows = _records(member_path); payload = json.loads(redemption_path.read_text(encoding="utf-8-sig"))
    return run_pipeline(rows, payload, as_of)
