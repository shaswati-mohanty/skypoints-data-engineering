"""Country-aware CSV normalization and Snowflake load-script generation."""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


STANDARD_COLUMNS = (
    "ID",
    "MEMBER_NAME",
    "DOB",
    "TIER_CODE",
    "ENROLLMENT_DATE",
    "IND_CORP",
    "FLIGHT_DATE",
)
DATE_COLUMNS = {"DOB", "ENROLLMENT_DATE", "FLIGHT_DATE"}
TIER_CODES = {"PLT", "SLV", "GLD"}
COUNTRY_ALIASES = {
    "US": "USA",
    "USA": "USA",
    "IN": "IND",
    "IND": "IND",
    "AU": "AUS",
    "AUS": "AUS",
    "PHIL": "PHL",
    "PHL": "PHL",
}
HEADER_ALIASES = {
    "ID": {"id", "memberid", "uniqueid", "membershipid"},
    "MEMBER_NAME": {"membername", "name"},
    "DOB": {"dob", "dateofbirth", "birthdate"},
    "TIER_CODE": {"tiercode", "tier", "tiertype"},
    "ENROLLMENT_DATE": {"enrollmentdate", "dateofenrollment", "enrolldt"},
    "IND_CORP": {"indcorp", "individualorcorporate", "individualcorporate", "membertype"},
    "FLIGHT_DATE": {"flightdate", "dateofflight", "lastflightdate", "flightdt"},
    "COUNTRY": {"country", "countrycode"},
}
NULL_VALUES = {"", "NULL", "NONE", "N/A", "NA"}


def _key(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


def _canonical_country(token: str) -> str:
    normalized = token.strip().upper()
    if normalized in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[normalized]
    if not re.fullmatch(r"[A-Z]{2,3}", normalized):
        raise ValueError(f"country token {token!r} is not a 2- or 3-letter country code")
    return normalized


def infer_country_code(filename: str) -> str:
    """Infer a country token at a filename boundary, e.g. AUS(Sheet1).csv."""
    stem = Path(filename).stem
    prefix = re.match(r"^([A-Za-z]{2,4})(?=[^A-Za-z]|$)", stem)
    if prefix:
        return _canonical_country(prefix.group(1))
    tokens = re.findall(r"(?<![A-Za-z])([A-Za-z]{2,3})(?![A-Za-z])", stem)
    mapped = []
    for token in tokens:
        try:
            code = _canonical_country(token)
        except ValueError:
            continue
        if code not in mapped:
            mapped.append(code)
    if not mapped:
        raise ValueError(f"no country code found in filename {filename!r}")
    if len(mapped) > 1:
        raise ValueError(f"ambiguous country codes in filename {filename!r}: {mapped}")
    return mapped[0]


def parse_source_date(value: Any) -> date | None:
    if value is None:
        return None
    raw = str(value).strip()
    if raw.upper() in NULL_VALUES:
        return None
    if re.fullmatch(r"\d{7,8}", raw):
        if len(raw) == 8 and raw[:4].isdigit() and 1900 <= int(raw[:4]) <= 2100:
            try:
                return datetime.strptime(raw, "%Y%m%d").date()
            except ValueError:
                pass
        year = int(raw[-4:])
        digits = raw[:-4]
        candidates = []
        for month_width in (1, 2):
            day_width = len(digits) - month_width
            if day_width not in (1, 2):
                continue
            candidates.append((int(digits[:month_width]), int(digits[month_width:])))
        for month, day_part in candidates:
            try:
                return date(year, month, day_part)
            except ValueError:
                continue
        raise ValueError(f"invalid date {raw!r}")
    formats = (
        "%Y-%m-%d",
        "%Y%m%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%m%d%Y",
        "%d/%m/%Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date {raw!r}")


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str | None]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"{path.name}: CSV has no header row")
        headers = [header.strip() if header else "" for header in reader.fieldnames]
        rows = []
        for row in reader:
            rows.append({(key.strip() if key else ""): value for key, value in row.items()})
        return headers, rows


def inspect_schema(headers: list[str]) -> dict[str, Any]:
    normalized = {_key(header): header for header in headers if header}
    mapping: dict[str, str] = {}
    for output_column, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            actual = normalized.get(_key(alias))
            if actual is not None:
                mapping[output_column] = actual
                break
    missing = [column for column in STANDARD_COLUMNS if column not in mapping]
    recognized = set(mapping.values())
    extra = [header for header in headers if header and header not in recognized]
    return {"source_headers": headers, "column_mapping": mapping, "missing_standard_columns": missing, "extra_source_columns": extra}


def _normalize_ind_corp(value: Any) -> str | None:
    if value is None or str(value).strip().upper() in NULL_VALUES:
        return None
    normalized = str(value).strip().upper()
    values = {"I": "I", "IND": "I", "INDIVIDUAL": "I", "C": "C", "CORP": "C", "CORPORATE": "C"}
    if normalized not in values:
        raise ValueError(f"invalid IND_CORP value {value!r}; expected I or C")
    return values[normalized]


def _normalize_row(
    raw: dict[str, str | None],
    schema: dict[str, Any],
    country: str,
    as_of: date,
) -> dict[str, str | date | None]:
    mapping: dict[str, str] = schema["column_mapping"]
    country_column = mapping.get("COUNTRY")
    source_country = raw.get(country_column) if country_column else None
    if source_country and source_country.strip().upper() not in NULL_VALUES:
        if _canonical_country(source_country) != country:
            raise ValueError(f"row country {source_country!r} conflicts with filename country {country}")

    normalized: dict[str, str | date | None] = {}
    for column in STANDARD_COLUMNS:
        source_column = mapping.get(column)
        value = raw.get(source_column) if source_column else None
        if isinstance(value, str):
            value = value.strip()
        if value is None or (isinstance(value, str) and value.upper() in NULL_VALUES):
            normalized[column] = None
        elif column in DATE_COLUMNS:
            try:
                normalized[column] = parse_source_date(value)
            except ValueError as exc:
                raise ValueError(f"{column}: {exc}") from exc
        elif column == "TIER_CODE":
            tier = str(value).strip().upper()
            if tier not in TIER_CODES:
                raise ValueError(f"invalid TIER_CODE {value!r}; expected PLT, SLV, or GLD")
            normalized[column] = tier
        elif column == "IND_CORP":
            normalized[column] = _normalize_ind_corp(value)
        else:
            normalized[column] = str(value).strip() if value is not None else None

    member_id = normalized["ID"]
    if member_id is not None and len(str(member_id)) > 18:
        raise ValueError("ID exceeds the 18-character limit")
    member_name = normalized["MEMBER_NAME"]
    if member_name is not None and len(str(member_name)) > 255:
        raise ValueError("MEMBER_NAME exceeds the 255-character limit")
    if member_id is None or member_name is None or normalized["ENROLLMENT_DATE"] is None:
        raise ValueError("missing mandatory ID, MEMBER_NAME, or ENROLLMENT_DATE")
    enrollment_date = normalized["ENROLLMENT_DATE"]
    dob = normalized["DOB"]
    flight_date = normalized["FLIGHT_DATE"]
    if isinstance(enrollment_date, date) and enrollment_date > as_of:
        raise ValueError("ENROLLMENT_DATE is in the future")
    if isinstance(dob, date) and dob > as_of:
        raise ValueError("DOB is in the future")
    if isinstance(flight_date, date) and flight_date > as_of:
        raise ValueError("FLIGHT_DATE is in the future")
    if isinstance(dob, date) and isinstance(enrollment_date, date) and dob > enrollment_date:
        raise ValueError("DOB is after ENROLLMENT_DATE")
    return normalized


def _raw_json(raw: dict[Any, Any]) -> str:
    return json.dumps({"" if key is None else str(key): value for key, value in raw.items()}, ensure_ascii=False, sort_keys=True)


@dataclass
class FileResult:
    source_file: str
    country_code: str
    target_table: str
    output_file: str
    source_headers: list[str]
    column_mapping: dict[str, str]
    missing_standard_columns: list[str]
    extra_source_columns: list[str]
    rows_read: int
    rows_accepted: int
    rows_rejected: int


@dataclass
class IngestionResult:
    ingested_on: date
    accepted_by_country: dict[str, list[dict[str, str | date | None]]]
    rejects: list[dict[str, Any]]
    files: list[FileResult]


def ingest_csv_files(
    input_files: list[Path],
    output_dir: Path,
    ingested_on: date | None = None,
) -> IngestionResult:
    """Normalize country-named CSVs and persist dated standard-schema files."""
    ingest_date = ingested_on or date.today()
    date_suffix = ingest_date.strftime("%Y%m%d")
    output_dir.mkdir(parents=True, exist_ok=True)
    accepted_by_country: dict[str, list[dict[str, str | date | None]]] = defaultdict(list)
    rejects: list[dict[str, Any]] = []
    file_results: list[FileResult] = []

    for path in sorted(input_files):
        country = infer_country_code(path.name)
        accepted_by_country[country]  # initialize an empty output even if every row is rejected
        headers, rows = _read_csv(path)
        schema = inspect_schema(headers)
        accepted_count = 0
        seen_ids: set[str] = set()
        for source_row_number, raw in enumerate(rows, start=2):
            try:
                row = _normalize_row(raw, schema, country, ingest_date)
                member_id = str(row["ID"])
                if member_id in seen_ids:
                    raise ValueError(f"duplicate ID {member_id!r} within source file")
                seen_ids.add(member_id)
                row["_SOURCE_FILE"] = path.name
                row["_SOURCE_ROW_NUMBER"] = source_row_number
                row["_RAW_RECORD"] = _raw_json(raw)
                accepted_by_country[country].append(row)
                accepted_count += 1
            except (ValueError, TypeError) as exc:
                rejects.append({
                    "COUNTRY_CODE": country,
                    "SOURCE_FILE": path.name,
                    "SOURCE_ROW_NUMBER": source_row_number,
                    "REASON_CODE": "INVALID_SOURCE_ROW",
                    "REASON_DETAIL": str(exc),
                    "RAW_RECORD": _raw_json(raw),
                })

        output_file = f"{path.stem}_{date_suffix}.csv"
        file_results.append(FileResult(
            source_file=path.name,
            country_code=country,
            target_table=f"MEMBER_{country}",
            output_file=output_file,
            source_headers=headers,
            column_mapping=schema["column_mapping"],
            missing_standard_columns=schema["missing_standard_columns"],
            extra_source_columns=schema["extra_source_columns"],
            rows_read=len(rows),
            rows_accepted=accepted_count,
            rows_rejected=len(rows) - accepted_count,
        ))

    for country, rows in accepted_by_country.items():
        # One dated country output is emitted even if multiple source files map to it.
        sources = [item for item in file_results if item.country_code == country]
        output_file = f"{country}_members_{date_suffix}.csv"
        with (output_dir / output_file).open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=STANDARD_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    key: (value.isoformat() if isinstance(value, date) else (value or ""))
                    for key, value in row.items()
                })
        for item in sources:
            item.output_file = output_file

    with (output_dir / f"rejects_{date_suffix}.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=("COUNTRY_CODE", "SOURCE_FILE", "SOURCE_ROW_NUMBER", "REASON_CODE", "REASON_DETAIL", "RAW_RECORD"))
        writer.writeheader()
        writer.writerows(rejects)

    manifest = {
        "ingested_on": ingest_date.isoformat(),
        "standard_columns": list(STANDARD_COLUMNS),
        "file_results": [asdict(item) for item in file_results],
        "accepted_rows": sum(len(rows) for rows in accepted_by_country.values()),
        "rejected_rows": len(rejects),
        "reject_file": f"rejects_{date_suffix}.csv",
    }
    (output_dir / f"ingestion_manifest_{date_suffix}.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return IngestionResult(ingest_date, dict(accepted_by_country), rejects, file_results)


def _sql_string(value: Any) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def generate_snowflake_load_sql(result: IngestionResult, output_file: Path, database: str, schema: str) -> None:
    """Generate idempotent DDL + INSERT statements for normalized local batch output."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", database) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", schema):
        raise ValueError("database and schema must be simple Snowflake identifiers")
    qualified_schema = f'"{database.upper()}"."{schema.upper()}"'
    date_suffix = result.ingested_on.strftime("%Y%m%d")
    ingest_date = result.ingested_on.isoformat()
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {qualified_schema};",
        f"USE DATABASE \"{database.upper()}\";",
        f"USE SCHEMA \"{schema.upper()}\";",
        "CREATE TABLE IF NOT EXISTS MEMBER_FILE_LANDING (BATCH_ID VARCHAR NOT NULL, COUNTRY_CODE VARCHAR(3) NOT NULL, SOURCE_FILE VARCHAR NOT NULL, SOURCE_ROW_NUMBER NUMBER NOT NULL, INGEST_DATE DATE NOT NULL, INGESTED_AT TIMESTAMP_NTZ NOT NULL, RAW_RECORD VARIANT NOT NULL, NORMALIZED_RECORD VARIANT NOT NULL, CONSTRAINT PK_MEMBER_FILE_LANDING PRIMARY KEY (BATCH_ID,COUNTRY_CODE,SOURCE_FILE,SOURCE_ROW_NUMBER) NOT ENFORCED);",
        "CREATE TABLE IF NOT EXISTS MEMBER_FILE_STAGING (COUNTRY_CODE VARCHAR(3) NOT NULL, ID VARCHAR(18) NOT NULL, MEMBER_NAME VARCHAR(255) NOT NULL, DOB DATE, TIER_CODE CHAR(3), ENROLLMENT_DATE DATE, IND_CORP CHAR(1), FLIGHT_DATE DATE, BATCH_ID VARCHAR NOT NULL, SOURCE_FILE VARCHAR NOT NULL, SOURCE_ROW_NUMBER NUMBER NOT NULL, INGEST_DATE DATE NOT NULL, INGESTED_AT TIMESTAMP_NTZ NOT NULL, AGE NUMBER(3,0), STALE_MEMBER BOOLEAN NOT NULL);",
        "CREATE TABLE IF NOT EXISTS FILE_DQ_REJECT (BATCH_ID VARCHAR NOT NULL, COUNTRY_CODE VARCHAR(3) NOT NULL, SOURCE_FILE VARCHAR NOT NULL, SOURCE_ROW_NUMBER NUMBER NOT NULL, REASON_CODE VARCHAR NOT NULL, REASON_DETAIL VARCHAR NOT NULL, RAW_RECORD VARIANT NOT NULL, INGEST_DATE DATE NOT NULL, REJECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());",
        "CREATE TABLE IF NOT EXISTS FILE_SCHEMA_AUDIT (BATCH_ID VARCHAR NOT NULL, COUNTRY_CODE VARCHAR(3) NOT NULL, SOURCE_FILE VARCHAR NOT NULL, SOURCE_HEADERS VARIANT NOT NULL, COLUMN_MAPPING VARIANT NOT NULL, MISSING_STANDARD_COLUMNS VARIANT NOT NULL, EXTRA_SOURCE_COLUMNS VARIANT NOT NULL, ROWS_READ NUMBER NOT NULL, ROWS_ACCEPTED NUMBER NOT NULL, ROWS_REJECTED NUMBER NOT NULL, INSPECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());",
    ]
    for file_result in result.files:
        schema_values = [
            _sql_string(date_suffix),
            _sql_string(file_result.country_code),
            _sql_string(file_result.source_file),
            f"PARSE_JSON('{json.dumps(file_result.source_headers).replace(chr(39), chr(39) * 2)}')",
            f"PARSE_JSON('{json.dumps(file_result.column_mapping).replace(chr(39), chr(39) * 2)}')",
            f"PARSE_JSON('{json.dumps(file_result.missing_standard_columns).replace(chr(39), chr(39) * 2)}')",
            f"PARSE_JSON('{json.dumps(file_result.extra_source_columns).replace(chr(39), chr(39) * 2)}')",
            str(file_result.rows_read), str(file_result.rows_accepted), str(file_result.rows_rejected),
        ]
        statements.append(
            "INSERT INTO FILE_SCHEMA_AUDIT (BATCH_ID,COUNTRY_CODE,SOURCE_FILE,SOURCE_HEADERS,COLUMN_MAPPING,MISSING_STANDARD_COLUMNS,EXTRA_SOURCE_COLUMNS,ROWS_READ,ROWS_ACCEPTED,ROWS_REJECTED) SELECT "
            + ",".join(schema_values)
            + f" WHERE NOT EXISTS (SELECT 1 FROM FILE_SCHEMA_AUDIT WHERE BATCH_ID='{date_suffix}' AND SOURCE_FILE={_sql_string(file_result.source_file)});"
        )
    for country, rows in sorted(result.accepted_by_country.items()):
        table = f"MEMBER_{country}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {table} (ID VARCHAR(18) NOT NULL, MEMBER_NAME VARCHAR(255) NOT NULL, DOB DATE, TIER_CODE CHAR(3), ENROLLMENT_DATE DATE, IND_CORP CHAR(1), FLIGHT_DATE DATE, INGEST_DATE DATE NOT NULL, SOURCE_FILE VARCHAR NOT NULL, SOURCE_ROW_NUMBER NUMBER NOT NULL, INGESTED_AT TIMESTAMP_NTZ NOT NULL, AGE NUMBER(3,0), STALE_MEMBER BOOLEAN NOT NULL, CONSTRAINT PK_{table} PRIMARY KEY (ID) NOT ENFORCED);"
        )
        landing_rows = []
        for row in rows:
            normalized = {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in row.items() if key in STANDARD_COLUMNS
            }
            source_file = str(row["_SOURCE_FILE"])
            source_row = int(row["_SOURCE_ROW_NUMBER"])
            landing_columns = ("BATCH_ID", "COUNTRY_CODE", "SOURCE_FILE", "SOURCE_ROW_NUMBER", "INGEST_DATE", "INGESTED_AT", "RAW_RECORD", "NORMALIZED_RECORD")
            landing_rows.append(
                "SELECT " + ",".join(f"{value} AS {column}" for value, column in zip([
                    _sql_string(date_suffix), _sql_string(country), _sql_string(source_file), str(source_row),
                    f"TO_DATE('{ingest_date}')", "CURRENT_TIMESTAMP()",
                    f"PARSE_JSON('{str(row['_RAW_RECORD']).replace(chr(39), chr(39) * 2)}')",
                    f"PARSE_JSON('{json.dumps(normalized, ensure_ascii=False).replace(chr(39), chr(39) * 2)}')",
                ], landing_columns))
            )
        if landing_rows:
            statements.append(
                "INSERT INTO MEMBER_FILE_LANDING (BATCH_ID,COUNTRY_CODE,SOURCE_FILE,SOURCE_ROW_NUMBER,INGEST_DATE,INGESTED_AT,RAW_RECORD,NORMALIZED_RECORD) SELECT src.* FROM ("
                + " UNION ALL ".join(landing_rows)
                + f") src WHERE NOT EXISTS (SELECT 1 FROM MEMBER_FILE_LANDING existing WHERE existing.BATCH_ID=src.BATCH_ID AND existing.COUNTRY_CODE=src.COUNTRY_CODE AND existing.SOURCE_FILE=src.SOURCE_FILE AND existing.SOURCE_ROW_NUMBER=src.SOURCE_ROW_NUMBER);"
            )
        dob = "TRY_TO_DATE(l.NORMALIZED_RECORD:DOB::VARCHAR)"
        flight_date = "TRY_TO_DATE(l.NORMALIZED_RECORD:FLIGHT_DATE::VARCHAR)"
        as_of = f"TO_DATE('{ingest_date}')"
        age_expr = f"IFF({dob} IS NULL,NULL,DATEDIFF(year,{dob},{as_of})-IFF(TO_CHAR({as_of},'MMDD')<TO_CHAR({dob},'MMDD'),1,0))"
        stale_expr = f"COALESCE(DATEDIFF(day,{flight_date},{as_of})>90,FALSE)"
        normalized_select = [
            "l.COUNTRY_CODE", "l.NORMALIZED_RECORD:ID::VARCHAR", "l.NORMALIZED_RECORD:MEMBER_NAME::VARCHAR",
            dob, "l.NORMALIZED_RECORD:TIER_CODE::VARCHAR", "TRY_TO_DATE(l.NORMALIZED_RECORD:ENROLLMENT_DATE::VARCHAR)",
            "l.NORMALIZED_RECORD:IND_CORP::VARCHAR", flight_date, "l.BATCH_ID", "l.SOURCE_FILE", "l.SOURCE_ROW_NUMBER",
            "l.INGEST_DATE", "l.INGESTED_AT", age_expr, stale_expr,
        ]
        statements.append(
            "INSERT INTO MEMBER_FILE_STAGING (COUNTRY_CODE,ID,MEMBER_NAME,DOB,TIER_CODE,ENROLLMENT_DATE,IND_CORP,FLIGHT_DATE,BATCH_ID,SOURCE_FILE,SOURCE_ROW_NUMBER,INGEST_DATE,INGESTED_AT,AGE,STALE_MEMBER) SELECT "
            + ",".join(normalized_select)
            + f" FROM MEMBER_FILE_LANDING l WHERE l.BATCH_ID='{date_suffix}' AND l.COUNTRY_CODE='{country}' AND NOT EXISTS (SELECT 1 FROM MEMBER_FILE_STAGING s WHERE s.BATCH_ID=l.BATCH_ID AND s.COUNTRY_CODE=l.COUNTRY_CODE AND s.SOURCE_FILE=l.SOURCE_FILE AND s.SOURCE_ROW_NUMBER=l.SOURCE_ROW_NUMBER);"
        )
        statements.append(
            f"MERGE INTO {table} t USING (SELECT * FROM MEMBER_FILE_STAGING WHERE COUNTRY_CODE='{country}' AND BATCH_ID='{date_suffix}' QUALIFY ROW_NUMBER() OVER(PARTITION BY ID ORDER BY INGESTED_AT DESC,SOURCE_FILE DESC,SOURCE_ROW_NUMBER DESC)=1) s ON t.ID=s.ID "
            "WHEN MATCHED AND s.INGESTED_AT>=t.INGESTED_AT THEN UPDATE SET "
            "MEMBER_NAME=s.MEMBER_NAME,DOB=s.DOB,TIER_CODE=s.TIER_CODE,ENROLLMENT_DATE=s.ENROLLMENT_DATE,IND_CORP=s.IND_CORP,FLIGHT_DATE=s.FLIGHT_DATE,INGEST_DATE=s.INGEST_DATE,SOURCE_FILE=s.SOURCE_FILE,SOURCE_ROW_NUMBER=s.SOURCE_ROW_NUMBER,INGESTED_AT=s.INGESTED_AT,AGE=s.AGE,STALE_MEMBER=s.STALE_MEMBER "
            "WHEN NOT MATCHED THEN INSERT (ID,MEMBER_NAME,DOB,TIER_CODE,ENROLLMENT_DATE,IND_CORP,FLIGHT_DATE,INGEST_DATE,SOURCE_FILE,SOURCE_ROW_NUMBER,INGESTED_AT,AGE,STALE_MEMBER) VALUES (s.ID,s.MEMBER_NAME,s.DOB,s.TIER_CODE,s.ENROLLMENT_DATE,s.IND_CORP,s.FLIGHT_DATE,s.INGEST_DATE,s.SOURCE_FILE,s.SOURCE_ROW_NUMBER,s.INGESTED_AT,s.AGE,s.STALE_MEMBER);"
        )
    if result.rejects:
        rejected_rows = []
        for rejected in result.rejects:
            raw = rejected["RAW_RECORD"].replace("'", "''")
            reject_columns = ("BATCH_ID", "COUNTRY_CODE", "SOURCE_FILE", "SOURCE_ROW_NUMBER", "REASON_CODE", "REASON_DETAIL", "RAW_RECORD", "INGEST_DATE")
            rejected_rows.append(
                "SELECT " + ",".join(f"{value} AS {column}" for value, column in zip([
                    _sql_string(date_suffix), _sql_string(rejected["COUNTRY_CODE"]),
                    _sql_string(rejected["SOURCE_FILE"]), str(rejected["SOURCE_ROW_NUMBER"]),
                    _sql_string(rejected["REASON_CODE"]), _sql_string(rejected["REASON_DETAIL"]),
                    f"PARSE_JSON('{raw}')", f"TO_DATE('{ingest_date}')",
                ], reject_columns))
            )
        statements.append(
            "INSERT INTO FILE_DQ_REJECT (BATCH_ID,COUNTRY_CODE,SOURCE_FILE,SOURCE_ROW_NUMBER,REASON_CODE,REASON_DETAIL,RAW_RECORD,INGEST_DATE) SELECT src.* FROM ("
            + " UNION ALL ".join(rejected_rows)
            + f") src WHERE NOT EXISTS (SELECT 1 FROM FILE_DQ_REJECT existing WHERE existing.BATCH_ID=src.BATCH_ID AND existing.SOURCE_FILE=src.SOURCE_FILE AND existing.SOURCE_ROW_NUMBER=src.SOURCE_ROW_NUMBER);"
        )
    for country in sorted(result.accepted_by_country):
        table = f"MEMBER_{country}"
        statements.append(f"SELECT '{table}' AS TARGET_TABLE, COUNT(*) AS ROW_COUNT, COUNT_IF(DOB IS NULL) AS NULL_DOB_ROWS, COUNT_IF(STALE_MEMBER) AS STALE_ROWS FROM {table} WHERE INGEST_DATE=TO_DATE('{result.ingested_on.isoformat()}');")
    statements.append(f"SELECT COUNTRY_CODE,SOURCE_FILE,SOURCE_ROW_NUMBER,REASON_CODE,REASON_DETAIL FROM FILE_DQ_REJECT WHERE BATCH_ID='{date_suffix}' ORDER BY SOURCE_FILE,SOURCE_ROW_NUMBER;")
    statements.append(f"SELECT COUNTRY_CODE,SOURCE_FILE,SOURCE_HEADERS,MISSING_STANDARD_COLUMNS,ROWS_READ,ROWS_ACCEPTED,ROWS_REJECTED FROM FILE_SCHEMA_AUDIT WHERE BATCH_ID='{date_suffix}' ORDER BY SOURCE_FILE;")
    output_file.write_text("\n\n".join(statements) + "\n", encoding="utf-8")
