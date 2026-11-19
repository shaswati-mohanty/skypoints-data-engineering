import csv
from datetime import date
import json
from pathlib import Path

from skypoints_etl.file_ingestion import (
    STANDARD_COLUMNS,
    generate_snowflake_load_sql,
    ingest_csv_files,
    inspect_schema,
    infer_country_code,
)


def write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


def test_country_codes_in_filenames_and_aliases():
    assert infer_country_code("USA(in).csv") == "USA"
    assert infer_country_code("IND(in).csv") == "IND"
    assert infer_country_code("AUS(Sheet1).csv") == "AUS"
    assert infer_country_code("members_AU_20261120.csv") == "AUS"
    assert infer_country_code("PHIL_members.csv") == "PHL"


def test_inspection_reports_missing_and_extra_columns():
    report = inspect_schema(["ID", "Name", "FlightDate", "SomeFlag"])
    assert report["column_mapping"] == {"ID": "ID", "MEMBER_NAME": "Name", "FLIGHT_DATE": "FlightDate"}
    assert report["missing_standard_columns"] == ["DOB", "TIER_CODE", "ENROLLMENT_DATE", "IND_CORP"]
    assert report["extra_source_columns"] == ["SomeFlag"]


def test_ingests_aliases_and_routes_invalid_date_to_rejects(tmp_path: Path):
    source = tmp_path / "AUS(Sheet1).csv"
    write_csv(source, ["Unique ID", "Member Name", "Tier Type", "Date of Birth", "Date of Enrollment", "Date of Flight"], [
        ["1", "Mike", "plt", "NULL", "2022-05-11", "2022-08-01"],
        ["2", "Jonnathan", "GLD", "1997-12-13", "2021-13-13", "2022-01-05"],
    ])
    output_dir = tmp_path / "normalized"
    result = ingest_csv_files([source], output_dir, date(2026, 11, 20))

    assert len(result.accepted_by_country["AUS"]) == 1
    assert result.accepted_by_country["AUS"][0]["DOB"] is None
    assert result.accepted_by_country["AUS"][0]["TIER_CODE"] == "PLT"
    assert result.accepted_by_country["AUS"][0]["ENROLLMENT_DATE"] == date(2022, 5, 11)
    assert len(result.rejects) == 1
    assert "ENROLLMENT_DATE" in result.rejects[0]["REASON_DETAIL"]
    assert (output_dir / "AUS_members_20261120.csv").exists()
    assert (output_dir / "rejects_20261120.csv").exists()
    manifest = json.loads((output_dir / "ingestion_manifest_20261120.json").read_text(encoding="utf-8"))
    assert manifest["file_results"][0]["target_table"] == "MEMBER_AUS"
    with (output_dir / "AUS_members_20261120.csv").open(encoding="utf-8", newline="") as stream:
        normalized = list(csv.DictReader(stream))
    assert tuple(normalized[0]) == STANDARD_COLUMNS
    assert normalized[0]["ENROLLMENT_DATE"] == "2022-05-11"


def test_missing_optional_columns_are_null_and_tier_corp_are_normalized(tmp_path: Path):
    source = tmp_path / "USA.csv"
    write_csv(source, ["ID", "Name", "TierCode", "EnrollmentDate", "FlightDate", "Individual or Corporate"], [[
        "1", "Sam", "slv", "6152022", "8202022", "Individual",
    ]])
    result = ingest_csv_files([source], tmp_path / "out", date(2026, 11, 20))
    row = result.accepted_by_country["USA"][0]
    assert row["DOB"] is None
    assert row["TIER_CODE"] == "SLV"
    assert row["IND_CORP"] == "I"
    assert row["ENROLLMENT_DATE"] == date(2022, 6, 15)
    assert row["FLIGHT_DATE"] == date(2022, 8, 20)


def test_mandatory_enrollment_and_temporal_consistency_are_rejected(tmp_path: Path):
    source = tmp_path / "USA_members.csv"
    write_csv(source, ["ID", "Name", "EnrollmentDate", "DOB", "FlightDate"], [
        ["1", "Missing enrollment", "", "1985-03-05", "2022-08-20"],
        ["2", "Future DOB", "2022-06-15", "2027-01-01", "2022-08-20"],
        ["3", "DOB after enrollment", "2022-06-15", "2022-07-01", "2022-08-20"],
        ["4", "Future flight", "2022-06-15", "1985-03-05", "2027-08-20"],
    ])
    result = ingest_csv_files([source], tmp_path / "out", date(2026, 11, 20))
    assert result.accepted_by_country["USA"] == []
    assert len(result.rejects) == 4
    assert "ENROLLMENT_DATE" in result.rejects[0]["REASON_DETAIL"]
    assert "DOB is in the future" in result.rejects[1]["REASON_DETAIL"]
    assert "DOB is after ENROLLMENT_DATE" in result.rejects[2]["REASON_DETAIL"]
    assert "FLIGHT_DATE is in the future" in result.rejects[3]["REASON_DETAIL"]


def test_snowflake_script_has_typed_dates_staging_and_reject_load(tmp_path: Path):
    source = tmp_path / "AUS(Sheet1).csv"
    write_csv(source, ["ID", "Name", "TierCode", "EnrollmentDate"], [
        ["1", "O'Neil", "GLD", "2022-05-11"],
        ["2", "Broken", "GLD", "2021-13-13"],
    ])
    result = ingest_csv_files([source], tmp_path / "normalized", date(2026, 11, 20))
    sql_path = tmp_path / "load.sql"
    generate_snowflake_load_sql(result, sql_path, "SNOWFLAKE_LEARNING_DB", "SKYPOINTS")
    sql = sql_path.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS MEMBER_FILE_LANDING" in sql
    assert "CREATE TABLE IF NOT EXISTS MEMBER_FILE_STAGING" in sql
    assert "MERGE INTO MEMBER_AUS" in sql
    assert '"ENROLLMENT_DATE": "2022-05-11"' in sql
    assert "TRY_TO_DATE(l.NORMALIZED_RECORD:ENROLLMENT_DATE::VARCHAR)" in sql
    assert "CREATE TABLE IF NOT EXISTS FILE_DQ_REJECT" in sql
    assert "CREATE TABLE IF NOT EXISTS FILE_SCHEMA_AUDIT" in sql
    assert "INSERT INTO FILE_SCHEMA_AUDIT" in sql
    assert "FILE_DQ_REJECT" in sql and "2021-13-13" in sql
    assert "O''Neil" in sql
