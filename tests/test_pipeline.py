import json
from datetime import date
from pathlib import Path
import pytest
from skypoints_etl.pipeline import _records, normalize_member, normalize_redemption_payload, parse_date, run_pipeline

AS_OF = "2024-01-15"
def row(**changes):
    x = {"Member Name":"Elena", "Member Id":"223457", "Enrollment Date":"20101012", "Last Flight Date":"20240101", "Tier Code":"GLD", "Agent Name":"Sam", "State":"CA", "Country":"USA", "DOB":"03051985", "Is Active":"A"}
    x.update(changes); return x

def test_normalizes_member_and_derives_age_and_freshness():
    m=normalize_member(row(), date.fromisoformat(AS_OF))
    assert m["age"] == 38 and not m["stale_member"] and m["country"] == "USA"

def test_strict_stale_boundary():
    assert not normalize_member(row(**{"Last Flight Date":"20231017"}), date(2024,1,15))["stale_member"]
    assert normalize_member(row(**{"Last Flight Date":"20231016"}), date(2024,1,15))["stale_member"]

def test_accepts_assessment_pipe_records():
    rows=[{"Member_Name":"Ravi","Member_Id":"223458","Enrollment_Date":"20101012","Last_Flight_Date":"20121013","Tier_Code":"SLV","Agent_Name":"Sam","State":"TN","Country":"IND","DOB":"03051985","Is_Active":"A"}]
    assert run_pipeline(rows, [], AS_OF).members[0]["country"] == "IND"

def test_pipe_parser_preserves_empty_optional_fields_and_rejects_wrong_width(tmp_path: Path):
    source = tmp_path / "members.txt"
    source.write_text(
        "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active\n"
        "|D|Ravi|223458|20101012|20121013|SLV||TN|IND|03051985|A\n"
        "|D|Broken|223459|20101012|20121013|GLD|Sam|CA|USA|03051985\n",
        encoding="utf-8",
    )
    rows = _records(source)
    assert rows[0]["Agent_Name"] == ""
    result = run_pipeline(rows, [], AS_OF)
    assert result.members[0]["agent_name"] is None
    assert len(result.rejects) == 1
    assert "expected 10 fields, received 9" in result.rejects[0]["reason"]

def test_accepts_unpadded_dates():
    assert parse_date("6152022").isoformat() == "2022-06-15"
    assert parse_date("3051985").isoformat() == "1985-03-05"

def test_invalid_dates_and_missing_required_are_rejected():
    for bad in ("2021-13-13", "not-a-date"):
        with pytest.raises(ValueError): normalize_member(row(DOB=bad), date(2024,1,15))
    with pytest.raises(ValueError): normalize_member(row(**{"Member Name":""}), date(2024,1,15))
    with pytest.raises(ValueError): normalize_member(row(**{"Tier Code":"BAD"}), date(2024,1,15))

def test_unknown_country_is_preserved_for_review():
    assert normalize_member(row(Country="XYZ"), date(2024,1,15))["country"] == "XYZ"

def test_redemptions_flatten_and_validate():
    payload={"member_id":"223457","feed_date":"20240115","redemptions":[{"txn_id":"RX1","txn_date":"20240110","partner":"AeroLink","miles_redeemed":12000,"status":"COMPLETED"}]}
    out=normalize_redemption_payload(payload,date(2024,1,15))
    assert out[0]["member_id"] == "223457" and out[0]["miles_redeemed"] == 12000
    payload["redemptions"][0]["miles_redeemed"]=-2
    with pytest.raises(ValueError): normalize_redemption_payload(payload,date(2024,1,15))

def test_deduplicates_members_and_transaction_ids():
    members=[row(Country="USA"),row(Country="IND")]
    payload=[{"member_id":"223457","feed_date":"20240115","redemptions":[{"txn_id":"RX1","txn_date":"20240110","partner":"A","miles_redeemed":1,"status":"PENDING"}]},
             {"member_id":"223457","feed_date":"20240115","redemptions":[{"txn_id":"RX1","txn_date":"20240110","partner":"A","miles_redeemed":1,"status":"PENDING"}]}]
    out=run_pipeline(members,payload,AS_OF)
    assert len(out.members)==1 and out.members[0]["country"]=="IND"
    assert len(out.redemptions)==1 and any("duplicate txn_id" in x["reason"] for x in out.rejects)

def test_invalid_redemption_is_quarantined_without_dropping_valid_sibling():
    payload={"member_id":"223457","feed_date":"20240115","redemptions":[
        {"txn_id":"RX1","txn_date":"20240110","partner":"A","miles_redeemed":100,"status":"COMPLETED"},
        {"txn_id":"RX2","txn_date":"20240112","partner":"A","miles_redeemed":0,"status":"PENDING"},
    ]}
    result=run_pipeline([],payload,AS_OF)
    assert [txn["txn_id"] for txn in result.redemptions] == ["RX1"]
    assert len(result.rejects) == 1
    assert result.rejects[0]["row"] == "1:2"

def test_bad_json_file_becomes_reject_instead_of_aborting(tmp_path: Path):
    from skypoints_etl.pipeline import process_files

    members = tmp_path / "members.csv"
    members.write_text(
        "Member Name,Member Id,Enrollment Date,Country\nElena,1,20240101,USA\n",
        encoding="utf-8",
    )
    redemptions = tmp_path / "redemptions.json"
    redemptions.write_text('{ broken json', encoding="utf-8")
    result = process_files(members, redemptions, AS_OF)
    assert len(result.members) == 1
    assert result.redemptions == []
    assert "unable to read/parse JSON feed" in result.rejects[0]["reason"]

def test_snowflake_transform_has_idempotent_merge_and_country_sync():
    sql = (Path(__file__).parents[1] / "sql" / "02_transform.sql").read_text(encoding="utf-8")
    assert "MERGE INTO SKYPOINTS.REDEMPTION_TRANSACTION" in sql
    assert "ARRAY_CONSTRUCT(l.RAW_PAYLOAD)" in sql
    assert "BEGIN TRANSACTION;" in sql and "COMMIT;" in sql
    assert "DELETE FROM SKYPOINTS.MEMBER_USA" in sql
    assert "DELETE FROM SKYPOINTS.MEMBER_INDIA" in sql
    assert "INSERT INTO SKYPOINTS.DQ_REJECT" in sql
