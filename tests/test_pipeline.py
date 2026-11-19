import json
from datetime import date
from pathlib import Path
import pytest
from skypoints_etl.pipeline import normalize_member, normalize_redemption_payload, run_pipeline

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

def test_invalid_dates_and_missing_required_are_rejected():
    for bad in ("2021-13-13", "not-a-date"):
        with pytest.raises(ValueError): normalize_member(row(DOB=bad), date(2024,1,15))
    with pytest.raises(ValueError): normalize_member(row(**{"Member Name":""}), date(2024,1,15))

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
