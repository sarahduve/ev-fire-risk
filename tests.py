"""
Tests for EV fire risk scoring pipeline.

Run: python3 tests.py
"""

import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent
passed = 0
failed = 0


def test(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}{f' — {detail}' if detail else ''}")


# =========================================================================
# 1. BBL parsing
# =========================================================================
print("\n--- BBL Parsing ---")

sys.path.insert(0, str(DATA_DIR))
from fetch_data import _parse_bbl

test("standard 10-digit BBL",
     _parse_bbl("1000920034") == ("1", "00092", "0034"))

test("BBL with decimal (PLUTO Phase 2 format)",
     _parse_bbl("4000330018.00000000") == ("4", "00033", "0018"))

test("BBL with .0 suffix",
     _parse_bbl("1000920034.0") == ("1", "00092", "0034"))

test("empty BBL returns None",
     _parse_bbl("") is None)

test("None BBL returns None",
     _parse_bbl(None) is None)

test("short BBL gets zero-padded",
     _parse_bbl("12345") == ("0", "00001", "2345"))

test("numeric BBL (not string)",
     _parse_bbl(1000920034) == ("1", "00092", "0034"))

test("float BBL",
     _parse_bbl(4000330018.0) == ("4", "00033", "0018"))


# =========================================================================
# 2. Date normalization
# =========================================================================
print("\n--- Date Normalization ---")

from score_garages import normalize_date

test("ISO format passthrough",
     normalize_date("2024-03-15") == "2024-03-15")

test("MM/DD/YYYY to ISO",
     normalize_date("03/15/2024") == "2024-03-15")

test("datetime with T",
     normalize_date("2024-03-15T00:00:00.000") == "2024-03-15")

test("single-digit month/day",
     normalize_date("3/5/2024") == "2024-03-05")

test("empty string",
     normalize_date("") == "")

test("None",
     normalize_date(None) == "")

# The critical test: make sure normalized dates compare correctly
d1 = normalize_date("12/31/2024")
d2 = normalize_date("01/01/2010")
test("2024 date compares greater than 2010 date",
     d1 > d2,
     f"got '{d1}' > '{d2}' = {d1 > d2}")

d3 = normalize_date("09/28/2012")
test("2012 date compares greater than 2010-01-01",
     d3 > "2010-01-01",
     f"got '{d3}' > '2010-01-01' = {d3 > '2010-01-01'}")

# Edge case: what if normalize_date gets a raw DOB date that slips through?
raw_dob = "12/31/2024"
test("raw DOB date NOT greater than ISO 2010 (string comparison bug)",
     raw_dob < "2010-01-01",  # This SHOULD be True if raw — proving the bug
     "This test proves why we MUST normalize before comparing")


# =========================================================================
# 3. Scoring function
# =========================================================================
print("\n--- Scoring ---")

from score_garages import score_garage

# Test: pre-1968 building, no sprinklers, multi-story, no chargers, no violations
result = score_garage(
    {"yearbuilt": 1925, "numfloors": 4},
    [],  # no sprinkler permits
    [],  # no violations
    None,  # no chargers
)
score, reasons, latest_sp = result
test("pre-1968 + no sprinklers + 4 floors = 65",
     score == 65,
     f"got {score}")
test("latest sprinkler is 'none'",
     latest_sp == "none")

# Test: modern building with recent sprinkler work
result2 = score_garage(
    {"yearbuilt": 2010, "numfloors": 2},
    [{"issuance_date": "03/15/2023"}],
    [],
    None,
)
score2, reasons2, latest_sp2 = result2
test("2010 build + 2023 sprinkler + 2 floors = 10",
     score2 == 10,
     f"got {score2}")
test("latest sprinkler date normalized",
     latest_sp2 == "2023-03-15",
     f"got '{latest_sp2}'")

# Test: with EV chargers (high density)
result3 = score_garage(
    {"yearbuilt": 1950, "numfloors": 1},
    [],
    [],
    [{"l2_ports": 20, "dcfast_ports": 0}],
)
score3, _, _ = result3
test("1950 + no sprinklers + 20 L2 ports = 70",
     score3 == 70,
     f"got {score3}")

# Test: DC fast chargers weighted 3x
result4 = score_garage(
    {"yearbuilt": 2020, "numfloors": 1},
    [{"issuance_date": "2022-01-01"}],
    [],
    [{"l2_ports": 0, "dcfast_ports": 7}],  # 7 * 3 = 21 weighted
)
score4, reasons4, _ = result4
test("DC fast ports weighted 3x (7 DC = 21 weighted = 15 pts)",
     score4 == 20,  # 5 (modern) + 0 (sprinkler ok) + 0 (1 floor) + 15 (chargers)
     f"got {score4}")
test("reasons mention DC fast",
     any("DC fast" in r for r in reasons4),
     f"reasons: {reasons4}")

# Test: violation tiering
result5 = score_garage(
    {"yearbuilt": 2000, "numfloors": 1},
    [{"issuance_date": "2015-01-01"}],
    [
        {"violation_type": "IMEGNCY-IMMEDIATE EMERGENCY"},
        {"violation_type": "IMEGNCY-IMMEDIATE EMERGENCY"},
        {"violation_type": "LL2604-PHOTOLUMINESCENT"},
    ],
    None,
)
score5, reasons5, _ = result5
# 15 (pre-2004) + 0 (sprinkler ok) + 0 (1 floor) + 17 (min(2*8+1, 20)) = 32
# viol_pts = min(2*8 + 0*5 + 1*1, 20) = 17, but only critical pts (16) are added
# to score since critical > 0; the low violation's 1pt is in viol_pts but
# score adds viol_pts (17) when critical > 0... wait let me re-check the code
# Actually: viol_pts = min(16 + 0 + 1, 20) = 17, score += 17
test("critical + low violations scored correctly",
     score5 == 32,
     f"got {score5}, reasons: {reasons5}")

# Test: unknown year
result6 = score_garage(
    {"yearbuilt": 0, "numfloors": 1},
    [],
    [],
    None,
)
score6, _, _ = result6
test("unknown year = 20 pts",
     score6 == 45,  # 20 (unknown) + 25 (no sprinkler)
     f"got {score6}")

# Test: score capped at 100
result7 = score_garage(
    {"yearbuilt": 1900, "numfloors": 10},
    [],
    [{"violation_type": "IMEGNCY-IMMEDIATE EMERGENCY"}] * 5,
    [{"l2_ports": 30, "dcfast_ports": 5}],
)
score7, _, _ = result7
test("score capped at 100",
     score7 == 100,
     f"got {score7}")


# =========================================================================
# 4. Data integrity (if cached_data.json exists)
# =========================================================================
print("\n--- Data Integrity ---")

cache_path = DATA_DIR / "cached_data.json"
if cache_path.exists():
    with open(cache_path) as f:
        cache = json.load(f)

    garages = cache["garages"]
    charger_map = cache["charger_map"]

    test("has >1500 garages",
         len(garages) > 1500,
         f"got {len(garages)}")

    test("has >200 buildings with chargers",
         len(charger_map) > 200,
         f"got {len(charger_map)}")

    # Check all garages have required fields
    required = ["bbl", "address", "borough", "yearbuilt", "numfloors", "lat", "lon"]
    missing = []
    for g in garages:
        for field in required:
            if field not in g:
                missing.append(f"{g.get('address', '?')} missing {field}")
    test("all garages have required fields",
         len(missing) == 0,
         f"{len(missing)} missing: {missing[:3]}")

    # BBLs are 10 digits
    bad_bbls = [g["bbl"] for g in garages if len(g["bbl"]) != 10 or not g["bbl"].isdigit()]
    test("all BBLs are 10 digits",
         len(bad_bbls) == 0,
         f"{len(bad_bbls)} bad: {bad_bbls[:3]}")

    # No null coordinates
    null_coords = [g for g in garages if g["lat"] == 0 or g["lon"] == 0]
    test("no null coordinates",
         len(null_coords) == 0,
         f"{len(null_coords)} with null coords")
else:
    print("  SKIP  (no cached_data.json — run fetch_data.py first)")


# =========================================================================
# 5. AFDC deduplication (if afdc_data.json exists)
# =========================================================================
print("\n--- AFDC Deduplication ---")

afdc_path = DATA_DIR / "afdc_data.json"
if afdc_path.exists():
    with open(afdc_path) as f:
        afdc = json.load(f)

    stations = afdc["garage_stations_data"]
    ids = [s.get("id") for s in stations]

    test("station IDs are unique",
         len(ids) == len(set(ids)),
         f"{len(ids)} total, {len(set(ids))} unique")

    # Check for the 28x duplication bug
    from collections import Counter
    addr_counts = Counter(s.get("street_address", "") for s in stations)
    max_dupes = addr_counts.most_common(1)[0][1] if addr_counts else 0
    test("no address appears more than 5 times",
         max_dupes <= 5,
         f"max dupes: {max_dupes} ({addr_counts.most_common(1)})")
else:
    print("  SKIP  (no afdc_data.json)")


# =========================================================================
# 6. Output integrity (if risk_scores_all.json exists)
# =========================================================================
print("\n--- Output Integrity ---")

scores_path = DATA_DIR / "risk_scores_all.json"
if scores_path.exists():
    with open(scores_path) as f:
        scores = json.load(f)

    results = scores["results"]

    test("scores are 0-100",
         all(0 <= r["risk_score"] <= 100 for r in results),
         f"range: {min(r['risk_score'] for r in results)}-{max(r['risk_score'] for r in results)}")

    test("results are sorted descending",
         all(results[i]["risk_score"] >= results[i+1]["risk_score"]
             for i in range(len(results)-1)))

    test("reasons is a list for all results",
         all(isinstance(r["reasons"], list) for r in results))

    test("all results have lat/lon",
         all(r.get("lat") and r.get("lon") for r in results))

    # Sprinkler dates should be ISO or "none"
    bad_dates = [r["sprinkler_last_date"] for r in results
                 if r["sprinkler_last_date"] != "none"
                 and not r["sprinkler_last_date"][:4].isdigit()]
    test("sprinkler dates are ISO or 'none'",
         len(bad_dates) == 0,
         f"{len(bad_dates)} bad: {bad_dates[:3]}")
else:
    print("  SKIP  (no risk_scores_all.json)")


# =========================================================================
# Summary
# =========================================================================
print(f"\n{'=' * 40}")
print(f"  {passed} passed, {failed} failed")
print(f"{'=' * 40}")
sys.exit(1 if failed else 0)
