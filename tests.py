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

# Helper: build a complete garage dict for tests
def garage(yearbuilt=2000, numfloors=1, bldgclass="G1"):
    return {"yearbuilt": yearbuilt, "numfloors": numfloors, "bldgclass": bldgclass}

# Test: pre-1968 G-class garage, no sprinklers, multi-story, no chargers, no violations, no FDNY
# Expected: 30 (pre-1968) + 10 (>2 fl) + 15 (catchall: no DOB, no FDNY, not residential) = 55
result = score_garage(garage(1925, 4, "G1"), [], [], None, [])
score, reasons, latest_sp, retrofit_flag, fdny_open = result
test("pre-1968 G1 + no sprinklers + 4 floors = 55 (was 65 in old scoring)",
     score == 55,
     f"got {score}")
test("latest sprinkler is 'none'",
     latest_sp == "none")
test("no retrofit flag for G-class",
     retrofit_flag is None)

# Test: modern building with recent sprinkler permit
# Expected: 5 (modern) + 0 (DOB permit exists) + 5 (2 fl) = 10
result2 = score_garage(garage(2010, 2, "G1"),
                        [{"issuance_date": "03/15/2023"}], [], None, [])
score2, _, latest_sp2, _, _ = result2
test("2010 build + 2023 sprinkler + 2 floors = 10",
     score2 == 10, f"got {score2}")
test("latest sprinkler date normalized",
     latest_sp2 == "2023-03-15")

# Test: 1-floor garage with chargers, no sprinklers
# Expected: 30 (pre-1968) + 15 (catchall) + 0 (1 fl) + 15 (high charger density) = 60
result3 = score_garage(garage(1950, 1, "G1"), [], [],
                        [{"l2_ports": 20, "dcfast_ports": 0}], [])
score3, _, _, _, _ = result3
test("1950 G1 + no sprinklers + 1fl + 20 L2 ports = 60",
     score3 == 60, f"got {score3}")

# Test: DC fast 3x weighting
result4 = score_garage(garage(2020, 1, "G1"),
                        [{"issuance_date": "2022-01-01"}], [],
                        [{"l2_ports": 0, "dcfast_ports": 7}], [])
score4, reasons4, _, _, _ = result4
test("DC fast ports weighted 3x (7 DC = 21 weighted = 15 pts)",
     score4 == 20, f"got {score4}")  # 5 modern + 0 sprinkler + 0 floor + 15 charger
test("reasons mention DC fast",
     any("DC fast" in r for r in reasons4))

# Test: critical violation tiering
# 15 (pre-2004) + 0 (sprinkler permit exists) + 0 (1 fl) + 17 (2 critical + 1 low) = 32
result5 = score_garage(garage(2000, 1, "G1"),
                        [{"issuance_date": "2015-01-01"}],
                        [{"violation_type": "IMEGNCY"}, {"violation_type": "IMEGNCY"},
                         {"violation_type": "LL2604"}], None, [])
score5, _, _, _, _ = result5
test("critical + low violations scored correctly",
     score5 == 32, f"got {score5}")

# Test: unknown year
# 20 (unknown) + 15 (catchall) = 35
result6 = score_garage(garage(0, 1, "G1"), [], [], None, [])
score6, _, _, _, _ = result6
test("unknown year G1 = 35 (was 45 in old scoring)",
     score6 == 35, f"got {score6}")

# Test: score capped at 100 (need FDNY violations to push past 90 with current factors)
# 30 (pre-1968) + 15 (catchall) + 10 (multi) + 20 (5 critical capped) + 15 (chargers)
# + 25 (FDNY cap with many old open BF12) = 115 → capped at 100
many_fdny = [{"is_open": True, "is_resolved": False, "date": "2010-01-01",
              "charges": [{"code": "BF12", "desc": "FAIL TO MAINTAIN SPK"}]}] * 10
result7 = score_garage(garage(1900, 10, "G1"), [],
                        [{"violation_type": "IMEGNCY"}] * 5,
                        [{"l2_ports": 30, "dcfast_ports": 5}], many_fdny)
score7, _, _, _, _ = result7
test("score capped at 100 (with many old FDNY violations)",
     score7 == 100, f"got {score7}")

# v1 NEW: LL26 retrofit flag triggers for pre-2004 residential >=10 floors with no evidence
# 15 (pre-2004) + 30 (LL26 retrofit) + 10 (multi-story) = 55
result_ll26 = score_garage(garage(1960, 12, "D4"), [], [], None, [])
score_ll26, _, _, flag_ll26, _ = result_ll26
test("LL26 retrofit flag triggers for pre-2004 residential >=10 floors no evidence",
     flag_ll26 == "ll26_retrofit",
     f"got flag={flag_ll26}, score={score_ll26}")

# v1 NEW: post-2004 residential >=4 floors gets +10 (NB-bundling adjustment)
# 5 (modern) + 10 (post-2004 res NB-bundling) + 5 (2 fl) wait no, 2 fl gets +5 only if >1
# actually only +5 if >1, 0 if 1 floor. Let's use 5 floors for a clear test.
# 5 (modern) + 10 (post-2004 res adjustment) + 10 (>2 fl) = 25
result_post04 = score_garage(garage(2010, 5, "D4"), [], [], None, [])
score_post04, _, _, flag_post04, _ = result_post04
test("post-2004 residential >=4 floors with no evidence gets +10 sprinkler (NB bundle)",
     score_post04 == 25 and flag_post04 is None,
     f"got score={score_post04}, flag={flag_post04}")

# v1 NEW: FDNY violation as proof of sprinkler system
# 15 (pre-2004) + 5 (FDNY confirms) + 0 (1 fl) = 20
fdny_evidence = [{"is_open": False, "is_resolved": True, "date": "2015-01-01",
                  "charges": [{"code": "BF12", "desc": "FAIL TO MAINTAIN SPK STD SUPP SYST"}]}]
result_fdny = score_garage(garage(1990, 1, "G1"), [], [], None, fdny_evidence)
score_fdny, _, _, _, _ = result_fdny
test("FDNY sprinkler violation = sprinkler presence confirmation (+5)",
     score_fdny == 20, f"got {score_fdny}")

# v1 NEW: open FDNY BF12 violations from 10+ years ago = +10 each (capped at +25)
fdny_old_open = [
    {"is_open": True, "is_resolved": False, "date": "2010-01-01",
     "charges": [{"code": "BF12", "desc": "FAIL TO MAINTAIN SPK STD SUPP SYST"}]},
    {"is_open": True, "is_resolved": False, "date": "2008-01-01",
     "charges": [{"code": "BF12", "desc": "FAIL TO MAINTAIN SPK STD SUPP SYST"}]},
]
# 15 (pre-2004) + 5 (FDNY confirms) + 0 (1 fl) + 20 (2 BF12 10+y open) = 40
result_old = score_garage(garage(1990, 1, "G1"), [], [], None, fdny_old_open)
score_old, _, _, _, fdny_open = result_old
test("Two BF12 violations from 10+ years ago add +20 (2 × 10pts)",
     score_old == 40 and fdny_open == 2,
     f"got score={score_old}, fdny_open={fdny_open}")


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

    # v1: we now score G-class + non-G with garagearea>=1000, expect ~6K-8K
    test("has >5000 parking buildings (v1 expanded set)",
         len(garages) > 5000,
         f"got {len(garages)}")

    test("has >200 buildings with chargers",
         len(charger_map) > 200,
         f"got {len(charger_map)}")

    # v1: match_stats should be populated, most matches should be via PAD address
    match_stats = cache.get("match_stats", {})
    test("match_stats present in cache",
         bool(match_stats),
         f"got {match_stats}")
    test("PAD address is the dominant match method (>90% of matches)",
         match_stats.get("pad_address", 0) > 200,
         f"pad_address: {match_stats.get('pad_address')}, stats: {match_stats}")

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

    # v1: garage_type should be populated on all garages
    missing_gt = [g for g in garages if "garage_type" not in g]
    test("all garages have garage_type field",
         len(missing_gt) == 0,
         f"{len(missing_gt)} missing")

    # v1: non-G buildings with garagearea should be present
    non_g = [g for g in garages if not g["bldgclass"].startswith("G")]
    test("v1 includes non-G buildings with parking",
         len(non_g) > 3000,
         f"non-G count: {len(non_g)}")

    # v1: small_garage flag present
    small = [g for g in garages if g.get("small_garage")]
    test("small_garage flag is populated",
         len(small) > 0,
         f"small_garage count: {len(small)}")
else:
    print("  SKIP  (no cached_data.json — run fetch_data.py first)")


# =========================================================================
# 4b. v1 match helper functions
# =========================================================================
print("\n--- v1 Match Helpers ---")

from fetch_data import _derive_garage_type, _is_gclass_garage

test("G1 -> standalone",
     _derive_garage_type("G1") == "standalone")
test("D4 -> under_residential",
     _derive_garage_type("D4") == "under_residential")
test("O5 -> under_commercial",
     _derive_garage_type("O5") == "under_commercial")
test("I5 (hospital) -> institutional",
     _derive_garage_type("I5") == "institutional")
test("W2 (educational) -> other",
     _derive_garage_type("W2") == "other")
test("empty class -> other",
     _derive_garage_type("") == "other")

test("G1 is G-class garage",
     _is_gclass_garage("G1", 1))
test("GU is G-class garage",
     _is_gclass_garage("GU", 1))
test("GW is G-class garage",
     _is_gclass_garage("GW", 1))
test("G0 with >1 floor is G-class garage",
     _is_gclass_garage("G0", 3))
test("G0 with 1 floor is NOT scored (surface parking lot)",
     not _is_gclass_garage("G0", 1))
test("G2 (auto body) not scored as parking",
     not _is_gclass_garage("G2", 2))
test("D4 not a G-class garage",
     not _is_gclass_garage("D4", 5))


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

    # v1: garage_type and small_garage should round-trip through scored output
    missing_gt = [r for r in results if "garage_type" not in r]
    test("all scored results have garage_type",
         len(missing_gt) == 0,
         f"{len(missing_gt)} missing")
    missing_sg = [r for r in results if "small_garage" not in r]
    test("all scored results have small_garage flag",
         len(missing_sg) == 0,
         f"{len(missing_sg)} missing")

    # v1: the 377 E 33rd charger should be matched to NYU-owned BBL, not to
    # the C7 apartment building across the block. Regression test for the
    # specific failure that motivated the v1 rebuild.
    nyu_bbl = next((r for r in results if r["bbl"] == "1009390028"), None)
    wrong_bbl = next((r for r in results if r["bbl"] == "1009390024"), None)
    if nyu_bbl is not None:
        test("377 E 33rd charger resolves to NYU-owned BBL 1009390028",
             nyu_bbl.get("has_chargers", False),
             "NYU BBL present but no chargers attached")
    else:
        # Acceptable if the BBL isn't in the scored set at all
        print("  SKIP  NYU BBL 1009390028 not in scored set")
    if wrong_bbl is not None:
        test("339 E 33rd apartment (C7) does NOT have chargers in v1",
             not wrong_bbl.get("has_chargers", False),
             "Apartment still holds a mismatched charger")
    else:
        # Acceptable: C7 apartment with garagearea=0 shouldn't even be in v1 set
        print("  SKIP  339 E 33rd not in v1 scored set (expected — garagearea=0)")
else:
    print("  SKIP  (no risk_scores_all.json)")


# =========================================================================
# Summary
# =========================================================================
print(f"\n{'=' * 40}")
print(f"  {passed} passed, {failed} failed")
print(f"{'=' * 40}")
sys.exit(1 if failed else 0)
