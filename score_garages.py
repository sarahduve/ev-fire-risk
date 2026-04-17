"""
Score NYC parking garages for EV fire risk.

Reads cached_data.json (produced by fetch_data.py) and applies the
scoring formula. This is instant — no API calls. Re-run freely when
tweaking scoring weights, labels, or adding new factors.

Usage:
    python3 fetch_data.py    # slow, ~30 min (only when data is stale)
    python3 score_garages.py # instant
    python3 build_map.py     # instant
"""

import json
import math
import time
import csv
from pathlib import Path

DATA_DIR = Path(__file__).parent


def haversine_ft(lat1, lon1, lat2, lon2):
    R = 20902231
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def normalize_date(raw):
    if not raw:
        return ""
    if "/" in raw:
        parts = raw.split("/")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    if "T" in raw:
        return raw[:10]
    return raw


def _has_fdny_sprinkler_evidence(fdny_recs):
    """Any FDNY violation specifically about sprinkler/standpipe/suppression
    proves the building has a sprinkler system (FDNY can't cite for failure
    to maintain a system that doesn't exist)."""
    if not fdny_recs:
        return False
    for v in fdny_recs:
        for ch in v.get("charges", []):
            code = (ch.get("code") or "").upper()
            desc = (ch.get("desc") or "").upper()
            if code == "BF12":
                return True
            if "SPK" in desc or "SPRINKLER" in desc or "STANDPIPE" in desc:
                return True
    return False


def _years_open(date_str, today_year=2026, today_month=4, today_day=15):
    """Approximate years between violation date and today."""
    if not date_str:
        return 0
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        days = (today_year - y) * 365.25 + (today_month - m) * 30 + (today_day - d)
        return max(days / 365.25, 0)
    except (ValueError, IndexError):
        return 0


def _classify_charge(charges):
    """Return charge category: 'bf12' (sprinkler direct), 'bf20' (inspect/test),
    'other_fp' (other fire protection), or None (not fire-protection-relevant)."""
    for ch in charges:
        code = (ch.get("code") or "").upper()
        desc = (ch.get("desc") or "").upper()
        # Skip false-alarm management — not suppression-relevant
        if "UNNECESSARY UNWARRANTED ALARM" in desc or "UNWANTED ALARM" in desc:
            continue
        if code == "BF12" or "SPK" in desc or "SPRINKLER" in desc or "STANDPIPE" in desc or "FAIL TO MAINTAIN SPK" in desc:
            return "bf12"
        if code in ("BF20", "VC20") or "INSPECTION AND TESTING" in desc or "FAIL TO CONDUCT REQUIRED TEST" in desc:
            return "bf20"
        if "FIRE PROTECTION SYSTEM" in desc or "PORTABLE FIRE EXTINGUISHER" in desc:
            return "other_fp"
    return None


# Time-weighted points matrix for OPEN FDNY fire-protection violations.
# Older open violations indicate persistent non-compliance, weighted higher.
# Sprinkler-direct (BF12) charges weighted higher than general inspection/test.
FDNY_POINTS = {
    "bf12":     {"0-2y": 3, "2-5y": 5, "5-10y": 7, "10+y": 10},
    "bf20":     {"0-2y": 2, "2-5y": 3, "5-10y": 5, "10+y": 7},
    "other_fp": {"0-2y": 1, "2-5y": 2, "5-10y": 3, "10+y": 4},
}
FDNY_FACTOR_CAP = 25  # Maximum points from FDNY compliance factor


def _age_bucket(years):
    if years < 2: return "0-2y"
    if years < 5: return "2-5y"
    if years < 10: return "5-10y"
    return "10+y"


# v1.3 DOB weights — per FDNY-expert guidance. Age multiplier applied only to
# ECB Class 1/2 records (not DOB NOW Parking Structures — LL126 program is ~4
# months old, so age signal is uninformative there).
ECB_WEIGHTS = {
    "construction_fire":  6,
    "construction_other": 3,
    "elevator_fireman":   4,
    "elevator_other":     1,
    "class2_fire":        2,
}


def _age_multiplier(years):
    """Older uncured Class 1/2 indicates structural non-compliance."""
    if years < 3:  return 1.0
    if years < 5:  return 1.25
    if years < 10: return 1.5
    return 2.0


def _score_dob_parking_structure(dob_now_recs):
    """DOB NOW Parking Structures (LL126) — failure to file OR failure to correct
    an 'unsafe' inspection report. Flat, no age decay.
    Take the HIGHER of PS-INITL (+8, never filed) or PS-UNSAFE (+12, filed+unsafe),
    since they're mutually informative signals about the same building.
    """
    if not dob_now_recs:
        return 0, []
    has_unsafe = any(r.get("class") == "ps_unsafe" for r in dob_now_recs)
    has_initl = any(r.get("class") == "ps_initl" for r in dob_now_recs)
    if has_unsafe:
        return 12, ["DOB LL126: Unsafe parking-structure inspection report, not corrected (+12)"]
    if has_initl:
        return 8, ["DOB LL126: Required parking-structure inspection report never filed (+8)"]
    return 0, []


def _score_dob_now_fire_systems(dob_now_recs):
    """Other DOB NOW device_types: sprinklers, emergency power, photoluminescent,
    structurally-compromised buildings. Small volumes, flat weights."""
    if not dob_now_recs:
        return 0, []
    points = 0
    parts = []
    if any(r.get("class") == "sprinkler" for r in dob_now_recs):
        points += 6
        parts.append("sprinkler system filing")
    if any(r.get("class") == "emergency_power" for r in dob_now_recs):
        points += 6
        parts.append("emergency power filing")
    if any(r.get("class") == "photoluminescent" for r in dob_now_recs):
        points += 1
        parts.append("exit-sign filing")
    if any(r.get("class") == "structurally_compromised" for r in dob_now_recs):
        points += 8
        parts.append("structurally compromised")
    if points == 0:
        return 0, []
    return points, [f"DOB NOW open {', '.join(parts)} violation(s) (+{points})"]


def _score_dob_ecb(ecb_recs):
    """ECB Class 1 + fire-relevant Class 2 violations, age-multiplied.
    Capped at 15 to prevent any single building from being dominated by ECB alone."""
    if not ecb_recs:
        return 0, []
    points = 0.0
    counts = {k: 0 for k in ECB_WEIGHTS}
    for r in ecb_recs:
        cls = r.get("class")
        if cls not in ECB_WEIGHTS:
            continue
        years = _years_open(r.get("date", ""))
        points += ECB_WEIGHTS[cls] * _age_multiplier(years)
        counts[cls] += 1
    points = min(int(round(points)), 15)
    if points == 0:
        return 0, []
    # Concise reason text
    labels = {
        "construction_fire":  "fire-egress/alarm Class 1",
        "construction_other": "other Class 1 construction",
        "elevator_fireman":   "elevator fireman-service",
        "elevator_other":     "other Class 1 elevator",
        "class2_fire":        "fire-relevant Class 2",
    }
    parts = [f"{counts[k]} {labels[k]}" for k in counts if counts[k] > 0]
    return points, [f"DOB ECB: {', '.join(parts)} (+{points}, age-weighted)"]


def _score_ll2604_active(ll2604_recs):
    """Active Local Law 26/2004 violations — small volume, high-specificity.
    LL26 of 2004 required exit signs, emergency power, and sprinkler retrofit
    for multi-story residential by 2019."""
    if not ll2604_recs:
        return 0, []
    points = 0
    parts = []
    by_class = {r.get("class") for r in ll2604_recs}
    if "ll2604_sprinkler" in by_class:
        points += 2
        parts.append("sprinkler")
    if "ll2604_emergency_power" in by_class:
        points += 1
        parts.append("emergency power")
    if "ll2604_photoluminescent" in by_class:
        points += 1
        parts.append("photoluminescent")
    if points == 0:
        return 0, []
    return points, [f"LL26/2004 retrofit mandate: active {', '.join(parts)} violation(s) (+{points})"]


def _score_fdny_compliance(fdny_recs):
    """Sum points for OPEN fire-protection violations, time-weighted."""
    if not fdny_recs:
        return 0, [], 0  # score, reason_strings, open_count
    points = 0
    bucket_counts = {}  # for reason text
    open_count = 0
    for v in fdny_recs:
        if not v.get("is_open"):
            continue
        cat = _classify_charge(v.get("charges", []))
        if not cat:
            continue
        years = _years_open(v.get("date", ""))
        bucket = _age_bucket(years)
        points += FDNY_POINTS[cat][bucket]
        key = (cat, bucket)
        bucket_counts[key] = bucket_counts.get(key, 0) + 1
        open_count += 1
    points = min(points, FDNY_FACTOR_CAP)
    # Build a concise reason string
    reasons = []
    if open_count:
        # Group by category for cleaner text
        by_cat = {"bf12": 0, "bf20": 0, "other_fp": 0}
        for (cat, _), ct in bucket_counts.items():
            by_cat[cat] += ct
        parts = []
        if by_cat["bf12"]:
            parts.append(f"{by_cat['bf12']} open sprinkler maintenance")
        if by_cat["bf20"]:
            parts.append(f"{by_cat['bf20']} open inspection/testing")
        if by_cat["other_fp"]:
            parts.append(f"{by_cat['other_fp']} other open fire protection")
        reasons.append(f"FDNY: {', '.join(parts)} violation(s) (+{points})")
    return points, reasons, open_count


# Retrofit mandate flag detection.
# LL26 of 2004: residential >=10 floors (proxy for >=100ft) had to retrofit by July 2019.
# LL16 of 1984: new offices >=7 floors (proxy for >=75ft) require sprinklers at construction.
# We flag only when there is NO evidence of sprinklers (no DOB permit AND no FDNY confirmation).
def _retrofit_flag(garage, has_dob, has_fdny):
    if has_dob or has_fdny:
        return None
    yb = garage["yearbuilt"]
    fl = garage["numfloors"]
    cls = (garage["bldgclass"] or "")[:1]
    if cls in ("C", "D", "R") and 0 < yb < 2004 and fl >= 10:
        return "ll26_retrofit"
    if cls == "O" and yb >= 1984 and fl >= 7:
        return "ll16_new"
    return None


def _score_sprinkler_evidence(garage, has_dob, has_fdny, retrofit_flag):
    """Sprinkler System Evidence factor (0-30 + flag bonus).

    Measures evidence the building has a sprinkler system — independent of
    whether it's being maintained (which is the FDNY compliance factor).
    """
    yb = garage["yearbuilt"]
    fl = garage["numfloors"]
    cls = (garage["bldgclass"] or "")[:1]
    is_residential = cls in ("C", "D", "R")

    if has_dob:
        return 0, "DOB sprinkler permit on record (+0)"
    if has_fdny:
        return 5, "FDNY violation confirms sprinkler system (no modern DOB record) (+5)"
    if retrofit_flag == "ll26_retrofit":
        return 30, "Pre-2004 residential ≥10 floors, no DOB permit, no FDNY evidence — Local Law 26 retrofit deadline (July 2019) likely missed (+30)"
    if retrofit_flag == "ll16_new":
        return 30, "Post-1984 office ≥7 floors, no DOB permit, no FDNY evidence — Local Law 16 sprinkler-at-construction requirement likely missed (+30)"
    if is_residential and yb >= 2004 and fl >= 4:
        return 10, "Post-2004 residential ≥4 floors — sprinklers required at construction by Local Law 26 but install may have been bundled in NB permit (+10)"
    return 15, "No DOB sprinkler permit on record and no FDNY violation confirming a system; sprinkler status unknown (+15)"


def score_garage(garage, sprinkler_permits, violations, charger_info,
                 fdny_violations=None, dob_ecb=None, dob_now=None, ll2604=None):
    """
    Score 0-100.

    Factors:
      - Age (0-30): pre-1968=30, 1968-2003=15, 2004+=5, unknown=20
      - Sprinkler System Evidence (0-30): tiered by evidence + retrofit mandate
      - Multi-story (0-10)
      - DOB LL126 Parking Structure (0-12): PS-UNSAFE=+12 or PS-INITL=+8 (take max)
      - DOB ECB Class 1/2 fire-relevant (0-15): weighted by category, age-multiplied
      - DOB NOW fire-system filings (0-20): sprinkler/EP/photoluminescent/SCB
      - Legacy LL2604 active (0-4): small-volume LL26/2004 mandate signal
      - FDNY Fire Protection Compliance (0-25): time-weighted open violations
      - EV charger bonus (0-15)

    Note: `violations` (3h2n-5cm9 keyword filter) is kept in the signature for
    backwards compat but is no longer scored in v1.3+. LL2604 extracted from
    there is now passed separately as `ll2604`.
    """
    score = 0
    reasons = []

    # Age
    yb = garage["yearbuilt"]
    if yb == 0:
        score += 20
        reasons.append("unknown construction date (+20)")
    elif yb < 1968:
        score += 30
        reasons.append(f"built {yb}, pre-1968 code (+30)")
    elif yb < 2004:
        score += 15
        reasons.append(f"built {yb}, pre-IBC 2003 (+15)")
    else:
        score += 5
        reasons.append(f"built {yb}, modern code (+5)")

    # Latest DOB sprinkler permit date (for display)
    latest_sprinkler = ""
    if sprinkler_permits:
        for p in sprinkler_permits:
            raw = p.get("issuance_date") or p.get("approved_date") or ""
            pdate = normalize_date(raw)
            if pdate > latest_sprinkler:
                latest_sprinkler = pdate

    has_dob = bool(sprinkler_permits)
    has_fdny = _has_fdny_sprinkler_evidence(fdny_violations)
    retrofit_flag = _retrofit_flag(garage, has_dob, has_fdny)

    # Sprinkler System Evidence
    sp_pts, sp_reason = _score_sprinkler_evidence(garage, has_dob, has_fdny, retrofit_flag)
    score += sp_pts
    reasons.append(sp_reason)

    # Multi-story
    floors = garage["numfloors"]
    if floors > 2:
        score += 10
        reasons.append(f"{int(floors)} floors, multi-story (+10)")
    elif floors > 1:
        score += 5
        reasons.append(f"{int(floors)} floors (+5)")

    # v1.3 DOB signals (replaces legacy violation_map / 3h2n-5cm9 keyword filter)
    # (1) LL126 parking-structure inspection program — DOB NOW
    ps_pts, ps_reasons = _score_dob_parking_structure(dob_now or [])
    if ps_pts > 0:
        score += ps_pts
        reasons.extend(ps_reasons)

    # (2) Other DOB NOW fire-system device filings (sprinkler, EP, photoluminescent, SCB)
    now_pts, now_reasons = _score_dob_now_fire_systems(dob_now or [])
    if now_pts > 0:
        score += now_pts
        reasons.extend(now_reasons)

    # (3) DOB ECB Class 1 + fire-relevant Class 2 (age-multiplied, capped at 15)
    ecb_pts, ecb_reasons = _score_dob_ecb(dob_ecb or [])
    if ecb_pts > 0:
        score += ecb_pts
        reasons.extend(ecb_reasons)

    # (4) Legacy LL2604 active records (small but high-specificity)
    ll_pts, ll_reasons = _score_ll2604_active(ll2604 or [])
    if ll_pts > 0:
        score += ll_pts
        reasons.extend(ll_reasons)

    # FDNY Fire Protection Compliance — time-weighted open violations
    fdny_pts, fdny_reasons, fdny_open_count = _score_fdny_compliance(fdny_violations)
    if fdny_pts > 0:
        score += fdny_pts
        reasons.extend(fdny_reasons)

    # EV charger bonus
    if charger_info:
        total_l2 = sum(c["l2_ports"] for c in charger_info)
        total_dc = sum(c["dcfast_ports"] for c in charger_info)
        weighted = total_l2 + total_dc * 3
        if weighted >= 20:
            score += 15
            reasons.append(f"EV chargers: {total_l2} L2 + {total_dc} DC fast (+15)")
        elif weighted >= 4:
            score += 10
            reasons.append(f"EV chargers: {total_l2} L2 + {total_dc} DC fast (+10)")
        elif weighted >= 1:
            score += 5
            reasons.append(f"EV chargers: {total_l2} L2 + {total_dc} DC fast (+5)")

    return min(score, 100), reasons, latest_sprinkler or "none", retrofit_flag, fdny_open_count


def main():
    cache_path = DATA_DIR / "cached_data.json"
    if not cache_path.exists():
        print("No cached_data.json found. Run fetch_data.py first.")
        return

    print("Loading cached data...")
    with open(cache_path) as f:
        cache = json.load(f)

    garages = cache["garages"]
    charger_map = cache["charger_map"]
    sprinkler_map = cache["sprinkler_map"]
    violation_map = cache["violation_map"]
    fdny_map = cache.get("fdny_violation_map", {})
    dob_ecb_map = cache.get("dob_ecb_map", {})
    dob_now_map = cache.get("dob_now_map", {})
    ll2604_map = cache.get("ll2604_map", {})

    print(f"  {len(garages)} garages, fetched {cache['fetched']}")
    print(f"  {len(charger_map)} with chargers")
    print(f"  {len(fdny_map)} with FDNY fire-protection violations")
    print(f"  {len(dob_ecb_map)} with DOB ECB Class 1/2 fire-relevant")
    print(f"  {len(dob_now_map)} with DOB NOW parking-structure / fire-system filings")
    print(f"  {len(ll2604_map)} with active LL2604 records")

    print("Scoring...")
    results = []
    for g in garages:
        bbl = g["bbl"]
        sp = sprinkler_map.get(bbl, [])
        viols = violation_map.get(bbl, [])
        chargers = charger_map.get(bbl, None)
        fdny = fdny_map.get(bbl, [])
        dob_ecb = dob_ecb_map.get(bbl, [])
        dob_now = dob_now_map.get(bbl, [])
        ll2604 = ll2604_map.get(bbl, [])

        risk_score, reasons, latest_sp, retrofit_flag, fdny_open = score_garage(
            g, sp, viols, chargers, fdny, dob_ecb=dob_ecb, dob_now=dob_now, ll2604=ll2604
        )

        has_chargers = chargers is not None
        total_ports = sum(c["l2_ports"] + c["dcfast_ports"] for c in chargers) if chargers else 0
        charger_names = "; ".join(c["name"] for c in chargers) if chargers else ""
        charger_addresses = "; ".join(
            sorted(set(c.get("address", "") for c in chargers if c.get("address")))
        ) if chargers else ""

        results.append({
            "risk_score": risk_score,
            "reasons": reasons,
            "address": g["address"],
            "borough": g["borough"],
            "bbl": bbl,
            "bldgclass": g["bldgclass"],
            "yearbuilt": g["yearbuilt"],
            "numfloors": g["numfloors"],
            "bldgarea_sqft": g["bldgarea"],
            "garagearea_sqft": g.get("garagearea", 0),
            "garage_type": g.get("garage_type", "other"),
            "small_garage": g.get("small_garage", False),
            "has_chargers": has_chargers,
            "total_ev_ports": total_ports,
            "charger_names": charger_names,
            "charger_addresses": charger_addresses,
            "sprinkler_permits_count": len(sp),
            "sprinkler_last_date": latest_sp,
            # Legacy keyword-filter count (SPRINKLER|UNSAFE|COMPROMISED|IMEGNCY|LL2604
            # from 3h2n-5cm9). Not scored in v1.3+ — kept for UI continuity until the
            # table/popup references are replaced with the v1.3 signal fields.
            "legacy_dob_keyword_count": len(viols),
            "fdny_violations_total": len(fdny),
            "fdny_violations_open": fdny_open,
            "retrofit_flag": retrofit_flag,
            "dob_ecb_count": len(dob_ecb),
            "dob_now_ps_status": (
                "unsafe" if any(r.get("class") == "ps_unsafe" for r in dob_now)
                else "initl" if any(r.get("class") == "ps_initl" for r in dob_now)
                else ""
            ),
            "dob_now_fire_system_count": sum(
                1 for r in dob_now
                if r.get("class") in ("sprinkler", "emergency_power",
                                       "photoluminescent", "structurally_compromised")
            ),
            "ll2604_active_count": len(ll2604),
            "lat": g["lat"],
            "lon": g["lon"],
        })

    results.sort(key=lambda x: x["risk_score"], reverse=True)

    # Cross-reference with OSM parking types
    osm_path = DATA_DIR / "osm_parking.json"
    if osm_path.exists():
        print("Cross-referencing with OSM parking types...")
        with open(osm_path) as f:
            osm = json.load(f)
        enclosed = [p for p in osm["elements"] if p["parking_type"] in ("underground", "multi-storey")]
        matched_osm = 0
        for r in results:
            best_osm, best_dist = None, float("inf")
            for p in enclosed:
                d = haversine_ft(r["lat"], r["lon"], p["lat"], p["lon"])
                if d < best_dist:
                    best_dist = d
                    best_osm = p
            if best_osm and best_dist <= 300:
                r["osm_parking_type"] = best_osm["parking_type"]
                r["osm_name"] = best_osm.get("name", "")
                matched_osm += 1
            else:
                r["osm_parking_type"] = ""
                r["osm_name"] = ""
        print(f"  {matched_osm} garages matched to OSM enclosed structures")
    else:
        for r in results:
            r["osm_parking_type"] = ""
            r["osm_name"] = ""

    # Add PLUTO basement codes from local bulk file (v1)
    # Previously fetched via a separate Socrata call filtered by G-class, which
    # wouldn't return codes for our v1 non-G buildings. Reading locally is also
    # faster and avoids an extra API dependency.
    print("Adding basement codes from pluto_all.json...")
    bsmt_map = {}
    pluto_path = DATA_DIR / "pluto_all.json"
    if pluto_path.exists():
        with open(pluto_path) as f:
            pluto = json.load(f)
        for r in pluto["records"]:
            bbl = str(r.get("bbl", "")).split(".")[0].zfill(10)
            bsmt_map[bbl] = r.get("bsmtcode", "0") or "0"
        for r in results:
            bsmt = bsmt_map.get(r["bbl"], "0")
            if bsmt in ("1", "2") and not r.get("osm_parking_type"):
                r["osm_parking_type"] = "underground"
            r["has_basement"] = bsmt in ("1", "2")
        underground = sum(1 for r in results if r.get("osm_parking_type") == "underground")
        print(f"  {underground} underground garages")
    else:
        print("  pluto_all.json not present; skipping basement codes")
        for r in results:
            r["has_basement"] = False

    output = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "data_fetched": cache["fetched"],
        "total_scored": len(results),
        "garages_with_chargers": sum(1 for r in results if r["has_chargers"]),
        "risk_distribution": {
            "high_70_plus": sum(1 for r in results if r["risk_score"] >= 70),
            "elevated_50_69": sum(1 for r in results if 50 <= r["risk_score"] < 70),
            "moderate_30_49": sum(1 for r in results if 30 <= r["risk_score"] < 50),
            "low_under_30": sum(1 for r in results if r["risk_score"] < 30),
        },
        "results": results,
    }

    with open(DATA_DIR / "risk_scores_all.json", "w") as f:
        json.dump(output, f, indent=2)

    with open(DATA_DIR / "risk_scores_all.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "risk_score", "address", "borough", "bldgclass", "garage_type",
            "yearbuilt", "numfloors", "has_chargers", "total_ev_ports",
            "sprinkler_permits_count", "sprinkler_last_date",
            "legacy_dob_keyword_count", "bldgarea_sqft", "garagearea_sqft",
            "small_garage", "lat", "lon", "reasons",
        ])
        writer.writeheader()
        for r in results:
            row = {k: r[k] for k in writer.fieldnames if k != "reasons"}
            row["reasons"] = "; ".join(r["reasons"])
            writer.writerow(row)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Scored {len(results)} garages ({output['garages_with_chargers']} with chargers)")
    print(f"High: {output['risk_distribution']['high_70_plus']} | "
          f"Elevated: {output['risk_distribution']['elevated_50_69']} | "
          f"Moderate: {output['risk_distribution']['moderate_30_49']} | "
          f"Low: {output['risk_distribution']['low_under_30']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
