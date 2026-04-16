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


def score_garage(garage, sprinkler_permits, violations, charger_info, fdny_violations=None):
    """
    Score 0-100.

    Factors:
      - Age (0-30): pre-1968=30, 1968-2003=15, 2004+=5, unknown=20
      - Sprinkler System Evidence (0-30): tiered by evidence + retrofit mandate
      - Multi-story (0-10)
      - DOB Safety violations (0-20): tiered by severity
      - FDNY Fire Protection Compliance (0-25): time-weighted open violations
      - EV charger bonus (0-15)
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

    # DOB Safety violations — tiered
    critical = high = low = 0
    for v in violations:
        vtype = (v.get("violation_type") or "").upper()
        if "IMEGNCY" in vtype or "UNSAFE" in vtype or "COMPROMISED" in vtype:
            critical += 1
        elif "SPRINKLER" in vtype:
            high += 1
        else:
            low += 1

    viol_pts = min(critical * 8 + high * 5 + low * 1, 20)
    if critical > 0:
        score += viol_pts
        reasons.append(f"{critical} DOB critical violation(s) (emergency/unsafe/structural) (+{min(critical * 8, 20)})")
    if high > 0:
        if critical == 0:
            score += min(high * 5, 20)
            reasons.append(f"{high} DOB sprinkler violation(s) (+{min(high * 5, 20)})")
        else:
            reasons.append(f"{high} DOB sprinkler violation(s)")
    if low > 0:
        if critical == 0 and high == 0:
            score += min(low, 3)
            reasons.append(f"{low} DOB minor violation(s) (+{min(low, 3)})")

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

    print(f"  {len(garages)} garages, fetched {cache['fetched']}")
    print(f"  {len(charger_map)} with chargers")
    print(f"  {len(fdny_map)} with FDNY fire-protection violations")

    print("Scoring...")
    results = []
    for g in garages:
        bbl = g["bbl"]
        sp = sprinkler_map.get(bbl, [])
        viols = violation_map.get(bbl, [])
        chargers = charger_map.get(bbl, None)
        fdny = fdny_map.get(bbl, [])

        risk_score, reasons, latest_sp, retrofit_flag, fdny_open = score_garage(g, sp, viols, chargers, fdny)

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
            "fire_violations_count": len(viols),
            "fdny_violations_total": len(fdny),
            "fdny_violations_open": fdny_open,
            "retrofit_flag": retrofit_flag,
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
            "fire_violations_count", "bldgarea_sqft", "garagearea_sqft",
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
