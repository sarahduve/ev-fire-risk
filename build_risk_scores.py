"""
EV Fire Risk Scoring for NYC Parking Garages

Joins EV charger locations (AFDC) with parking garage data (PLUTO) and
DOB sprinkler permit history to identify garages that have added EV charging
but may lack adequate fire suppression upgrades.

Risk factors:
  - Age of structure (pre-1968 = highest risk)
  - EV charger density (more ports = more concentrated fire risk)
  - No sprinkler permit on record after charger installation
  - Below-grade / multi-story (harder ventilation, egress)
  - Existing fire-related DOB violations
"""

import json
import math
import time
import urllib.request
import urllib.parse
import csv
import io
from pathlib import Path

DATA_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# 1. Load local data
# ---------------------------------------------------------------------------

def load_afdc_garages():
    with open(DATA_DIR / "afdc_data.json") as f:
        data = json.load(f)
    stations = data["garage_stations_data"]
    out = []
    for s in stations:
        lat = s.get("latitude")
        lon = s.get("longitude")
        if not lat or not lon:
            continue
        out.append({
            "afdc_id": s.get("id"),
            "name": s.get("station_name", ""),
            "address": s.get("street_address", ""),
            "city": s.get("city", ""),
            "zip": s.get("zip", ""),
            "lat": float(lat),
            "lon": float(lon),
            "l2_ports": int(s.get("ev_level2_evse_num") or 0),
            "dcfast_ports": int(s.get("ev_dc_fast_num") or 0),
            "facility_type": s.get("facility_type", ""),
            "open_date": s.get("open_date", ""),
            "access": s.get("access_code", ""),
            "network": s.get("ev_network", ""),
        })
    return out


def load_pluto_garages():
    with open(DATA_DIR / "pluto_garages.json") as f:
        data = json.load(f)
    records = data["records"]
    out = []
    for r in records:
        lat = r.get("latitude")
        lon = r.get("longitude")
        if not lat or not lon:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (ValueError, TypeError):
            continue
        if lat == 0 or lon == 0:
            continue
        yearbuilt = int(r.get("yearbuilt") or 0)
        out.append({
            "bbl": r.get("bbl", ""),
            "address": r.get("address", ""),
            "borough": r.get("borough", ""),
            "bldgclass": r.get("bldgclass", ""),
            "yearbuilt": yearbuilt,
            "numfloors": float(r.get("numfloors") or 0),
            "bldgarea": float(r.get("bldgarea") or 0),
            "garagearea": float(r.get("garagearea") or 0),
            "lotarea": float(r.get("lotarea") or 0),
            "zipcode": r.get("zipcode", ""),
            "lat": lat,
            "lon": lon,
            "zonedist1": r.get("zonedist1", ""),
        })
    return out


# ---------------------------------------------------------------------------
# 2. Spatial matching — find nearest PLUTO garage for each AFDC station
# ---------------------------------------------------------------------------

def haversine_ft(lat1, lon1, lat2, lon2):
    """Approximate distance in feet between two lat/lon points."""
    R = 20902231  # Earth radius in feet
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def match_stations_to_garages(stations, garages, max_dist_ft=500):
    """For each AFDC station, find the nearest PLUTO garage within max_dist_ft."""
    matched = []
    unmatched = []
    for s in stations:
        best = None
        best_dist = float("inf")
        for g in garages:
            d = haversine_ft(s["lat"], s["lon"], g["lat"], g["lon"])
            if d < best_dist:
                best_dist = d
                best = g
        if best and best_dist <= max_dist_ft:
            matched.append({**s, "pluto": best, "match_dist_ft": round(best_dist, 1)})
        else:
            unmatched.append({**s, "nearest_dist_ft": round(best_dist, 1) if best else None})
    return matched, unmatched


# ---------------------------------------------------------------------------
# 3. Query DOB for sprinkler permits at matched garages
# ---------------------------------------------------------------------------

BORO_CODE_TO_NAME = {
    "1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN",
    "4": "QUEENS", "5": "STATEN ISLAND",
}


def _parse_bbl(bbl):
    """Parse PLUTO BBL into (boro_code, block_5, lot_4) or None."""
    if not bbl:
        return None
    s = str(bbl).split(".")[0].zfill(10)
    if len(s) < 10:
        return None
    return s[0], s[1:6], s[6:10]


def _socrata_get(url):
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception:
        return []


def query_dob_sprinkler_permits(bbl):
    """Check DOB Permit Issuance + DOB NOW for sprinkler work at a BBL."""
    parsed = _parse_bbl(bbl)
    if not parsed:
        return []
    boro_code, block, lot_4 = parsed
    boro_name = BORO_CODE_TO_NAME.get(boro_code, "")
    lot_5 = lot_4.zfill(5)

    results = []

    # 1) DOB Permit Issuance (older system) — borough=name, lot=4 or 5-digit
    lot_clause = f"(lot='{lot_5}' OR lot='{lot_4}')" if lot_4 != lot_5 else f"lot='{lot_5}'"
    params = urllib.parse.urlencode({
        "$where": f"borough='{boro_name}' AND block='{block}' AND {lot_clause} AND permit_subtype='SP'",
        "$limit": 50,
        "$order": "issuance_date DESC",
    })
    results.extend(_socrata_get(
        f"https://data.cityofnewyork.us/resource/ipu4-2q9a.json?{params}"
    ))

    # 2) DOB NOW Build (newer system) — has bbl field (10-digit, no decimals)
    bbl_10 = str(bbl).split(".")[0].zfill(10)
    params = urllib.parse.urlencode({
        "$where": f"bbl='{bbl_10}' AND (work_type='Sprinklers' OR upper(job_description) like '%25SPRINKLER%25')",
        "$limit": 50,
    })
    results.extend(_socrata_get(
        f"https://data.cityofnewyork.us/resource/rbx6-tga4.json?{params}"
    ))

    return results


def query_dob_violations(bbl):
    """Check for safety-related violations at a BBL.

    Searches for: sprinkler deficiencies, unsafe building designations,
    structurally compromised buildings, immediate emergencies, and
    Local Law 26/04 fire safety violations.
    """
    parsed = _parse_bbl(bbl)
    if not parsed:
        return []
    boro_code, block, lot_4 = parsed
    lot_5 = lot_4.zfill(5)

    # DOB Violations uses boro=code (number), lot padding varies (try both)
    # Violation types: LL2604S (sprinkler), LL2604E (emergency power),
    # LL2604 (photoluminescent), UB (unsafe buildings),
    # COMPBLD (structurally compromised), IMEGNCY (immediate emergency)
    lot_clause = f"(lot='{lot_5}' OR lot='{lot_4}')" if lot_4 != lot_5 else f"lot='{lot_5}'"
    params = urllib.parse.urlencode({
        "$where": (
            f"boro='{boro_code}' AND block='{block}' AND {lot_clause} "
            f"AND (violation_type like '%SPRINKLER%' "
            f"OR violation_type like '%UNSAFE%' "
            f"OR violation_type like '%COMPROMISED%' "
            f"OR violation_type like '%IMEGNCY%' "
            f"OR violation_type like '%LL2604%')"
        ),
        "$limit": 50,
    })
    return _socrata_get(
        f"https://data.cityofnewyork.us/resource/3h2n-5cm9.json?{params}"
    )


# ---------------------------------------------------------------------------
# 4. Risk scoring
# ---------------------------------------------------------------------------

def score_risk(entry, sprinkler_permits, violations):
    """
    Score 0-100 where higher = more risk.

    Factors:
      - Age (0-30 pts): pre-1968 = 30, 1968-2003 = 15, 2004+ = 5, unknown = 20
      - EV density (0-20 pts): based on total ports
      - No sprinkler upgrade (0-25 pts): no permit after charger install = 25
      - Multi-story (0-10 pts): >2 floors = 10
      - Fire violations (0-15 pts): each violation = 5, max 15
    """
    score = 0
    reasons = []
    pluto = entry["pluto"]

    # Age
    yb = pluto["yearbuilt"]
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

    # EV density — DC fast ports count 3x (higher energy, more heat, more stress)
    l2 = entry["l2_ports"]
    dc = entry["dcfast_ports"]
    weighted_ports = l2 + dc * 3
    if weighted_ports >= 20:
        score += 20
        reasons.append(f"{l2} L2 + {dc} DC fast ports, high density (+20)")
    elif weighted_ports >= 10:
        score += 15
        reasons.append(f"{l2} L2 + {dc} DC fast ports, moderate density (+15)")
    elif weighted_ports >= 4:
        score += 10
        reasons.append(f"{l2} L2 + {dc} DC fast ports (+10)")
    else:
        score += 5
        reasons.append(f"{l2} L2 + {dc} DC fast ports (+5)")

    # Sprinkler upgrade check
    charger_open = entry.get("open_date", "")  # ISO: YYYY-MM-DD
    has_recent_sprinkler = False
    latest_sprinkler_date = ""
    if sprinkler_permits:
        for p in sprinkler_permits:
            # Normalize date — DOB uses MM/DD/YYYY, DOB NOW uses ISO
            raw = p.get("issuance_date") or p.get("approved_date") or ""
            if "/" in raw:
                parts = raw.split("/")
                if len(parts) == 3:
                    pdate = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                else:
                    pdate = raw
            elif "T" in raw:
                pdate = raw[:10]
            else:
                pdate = raw
            if pdate > latest_sprinkler_date:
                latest_sprinkler_date = pdate
            if charger_open and pdate >= charger_open:
                has_recent_sprinkler = True
            elif not charger_open and pdate >= "2015-01-01":
                has_recent_sprinkler = True

    if not sprinkler_permits:
        score += 25
        reasons.append("no sprinkler permits on record (+25)")
    elif not has_recent_sprinkler:
        score += 15
        reasons.append("no sprinkler upgrade since charger install (+15)")
    else:
        reasons.append("sprinkler work after charger install (+0)")

    # Multi-story
    floors = pluto["numfloors"]
    if floors > 2:
        score += 10
        reasons.append(f"{int(floors)} floors, multi-story (+10)")
    elif floors > 1:
        score += 5
        reasons.append(f"{int(floors)} floors (+5)")

    # Safety violations — tiered by severity
    critical = 0  # IMEGNCY, UNSAFE, COMPROMISED
    high = 0      # SPRINKLER
    low = 0       # PHOTOLUMINESCENT, EMERGENCY POWER
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
        reasons.append(f"{critical} critical violation(s) (emergency/unsafe/structural) (+{min(critical*8, 20)})")
    if high > 0:
        score += min(high * 5, 20) if critical == 0 else 0  # already counted above
        if critical == 0:
            reasons.append(f"{high} sprinkler violation(s) (+{min(high*5, 20)})")
        else:
            reasons.append(f"{high} sprinkler violation(s)")
    if low > 0:
        if critical == 0 and high == 0:
            score += min(low, 3)
            reasons.append(f"{low} minor violation(s) (exit signs/backup power) (+{min(low, 3)})")
        else:
            reasons.append(f"{low} minor violation(s) (exit signs/backup power)")

    return min(score, 100), reasons, latest_sprinkler_date


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------

def query_pluto_nearest(lat, lon, radius=0.002):
    """Query PLUTO API for the nearest lot to a lat/lon point."""
    params = urllib.parse.urlencode({
        "$where": (
            f"latitude > {lat - radius} AND latitude < {lat + radius} "
            f"AND longitude > {lon - radius} AND longitude < {lon + radius}"
        ),
        "$limit": 20,
        "$select": ("bbl,address,bldgclass,yearbuilt,numfloors,bldgarea,"
                     "garagearea,lotarea,landuse,latitude,longitude,borough,zipcode,zonedist1"),
    })
    url = f"https://data.cityofnewyork.us/resource/64uk-42ks.json?{params}"
    results = _socrata_get(url)
    if not results:
        return None
    # Find closest
    best, best_dist = None, float("inf")
    for r in results:
        try:
            rlat, rlon = float(r["latitude"]), float(r["longitude"])
        except (KeyError, ValueError, TypeError):
            continue
        d = haversine_ft(lat, lon, rlat, rlon)
        if d < best_dist:
            best_dist = d
            best = r
    if not best:
        return None
    return {
        "bbl": best.get("bbl", ""),
        "address": best.get("address", ""),
        "borough": best.get("borough", ""),
        "bldgclass": best.get("bldgclass", ""),
        "yearbuilt": int(best.get("yearbuilt") or 0),
        "numfloors": float(best.get("numfloors") or 0),
        "bldgarea": float(best.get("bldgarea") or 0),
        "garagearea": float(best.get("garagearea") or 0),
        "lotarea": float(best.get("lotarea") or 0),
        "zipcode": best.get("zipcode", ""),
        "lat": float(best["latitude"]),
        "lon": float(best["longitude"]),
        "zonedist1": best.get("zonedist1", ""),
        "match_dist_ft": round(best_dist, 1),
    }


GARAGE_OPERATORS = {
    "icon", "rapidpark", "parkit", "quik park", "ipark", "iparknyc",
    "ggmc", "parkright", "mpg", "champion", "apple parking",
    "central parking", "standard parking", "discount parking",
    "impark", "sp+", "laz parking", "propark",
}


def classify_confidence(entry):
    """Classify how confident we are the charger is inside an enclosed structure.

    Returns (level, reason):
      high   — strong evidence charger is inside a parking structure
      medium — likely inside a structure but not certain
      low    — may be outdoor/surface/yard; not useful for fire risk
    """
    ftype = entry.get("facility_type", "")
    name_lower = entry.get("name", "").lower()
    pluto = entry.get("pluto", {})
    bldgclass = pluto.get("bldgclass", "")
    garagearea = pluto.get("garagearea", 0)
    numfloors = pluto.get("numfloors", 0)

    is_garage_operator = any(op in name_lower for op in GARAGE_OPERATORS)
    is_garage_bldgclass = bldgclass.startswith("G") and bldgclass not in ("G6", "G7")
    has_garage_area = garagearea > 0
    is_multistory = numfloors > 1

    if ftype == "FLEET_STATION":
        if is_garage_bldgclass or (has_garage_area and is_multistory):
            return "medium", "fleet station in garage-classified building"
        return "low", "fleet station — may be outdoor/surface lot"

    if ftype == "MUNICIPAL_GARAGE":
        return "high", "municipal parking garage"

    # PAY_GARAGE or PARKING_GARAGE
    if is_garage_operator and (is_garage_bldgclass or has_garage_area):
        return "high", "named garage operator + garage-classified building"
    if is_garage_operator:
        return "high", "named garage operator"
    if is_garage_bldgclass:
        return "high", "PLUTO garage building class"
    if has_garage_area and is_multistory:
        return "medium", "building has garage area + multiple floors"
    if has_garage_area:
        return "medium", "building has garage area"
    if "garage" in name_lower or "parking" in name_lower:
        return "medium", "name mentions garage/parking"

    return "low", "no strong evidence charger is inside a structure"


def score_and_append(entry, results):
    """Query DOB and score a single matched station, appending to results."""
    bbl = entry["pluto"]["bbl"]
    sprinkler_permits = query_dob_sprinkler_permits(bbl)
    violations = query_dob_violations(bbl)
    risk_score, reasons, latest_sprinkler = score_risk(entry, sprinkler_permits, violations)
    confidence, confidence_reason = classify_confidence(entry)

    results.append({
        "risk_score": risk_score,
        "reasons": reasons,
        "station_name": entry["name"],
        "station_address": entry["address"],
        "city": entry["city"],
        "zip": entry["zip"],
        "total_ev_ports": entry["l2_ports"] + entry["dcfast_ports"],
        "l2_ports": entry["l2_ports"],
        "dcfast_ports": entry["dcfast_ports"],
        "charger_open_date": entry["open_date"],
        "network": entry["network"],
        "facility_type": entry.get("facility_type", ""),
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "garage_bbl": bbl,
        "garage_address": entry["pluto"]["address"],
        "borough": entry["pluto"]["borough"],
        "yearbuilt": entry["pluto"]["yearbuilt"],
        "numfloors": entry["pluto"]["numfloors"],
        "bldgclass": entry["pluto"]["bldgclass"],
        "bldgarea_sqft": entry["pluto"]["bldgarea"],
        "match_dist_ft": entry["match_dist_ft"],
        "sprinkler_permits_count": len(sprinkler_permits),
        "sprinkler_last_date": latest_sprinkler or "none",
        "fire_violations_count": len(violations),
        "lat": entry["lat"],
        "lon": entry["lon"],
    })


def main():
    print("Loading AFDC garage stations...")
    stations = load_afdc_garages()
    print(f"  {len(stations)} stations with coordinates")

    print("Loading PLUTO garages (G-class)...")
    garages = load_pluto_garages()
    print(f"  {len(garages)} garages with coordinates")

    # Phase 1: match to G-class garage structures
    structures = [g for g in garages if g["bldgclass"] in ("G0", "G1", "GU", "GW")]
    print(f"  {len(structures)} are actual garage structures (G0/G1/GU/GW)")

    print("\nPhase 1: Matching to G-class garages (within 500 ft)...")
    matched, unmatched_stations = match_stations_to_garages(stations, structures)
    print(f"  {len(matched)} matched, {len(unmatched_stations)} unmatched")

    # Phase 2: for unmatched stations, query PLUTO API for nearest lot of ANY class
    print(f"\nPhase 2: Querying PLUTO API for {len(unmatched_stations)} unmatched stations...")
    phase2_matched = []
    still_unmatched = []
    for i, s in enumerate(unmatched_stations):
        pluto_lot = query_pluto_nearest(s["lat"], s["lon"])
        if pluto_lot and pluto_lot["match_dist_ft"] <= 500:
            dist = pluto_lot.pop("match_dist_ft")
            phase2_matched.append({**s, "pluto": pluto_lot, "match_dist_ft": dist})
        else:
            still_unmatched.append(s)
        if (i + 1) % 25 == 0:
            print(f"  Queried {i + 1}/{len(unmatched_stations)}...")
            time.sleep(0.3)

    print(f"  Phase 2 matched: {len(phase2_matched)}, still unmatched: {len(still_unmatched)}")

    all_matched = matched + phase2_matched
    print(f"\nTotal matched: {len(all_matched)}")

    print(f"\nQuerying DOB for sprinkler permits and violations at {len(all_matched)} garages...")
    print("  (This may take a few minutes due to API rate limits)")

    results = []
    for i, entry in enumerate(all_matched):
        score_and_append(entry, results)
        if (i + 1) % 25 == 0:
            print(f"  Processed {i + 1}/{len(all_matched)}...")
            time.sleep(0.3)

    # Sort by risk score descending
    results.sort(key=lambda x: x["risk_score"], reverse=True)

    # Save full results
    output = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_scored": len(results),
        "unmatched_stations": len(still_unmatched),
        "risk_distribution": {
            "high_70_plus": sum(1 for r in results if r["risk_score"] >= 70),
            "elevated_50_69": sum(1 for r in results if 50 <= r["risk_score"] < 70),
            "moderate_30_49": sum(1 for r in results if 30 <= r["risk_score"] < 50),
            "low_under_30": sum(1 for r in results if r["risk_score"] < 30),
        },
        "confidence_distribution": {
            "high": sum(1 for r in results if r["confidence"] == "high"),
            "medium": sum(1 for r in results if r["confidence"] == "medium"),
            "low": sum(1 for r in results if r["confidence"] == "low"),
        },
        "results": results,
    }

    with open(DATA_DIR / "risk_scores.json", "w") as f:
        json.dump(output, f, indent=2)

    # Also write a CSV for easy viewing
    with open(DATA_DIR / "risk_scores.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "risk_score", "confidence", "station_name", "station_address",
            "borough", "yearbuilt", "numfloors", "total_ev_ports",
            "charger_open_date", "sprinkler_permits_count", "sprinkler_last_date",
            "fire_violations_count", "facility_type", "confidence_reason",
            "bldgclass", "bldgarea_sqft", "lat", "lon", "reasons",
        ])
        writer.writeheader()
        for r in results:
            row = {k: r[k] for k in writer.fieldnames if k != "reasons"}
            row["reasons"] = "; ".join(r["reasons"])
            writer.writerow(row)

    # Print summary
    print("\n" + "=" * 70)
    print("EV FIRE RISK SCORES — NYC PARKING GARAGES")
    print("=" * 70)
    print(f"\nScored: {len(results)} garages with EV chargers")
    print(f"Unmatched stations (no nearby PLUTO lot): {len(still_unmatched)}")
    print(f"\nRisk distribution:")
    for k, v in output["risk_distribution"].items():
        print(f"  {k.replace('_', ' ').title()}: {v}")
    print(f"\nConfidence (charger is inside an enclosed structure):")
    for k, v in output["confidence_distribution"].items():
        print(f"  {k.title()}: {v}")

    conf_label = {"high": "H", "medium": "M", "low": "L"}
    print(f"\n{'—' * 70}")
    print("TOP 25 HIGHEST RISK (high/medium confidence only)")
    print(f"{'—' * 70}")
    print(f"{'Score':>5}  {'Conf':>4}  {'Year':>5}  {'Flrs':>4}  {'Ports':>5}  {'Sprk':>4}  {'Viol':>4}  Name / Address")
    print(f"{'—' * 70}")
    shown = 0
    for r in results:
        if r["confidence"] == "low":
            continue
        yb = r["yearbuilt"] if r["yearbuilt"] > 0 else "????"
        print(f"{r['risk_score']:>5}  {conf_label[r['confidence']]:>4}  {yb:>5}  {r['numfloors']:>4.0f}  "
              f"{r['total_ev_ports']:>5}  {r['sprinkler_permits_count']:>4}  "
              f"{r['fire_violations_count']:>4}  {r['station_name'][:42]}")
        print(f"{'':>38}{r['station_address']}, {r['borough']}")
        shown += 1
        if shown >= 25:
            break

    print(f"\nFull results: {DATA_DIR / 'risk_scores.csv'}")
    print(f"JSON data:    {DATA_DIR / 'risk_scores.json'}")


if __name__ == "__main__":
    main()
