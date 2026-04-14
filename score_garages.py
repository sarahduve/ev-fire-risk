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


def score_garage(garage, sprinkler_permits, violations, charger_info):
    """
    Score 0-100.

    Factors:
      - Age (0-30): pre-1968=30, 1968-2003=15, 2004+=5, unknown=20
      - Fire suppression maintenance (0-25)
      - Multi-story (0-10)
      - Safety violations (0-20): tiered by severity
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

    # Sprinkler check
    latest_sprinkler = ""
    if sprinkler_permits:
        for p in sprinkler_permits:
            raw = p.get("issuance_date") or p.get("approved_date") or ""
            pdate = normalize_date(raw)
            if pdate > latest_sprinkler:
                latest_sprinkler = pdate

    if not sprinkler_permits:
        score += 25
        reasons.append("no fire suppression maintenance on record (+25)")
    elif latest_sprinkler < "2010-01-01":
        score += 15
        reasons.append(f"last sprinkler work {latest_sprinkler}, predates 2022 NFPA density increase (+15)")
    else:
        reasons.append(f"sprinkler work {latest_sprinkler}")

    # Multi-story
    floors = garage["numfloors"]
    if floors > 2:
        score += 10
        reasons.append(f"{int(floors)} floors, multi-story (+10)")
    elif floors > 1:
        score += 5
        reasons.append(f"{int(floors)} floors (+5)")

    # Safety violations — tiered
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
        reasons.append(f"{critical} critical violation(s) (emergency/unsafe/structural) (+{min(critical * 8, 20)})")
    if high > 0:
        if critical == 0:
            score += min(high * 5, 20)
            reasons.append(f"{high} sprinkler violation(s) (+{min(high * 5, 20)})")
        else:
            reasons.append(f"{high} sprinkler violation(s)")
    if low > 0:
        if critical == 0 and high == 0:
            score += min(low, 3)
            reasons.append(f"{low} minor violation(s) (+{min(low, 3)})")

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

    return min(score, 100), reasons, latest_sprinkler or "none"


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

    print(f"  {len(garages)} garages, fetched {cache['fetched']}")
    print(f"  {len(charger_map)} with chargers")

    print("Scoring...")
    results = []
    for g in garages:
        bbl = g["bbl"]
        sp = sprinkler_map.get(bbl, [])
        viols = violation_map.get(bbl, [])
        chargers = charger_map.get(bbl, None)

        risk_score, reasons, latest_sp = score_garage(g, sp, viols, chargers)

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
