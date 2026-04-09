"""
EV Fire Risk Scoring for ALL NYC Parking Garages

Expands scoring from only garages-with-chargers to all commercial
parking garages (G1, GU, GW + multi-story G0). EV charger presence
becomes a risk bonus, not a prerequisite.

Risk factors:
  - Age of structure (0-30 pts)
  - No sprinkler permits on record (0-25 pts)
  - Multi-story / below grade (0-10 pts)
  - Safety violations (0-20 pts)
  - EV charger presence and density (0-15 pts bonus)
"""

import json
import math
import time
import urllib.request
import urllib.parse
import csv
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).parent

BORO_CODE_TO_NAME = {
    "1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN",
    "4": "QUEENS", "5": "STATEN ISLAND",
}

GARAGE_OPERATORS = {
    "icon", "rapidpark", "parkit", "quik park", "ipark", "iparknyc",
    "ggmc", "parkright", "mpg", "champion", "apple parking",
    "central parking", "standard parking", "discount parking",
    "impark", "sp+", "laz parking", "propark",
}


def _parse_bbl(bbl):
    if not bbl:
        return None
    s = str(bbl).split(".")[0].zfill(10)
    if len(s) < 10:
        return None
    return s[0], s[1:6], s[6:10]


def _socrata_get(url, retries=3):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)  # exponential backoff: 1s, 2s, 4s
            else:
                return []


def haversine_ft(lat1, lon1, lat2, lon2):
    R = 20902231
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# 1. Load PLUTO garages
# ---------------------------------------------------------------------------

def load_target_garages():
    with open(DATA_DIR / "pluto_garages.json") as f:
        data = json.load(f)
    out = []
    for r in data["records"]:
        bldgclass = r.get("bldgclass", "")
        numfloors = float(r.get("numfloors") or 0)
        # G1 (commercial), GU/GW (condo), or multi-story G0
        if bldgclass not in ("G1", "GU", "GW"):
            if not (bldgclass == "G0" and numfloors > 1):
                continue
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
        out.append({
            "bbl": str(r.get("bbl", "")).split(".")[0].zfill(10),
            "address": r.get("address", ""),
            "borough": r.get("borough", ""),
            "bldgclass": bldgclass,
            "yearbuilt": int(r.get("yearbuilt") or 0),
            "numfloors": numfloors,
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
# 2. Load AFDC charger data and match to garages
# ---------------------------------------------------------------------------

def load_charger_stations():
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
            "name": s.get("station_name", ""),
            "address": s.get("street_address", ""),
            "lat": float(lat),
            "lon": float(lon),
            "l2_ports": int(s.get("ev_level2_evse_num") or 0),
            "dcfast_ports": int(s.get("ev_dc_fast_num") or 0),
            "facility_type": s.get("facility_type", ""),
            "open_date": s.get("open_date", ""),
            "network": s.get("ev_network", ""),
        })
    return out


def match_chargers_to_garages(garages, stations):
    """Match charger stations to garages using local PLUTO bulk data.

    Loads pluto_all.json (full 857K-record PLUTO dataset) and uses a
    spatial grid index for fast nearest-lot lookup. No per-station API calls.

    Returns (charger_map, extra_garages) where:
      charger_map: bbl -> list of matched stations
      extra_garages: list of non-garage PLUTO lots that have chargers
    """
    # Load full PLUTO and build spatial index
    print("  Loading full PLUTO dataset for spatial matching...")
    with open(DATA_DIR / "pluto_all.json") as f:
        pluto_all = json.load(f)

    grid = defaultdict(list)
    for lot in pluto_all["records"]:
        try:
            lat, lon = float(lot["latitude"]), float(lot["longitude"])
        except (ValueError, TypeError, KeyError):
            continue
        cell = (round(lat, 3), round(lon, 3))
        grid[cell].append(lot)
    print(f"  Indexed {len(pluto_all['records'])} lots in {len(grid)} grid cells")

    def find_nearest(lat, lon, max_dist_ft=800):
        cell = (round(lat, 3), round(lon, 3))
        candidates = []
        for dlat in [-0.001, 0, 0.001]:
            for dlon in [-0.001, 0, 0.001]:
                candidates.extend(grid.get((round(lat + dlat, 3), round(lon + dlon, 3)), []))
        best, best_dist = None, float("inf")
        for lot in candidates:
            try:
                d = haversine_ft(lat, lon, float(lot["latitude"]), float(lot["longitude"]))
            except:
                continue
            if d < best_dist:
                best_dist = d
                best = lot
        return best if best and best_dist <= max_dist_ft else None

    garage_bbls = {g["bbl"] for g in garages}
    charger_map = {}
    extra_garages = []
    seen_extra_bbls = set()

    for s in stations:
        if s.get("facility_type") == "FLEET_STATION":
            continue
        lot = find_nearest(s["lat"], s["lon"])
        if not lot:
            continue
        bbl = str(lot.get("bbl", "")).split(".")[0].zfill(10)
        charger_map.setdefault(bbl, []).append(s)
        if bbl not in garage_bbls and bbl not in seen_extra_bbls:
            extra_garages.append({
                "bbl": bbl,
                "address": lot.get("address", ""),
                "borough": lot.get("borough", ""),
                "bldgclass": lot.get("bldgclass", ""),
                "yearbuilt": int(lot.get("yearbuilt") or 0),
                "numfloors": float(lot.get("numfloors") or 0),
                "bldgarea": float(lot.get("bldgarea") or 0),
                "garagearea": float(lot.get("garagearea") or 0),
                "lotarea": float(lot.get("lotarea") or 0),
                "zipcode": lot.get("zipcode", ""),
                "lat": float(lot["latitude"]),
                "lon": float(lot["longitude"]),
                "zonedist1": lot.get("zonedist1", ""),
            })
            seen_extra_bbls.add(bbl)

    return charger_map, extra_garages


# ---------------------------------------------------------------------------
# 3. Batch DOB queries by block
# ---------------------------------------------------------------------------

def batch_sprinkler_permits(garages):
    """Query DOB for sprinkler permits, batched by boro+block."""
    # Group garages by boro+block
    blocks = defaultdict(list)
    for g in garages:
        parsed = _parse_bbl(g["bbl"])
        if not parsed:
            continue
        boro, block, lot = parsed
        blocks[(boro, block)].append((g["bbl"], lot))

    results = {}  # bbl -> list of permits
    total = len(blocks)

    print(f"  Querying DOB Permit Issuance for {total} blocks...")
    for i, ((boro, block), lots) in enumerate(blocks.items()):
        boro_name = BORO_CODE_TO_NAME.get(boro, "")
        # Build OR clause for all lots in this block
        lot_clauses = []
        for bbl, lot_4 in lots:
            lot_5 = lot_4.zfill(5)
            if lot_4 != lot_5:
                lot_clauses.extend([f"lot='{lot_5}'", f"lot='{lot_4}'"])
            else:
                lot_clauses.append(f"lot='{lot_5}'")

        lot_filter = " OR ".join(lot_clauses)
        params = urllib.parse.urlencode({
            "$where": f"borough='{boro_name}' AND block='{block}' AND ({lot_filter}) AND permit_subtype='SP'",
            "$limit": 200,
            "$order": "issuance_date DESC",
        })
        permits = _socrata_get(
            f"https://data.cityofnewyork.us/resource/ipu4-2q9a.json?{params}"
        )

        # Distribute permits back to individual BBLs
        for p in permits:
            p_lot = p.get("lot", "").lstrip("0") or "0"
            for bbl, lot_4 in lots:
                if lot_4.lstrip("0") == p_lot or lot_4.zfill(5).lstrip("0") == p_lot:
                    results.setdefault(bbl, []).append(p)
                    break

        if (i + 1) % 100 == 0:
            print(f"    {i + 1}/{total} blocks...")
            time.sleep(0.2)

    # Also query DOB NOW for each BBL individually (has bbl field)
    print(f"  Querying DOB NOW for {len(garages)} garages...")
    for i, g in enumerate(garages):
        bbl = g["bbl"]
        params = urllib.parse.urlencode({
            "$where": f"bbl='{bbl}' AND (work_type='Sprinklers' OR upper(job_description) like '%25SPRINKLER%25')",
            "$limit": 50,
        })
        now_permits = _socrata_get(
            f"https://data.cityofnewyork.us/resource/rbx6-tga4.json?{params}"
        )
        if now_permits:
            results.setdefault(bbl, []).extend(now_permits)

        if (i + 1) % 200 == 0:
            print(f"    {i + 1}/{len(garages)}...")
            time.sleep(0.2)

    return results


def batch_violations(garages):
    """Query DOB violations, batched by boro+block."""
    blocks = defaultdict(list)
    for g in garages:
        parsed = _parse_bbl(g["bbl"])
        if not parsed:
            continue
        boro, block, lot = parsed
        blocks[(boro, block)].append((g["bbl"], lot))

    results = {}
    total = len(blocks)

    print(f"  Querying DOB Violations for {total} blocks...")
    for i, ((boro, block), lots) in enumerate(blocks.items()):
        lot_clauses = []
        for bbl, lot_4 in lots:
            lot_5 = lot_4.zfill(5)
            if lot_4 != lot_5:
                lot_clauses.extend([f"lot='{lot_5}'", f"lot='{lot_4}'"])
            else:
                lot_clauses.append(f"lot='{lot_5}'")

        lot_filter = " OR ".join(lot_clauses)
        params = urllib.parse.urlencode({
            "$where": (
                f"boro='{boro}' AND block='{block}' AND ({lot_filter}) "
                f"AND (violation_type like '%SPRINKLER%' "
                f"OR violation_type like '%UNSAFE%' "
                f"OR violation_type like '%COMPROMISED%' "
                f"OR violation_type like '%IMEGNCY%' "
                f"OR violation_type like '%LL2604%')"
            ),
            "$limit": 200,
        })
        viols = _socrata_get(
            f"https://data.cityofnewyork.us/resource/3h2n-5cm9.json?{params}"
        )

        for v in viols:
            v_lot = v.get("lot", "").lstrip("0") or "0"
            for bbl, lot_4 in lots:
                if lot_4.lstrip("0") == v_lot or lot_4.zfill(5).lstrip("0") == v_lot:
                    results.setdefault(bbl, []).append(v)
                    break

        if (i + 1) % 100 == 0:
            print(f"    {i + 1}/{total} blocks...")
            time.sleep(0.2)

    return results


# ---------------------------------------------------------------------------
# 4. Scoring
# ---------------------------------------------------------------------------

def normalize_date(raw):
    """Convert DOB date formats to ISO YYYY-MM-DD."""
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
      - No sprinkler upgrade (0-25)
      - Multi-story (0-10)
      - Safety violations (0-20): tiered by severity
      - EV charger bonus (0-15): chargers present = more concentrated risk
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
        reasons.append(f"{critical} critical violation(s) (+{min(critical * 8, 20)})")
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


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------

def query_pluto_nearest(lat, lon, radius=0.004):
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
    if not best or best_dist > 1000:
        return None
    return {
        "bbl": str(best.get("bbl", "")).split(".")[0].zfill(10),
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
    }


def main():
    print("Loading target garages (G1/GU/GW + multi-story G0)...")
    garages = load_target_garages()
    print(f"  {len(garages)} garages")

    print("Loading EV charger stations...")
    stations = load_charger_stations()
    print(f"  {len(stations)} stations")

    print("Matching chargers to garages via PLUTO lookup...")
    print(f"  (This queries PLUTO for each of {len(stations)} stations)")
    charger_map, extra_garages = match_chargers_to_garages(garages, stations)
    print(f"  {len(charger_map)} buildings have chargers")
    print(f"  {sum(1 for bbl in charger_map if bbl in {g['bbl'] for g in garages})} are G-class garages")
    print(f"  {len(extra_garages)} additional non-garage buildings with chargers")

    all_garages = garages + extra_garages
    print(f"  Total to score: {len(all_garages)}")

    print("\nQuerying DOB (batched by block)...")
    sprinkler_map = batch_sprinkler_permits(all_garages)
    print(f"  {sum(1 for v in sprinkler_map.values() if v)} garages have sprinkler permits")

    violation_map = batch_violations(all_garages)
    print(f"  {sum(1 for v in violation_map.values() if v)} garages have safety violations")

    print("\nScoring...")
    results = []
    for g in all_garages:
        bbl = g["bbl"]
        sp = sprinkler_map.get(bbl, [])
        viols = violation_map.get(bbl, [])
        chargers = charger_map.get(bbl, None)

        risk_score, reasons, latest_sp = score_garage(g, sp, viols, chargers)

        has_chargers = chargers is not None
        total_ports = sum(c["l2_ports"] + c["dcfast_ports"] for c in chargers) if chargers else 0
        charger_names = "; ".join(c["name"] for c in chargers) if chargers else ""

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
            "has_chargers": has_chargers,
            "total_ev_ports": total_ports,
            "charger_names": charger_names,
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
        print("\nCross-referencing with OSM parking types...")
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
        print("\nNo OSM data found, skipping parking type enrichment")
        for r in results:
            r["osm_parking_type"] = ""
            r["osm_name"] = ""

    output = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
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
            "risk_score", "address", "borough", "bldgclass",
            "yearbuilt", "numfloors", "has_chargers", "total_ev_ports",
            "sprinkler_permits_count", "sprinkler_last_date",
            "fire_violations_count", "bldgarea_sqft", "lat", "lon", "reasons",
        ])
        writer.writeheader()
        for r in results:
            row = {k: r[k] for k in writer.fieldnames if k != "reasons"}
            row["reasons"] = "; ".join(r["reasons"])
            writer.writerow(row)

    # Summary
    print("\n" + "=" * 70)
    print("ALL NYC PARKING GARAGES — EV FIRE RISK SCORES")
    print("=" * 70)
    print(f"\nTotal garages scored: {len(results)}")
    print(f"Garages with EV chargers: {output['garages_with_chargers']}")
    print(f"\nRisk distribution:")
    for k, v in output["risk_distribution"].items():
        print(f"  {k.replace('_', ' ').title()}: {v}")

    print(f"\n{'—' * 70}")
    print("TOP 25 HIGHEST RISK")
    print(f"{'—' * 70}")
    print(f"{'Score':>5}  {'Year':>5}  {'Flrs':>4}  {'EV':>3}  {'Sprk':>4}  {'Viol':>4}  Address")
    print(f"{'—' * 70}")
    for r in results[:25]:
        yb = r["yearbuilt"] if r["yearbuilt"] > 0 else "????"
        ev = "Y" if r["has_chargers"] else "-"
        print(f"{r['risk_score']:>5}  {yb:>5}  {r['numfloors']:>4.0f}  {ev:>3}  "
              f"{r['sprinkler_permits_count']:>4}  {r['fire_violations_count']:>4}  "
              f"{r['address'][:45]}, {r['borough']}")

    print(f"\nFull results: {DATA_DIR / 'risk_scores_all.csv'}")
    print(f"JSON data:    {DATA_DIR / 'risk_scores_all.json'}")


if __name__ == "__main__":
    main()
