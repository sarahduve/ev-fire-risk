"""
Fetch all raw data for NYC parking garage EV fire risk scoring.

Downloads PLUTO, AFDC, OSM, and DOB data, matches chargers to garages,
and saves everything to cached_data.json. This is the slow/expensive step
(~30 min, ~4,000 API calls). Re-run only when you need fresh data.

Score changes, label tweaks, and formula adjustments should use
score_garages.py instead, which reads from the cache instantly.
"""

import json
import math
import time
import os
import urllib.request
import urllib.parse
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent

BORO_CODE_TO_NAME = {
    "1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN",
    "4": "QUEENS", "5": "STATEN ISLAND",
}


def _socrata_get(url, retries=3):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read())
        except Exception:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return []


def _parse_bbl(bbl):
    if not bbl:
        return None
    s = str(bbl).split(".")[0].zfill(10)
    if len(s) < 10:
        return None
    return s[0], s[1:6], s[6:10]


def haversine_ft(lat1, lon1, lat2, lon2):
    R = 20902231
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# 1. Load PLUTO garages from local file
# ---------------------------------------------------------------------------

def load_target_garages():
    with open(DATA_DIR / "pluto_garages.json") as f:
        data = json.load(f)
    out = []
    for r in data["records"]:
        bldgclass = r.get("bldgclass", "")
        numfloors = float(r.get("numfloors") or 0)
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
# 2. Load AFDC charger stations
# ---------------------------------------------------------------------------

def load_charger_stations():
    with open(DATA_DIR / "afdc_data.json") as f:
        data = json.load(f)
    out = []
    for s in data["garage_stations_data"]:
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


# ---------------------------------------------------------------------------
# 3. Match chargers to garages using local PLUTO bulk data
# ---------------------------------------------------------------------------

def match_chargers_to_garages(garages, stations):
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
# 4. Bulk download DOB data and match locally
# ---------------------------------------------------------------------------

def _bulk_paginate(base_url, where_clause, select_fields, label):
    """Download a full filtered dataset via paginated Socrata queries."""
    all_records = []
    offset = 0
    limit = 50000
    while True:
        params = urllib.parse.urlencode({
            "$where": where_clause,
            "$select": select_fields,
            "$limit": limit,
            "$offset": offset,
            "$order": ":id",
        })
        url = f"{base_url}?{params}"
        data = _socrata_get(url)
        if not data:
            break
        all_records.extend(data)
        print(f"    {label}: {len(all_records)} records...")
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.5)
    return all_records


def _make_bbl(boro_code, block, lot):
    """Construct a 10-digit BBL from boro code, block, and lot."""
    return f"{boro_code}{block.zfill(5)}{lot.zfill(4)}"


def _normalize_bbl_from_permit(p):
    """Extract a normalized BBL from a DOB permit record."""
    boro = p.get("borough", "")
    block = p.get("block", "").zfill(5)
    lot = p.get("lot", "").lstrip("0").zfill(4)
    boro_code = {"MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3",
                 "QUEENS": "4", "STATEN ISLAND": "5"}.get(boro, "0")
    return _make_bbl(boro_code, block, lot)


def _normalize_bbl_from_violation(v):
    """Extract a normalized BBL from a DOB violation record."""
    boro = v.get("boro", "0")
    block = v.get("block", "").zfill(5)
    lot = v.get("lot", "").lstrip("0").zfill(4)
    return _make_bbl(boro, block, lot)


def bulk_sprinkler_permits(garage_bbls):
    """Bulk download all sprinkler permits and DOB NOW sprinkler work, match by BBL."""
    results = {}

    # 1) DOB Permit Issuance — all sprinkler permits in NYC (~140K total)
    print("  Downloading all sprinkler permits (DOB Permit Issuance)...")
    permits = _bulk_paginate(
        "https://data.cityofnewyork.us/resource/ipu4-2q9a.json",
        "permit_subtype='SP'",
        "borough,block,lot,issuance_date,permit_status,work_type",
        "Sprinkler permits",
    )
    for p in permits:
        bbl = _normalize_bbl_from_permit(p)
        if bbl in garage_bbls:
            results.setdefault(bbl, []).append(p)

    matched_old = sum(1 for v in results.values() if v)
    print(f"    Matched {matched_old} garages from DOB Permit Issuance")

    # 2) DOB NOW Build — sprinkler-related work (has bbl field directly)
    print("  Downloading sprinkler work (DOB NOW Build)...")
    now_permits = _bulk_paginate(
        "https://data.cityofnewyork.us/resource/rbx6-tga4.json",
        "work_type='Sprinklers' OR upper(job_description) like '%25SPRINKLER%25'",
        "bbl,approved_date,work_type,job_description",
        "DOB NOW sprinkler",
    )
    for p in now_permits:
        bbl = str(p.get("bbl", "")).split(".")[0].zfill(10)
        if bbl in garage_bbls:
            results.setdefault(bbl, []).append(p)

    matched_now = sum(1 for v in results.values() if v) - matched_old
    print(f"    Matched {matched_now} additional garages from DOB NOW")

    return results


def bulk_violations(garage_bbls):
    """Bulk download safety-related violations and match by BBL."""
    print("  Downloading safety violations (DOB Violations)...")
    viols = _bulk_paginate(
        "https://data.cityofnewyork.us/resource/3h2n-5cm9.json",
        ("violation_type like '%SPRINKLER%' "
         "OR violation_type like '%UNSAFE%' "
         "OR violation_type like '%COMPROMISED%' "
         "OR violation_type like '%IMEGNCY%' "
         "OR violation_type like '%LL2604%'"),
        "boro,block,lot,violation_type,description,issue_date",
        "Safety violations",
    )

    results = {}
    for v in viols:
        bbl = _normalize_bbl_from_violation(v)
        if bbl in garage_bbls:
            results.setdefault(bbl, []).append(v)

    print(f"    Matched {sum(1 for v in results.values() if v)} garages with violations")
    return results


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("FETCH DATA — downloading raw data from NYC Open Data + DOB")
    print("=" * 60)

    print("\nLoading target garages (G1/GU/GW + multi-story G0)...")
    garages = load_target_garages()
    print(f"  {len(garages)} garages")

    print("\nLoading EV charger stations...")
    stations = load_charger_stations()
    print(f"  {len(stations)} stations")

    print("\nMatching chargers to garages via PLUTO lookup...")
    charger_map, extra_garages = match_chargers_to_garages(garages, stations)
    print(f"  {len(charger_map)} buildings have chargers")
    print(f"  {len(extra_garages)} additional non-garage buildings with chargers")

    all_garages = garages + extra_garages
    print(f"  Total buildings: {len(all_garages)}")

    garage_bbls = {g["bbl"] for g in all_garages}

    print("\nBulk downloading DOB data...")
    sprinkler_map = bulk_sprinkler_permits(garage_bbls)
    print(f"  Total: {sum(1 for v in sprinkler_map.values() if v)} garages have sprinkler permits")

    violation_map = bulk_violations(garage_bbls)
    print(f"  Total: {sum(1 for v in violation_map.values() if v)} garages have safety violations")

    # Save cache
    cache = {
        "fetched": time.strftime("%Y-%m-%d %H:%M:%S"),
        "garages": all_garages,
        "charger_map": charger_map,
        "sprinkler_map": sprinkler_map,
        "violation_map": violation_map,
    }

    cache_path = DATA_DIR / "cached_data.json"
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    print(f"\nCached data saved to {cache_path}")
    print(f"  {len(all_garages)} garages, {len(charger_map)} with chargers")
    print(f"  Run score_garages.py to generate risk scores (instant, no API calls)")


if __name__ == "__main__":
    main()
