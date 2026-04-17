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
# 1. Load PLUTO buildings with parking from local bulk file (v1)
#
# v1 change: "parking buildings" means EITHER a dedicated G-class garage
# (G0 multi-story, G1, GU, GW) OR any non-G building with garagearea >= 1000
# sqft (roughly 3+ cars). This catches apartment / office / institutional
# buildings with ground-floor or basement garages, which PLUTO classifies by
# the dominant use upstairs rather than by the garage itself.
#
# Pre-v1 scored only G-class plus non-G buildings that happened to be matched
# to an AFDC charger — which made the non-G subset definitionally "has EV
# charger" and broke any EV-risk correlation analysis.
# ---------------------------------------------------------------------------

GARAGEAREA_MIN_SQFT = 1000  # ~3 cars; flag garagearea < 2500 as small

# Building classes to exclude even if they have garagearea. These are not
# parking garages in any meaningful sense:
#   T = Transportation (piers, airport terminals, bus terminals, ferry buildings)
#   Q = Recreation (parks, rec centers — "garagearea" is often vehicle maintenance)
#   U = Utility (power substations, water treatment)
EXCLUDE_CLASSES = ("T", "Q", "U")

# Map building class -> broad category for UI filtering / popup labels
def _derive_garage_type(bldgclass):
    if not bldgclass:
        return "other"
    letter = bldgclass[0]
    if letter == "G":
        return "standalone"
    if letter in ("A", "B", "C", "D", "R"):
        return "under_residential"
    if letter in ("K", "O", "S"):
        return "under_commercial"
    if letter in ("H", "I", "M"):
        return "institutional"
    return "other"


def _is_gclass_garage(bldgclass, numfloors):
    """Keep the v0 G-class criteria: G1/GU/GW always, G0 only if multi-story."""
    if bldgclass in ("G1", "GU", "GW"):
        return True
    if bldgclass == "G0" and numfloors > 1:
        return True
    return False


def _normalize_pluto_record(r):
    """Convert a PLUTO record dict to the garage shape used downstream."""
    lat = r.get("latitude")
    lon = r.get("longitude")
    if not lat or not lon:
        return None
    try:
        lat, lon = float(lat), float(lon)
    except (ValueError, TypeError):
        return None
    if lat == 0 or lon == 0:
        return None
    bldgclass = r.get("bldgclass", "") or ""
    numfloors = float(r.get("numfloors") or 0)
    garagearea = float(r.get("garagearea") or 0)
    return {
        "bbl": str(r.get("bbl", "")).split(".")[0].zfill(10),
        "address": r.get("address", ""),
        "borough": r.get("borough", ""),
        "bldgclass": bldgclass,
        "yearbuilt": int(r.get("yearbuilt") or 0),
        "numfloors": numfloors,
        "bldgarea": float(r.get("bldgarea") or 0),
        "garagearea": garagearea,
        "lotarea": float(r.get("lotarea") or 0),
        "zipcode": r.get("zipcode", ""),
        "lat": lat,
        "lon": lon,
        "zonedist1": r.get("zonedist1", ""),
        "garage_type": _derive_garage_type(bldgclass),
        "small_garage": 0 < garagearea < 2500,
    }


def load_target_garages():
    """Load all parking buildings from pluto_all.json: G-class + non-G with garagearea."""
    with open(DATA_DIR / "pluto_all.json") as f:
        data = json.load(f)
    out = []
    gclass_count = non_g_count = 0
    for r in data["records"]:
        bldgclass = r.get("bldgclass", "") or ""
        numfloors = float(r.get("numfloors") or 0)
        garagearea = float(r.get("garagearea") or 0)
        # Exclude non-parking classes (piers, airports, parks, utilities)
        if any(bldgclass.startswith(ex) for ex in EXCLUDE_CLASSES):
            continue
        is_g = _is_gclass_garage(bldgclass, numfloors)
        has_garage_area = (not bldgclass.startswith("G")) and garagearea >= GARAGEAREA_MIN_SQFT
        if not (is_g or has_garage_area):
            continue
        norm = _normalize_pluto_record(r)
        if norm is None:
            continue
        out.append(norm)
        if is_g:
            gclass_count += 1
        else:
            non_g_count += 1
    print(f"  Loaded {len(out)} parking buildings: {gclass_count} G-class + {non_g_count} non-G with garagearea>={GARAGEAREA_MIN_SQFT}")
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
        # Skip NJ / other-state stations
        if (s.get("state") or "").upper() != "NY":
            continue
        out.append({
            "id": s.get("id"),
            "name": s.get("station_name", ""),
            "address": s.get("street_address", ""),
            "city": s.get("city", ""),
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
# 3. Match chargers to buildings (v1 cascade)
#
# (A) PAD address lookup via NYC Planning Labs Geosearch API
# (B) ArcGIS MapPLUTO point-in-polygon
# (C) shapely nearest-polygon-edge within 20 ft
# (D) unmatched (charger kept as floating map dot, no scoring effect)
#
# All results cached in charger_bbl_map.json to avoid re-querying on
# subsequent runs. Safe to delete the cache if you want to force a refresh.
# ---------------------------------------------------------------------------

GEOSEARCH_URL = "https://geosearch.planninglabs.nyc/v2/search"
ARCGIS_MAPPLUTO = "https://a841-dotweb01.nyc.gov/arcgis/rest/services/GAZETTEER/MapPLUTO/MapServer/0/query"


def _geosearch_bbl(address, city="", focus_lat=None, focus_lon=None, max_drift_ft=1000):
    """Resolve a street address to a BBL via Planning Labs Geosearch (PAD-backed).

    Iterates features in relevance-rank order and picks the first one whose
    coordinates are within max_drift_ft of the charger's own lat/lon. This
    handles two failure modes:

    1. Fuzzy ranked matches: searching "251 Avenue C" can return "251 NEW
       JERSEY AVENUE" as the top result because of token overlap. Geographic
       sanity check skips these.

    2. Cross-borough ambiguity: "Avenue C" exists in Manhattan AND Brooklyn.
       The charger coord tells us which borough. We skip Brooklyn results when
       the charger is in Manhattan.

    Relevance ranking wins when it produces a feature near the charger;
    distance-based fallback only kicks in when the top-ranked feature is far
    from the charger.
    """
    if not address:
        return None
    text = f"{address} {city}".strip() if city else address
    params = {"text": text, "size": 5}
    if focus_lat is not None and focus_lon is not None:
        params["focus.point.lat"] = focus_lat
        params["focus.point.lon"] = focus_lon
    try:
        url = f"{GEOSEARCH_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": "ev-fire-risk/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception:
        return None
    features = data.get("features") or []
    if not features:
        return None

    def _bbl_of(feature):
        pad = feature.get("properties", {}).get("addendum", {}).get("pad", {})
        bbl = pad.get("bbl")
        return str(bbl).zfill(10) if bbl else None

    # Without a focus point we have nothing to validate against; trust ranking.
    if focus_lat is None or focus_lon is None:
        return _bbl_of(features[0])

    # With focus point: first feature within max_drift_ft wins. This preserves
    # relevance ranking (the intended address is usually #1) while skipping
    # fuzzy matches that landed on a totally different block.
    for f in features:
        coords = f.get("geometry", {}).get("coordinates")
        if not coords:
            continue
        flon, flat = coords[0], coords[1]
        if haversine_ft(focus_lat, focus_lon, flat, flon) <= max_drift_ft:
            return _bbl_of(f)
    return None


def _arcgis_bbox_query(lat, lon, buffer_deg=0.0003):
    """Query MapPLUTO for polygons intersecting a small bbox around the point."""
    xmin, xmax = lon - buffer_deg, lon + buffer_deg
    ymin, ymax = lat - buffer_deg, lat + buffer_deg
    params = {
        "geometry": json.dumps({
            "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "spatialReference": {"wkid": 4326},
        }),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "BBL,Address,BldgClass",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
    }
    try:
        url = f"{ARCGIS_MAPPLUTO}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": "ev-fire-risk/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except Exception:
        return {"features": []}


def _pip_and_nearest(lat, lon, max_edge_ft=20):
    """Return (bbl, method) via point-in-polygon then nearest-edge fallback."""
    from shapely.geometry import Point, shape  # lazy import so non-match users don't pay

    data = _arcgis_bbox_query(lat, lon)
    features = data.get("features", [])
    if not features:
        return None, None

    pt = Point(lon, lat)
    best_dist_ft, best_bbl = float("inf"), None
    for f in features:
        rings = f.get("geometry", {}).get("rings")
        if not rings:
            continue
        poly = shape({"type": "Polygon", "coordinates": rings})
        bbl = str(int(f["attributes"]["BBL"])).zfill(10)
        if poly.contains(pt):
            return bbl, "point_in_polygon"
        # Approximate deg->ft at NYC latitude; fine for ranking under ~50ft
        dist_ft = poly.distance(pt) * 280000
        if dist_ft < best_dist_ft:
            best_dist_ft, best_bbl = dist_ft, bbl
    if best_bbl and best_dist_ft <= max_edge_ft:
        return best_bbl, "nearest_edge"
    return None, None


def _load_pluto_by_bbl_index():
    """Build an in-memory BBL -> record index from pluto_all.json."""
    with open(DATA_DIR / "pluto_all.json") as f:
        data = json.load(f)
    idx = {}
    for r in data["records"]:
        bbl = str(r.get("bbl", "")).split(".")[0].zfill(10)
        idx[bbl] = r
    return idx


def match_chargers_to_garages(garages, stations):
    """Cascade: PAD address -> ArcGIS PIP -> nearest-edge -> unmatched.

    Returns (charger_map, extra_garages, unmatched, stats).
      charger_map: {bbl: [station_dict, ...]}
      extra_garages: non-garage-set BBLs that a charger matched to (for append to garage list)
      unmatched: stations with no BBL match (floating map dots)
      stats: counts per match method
    """
    cache_path = DATA_DIR / "charger_bbl_map.json"
    cache = {}
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    print("  Building PLUTO BBL index for extra-garage lookup...")
    pluto_by_bbl = _load_pluto_by_bbl_index()

    garage_bbls = {g["bbl"] for g in garages}
    charger_map = {}
    extra_garages = []
    seen_extra_bbls = set()
    unmatched = []
    stats = {"pad_address": 0, "point_in_polygon": 0, "nearest_edge": 0,
             "unmatched": 0, "cached": 0, "pip_recheck": 0}

    for i, s in enumerate(stations):
        if s.get("facility_type") == "FLEET_STATION":
            continue

        key = str(s.get("id") or f"{s['lat']},{s['lon']}")
        cached = cache.get(key)
        bbl = None
        method = None

        if cached:
            bbl = cached.get("bbl")
            method = cached.get("method")
            stats["cached"] += 1
        else:
            # Step A: PAD address lookup biased by charger coords
            bbl = _geosearch_bbl(
                s.get("address", ""), s.get("city", ""),
                focus_lat=s["lat"], focus_lon=s["lon"],
            )
            if bbl:
                method = "pad_address"
            else:
                # Step B/C: spatial fallback
                bbl, method = _pip_and_nearest(s["lat"], s["lon"], max_edge_ft=20)
            cache[key] = {"bbl": bbl, "method": method,
                          "address": s.get("address"), "name": s.get("name")}
            # Throttle external APIs
            time.sleep(0.05)

        # If resolved BBL is not in our target set, try PIP to find the
        # correct building.  Charger addresses often use a garage entrance
        # on a different street than the building's PLUTO primary address,
        # causing PAD to resolve to a neighboring lot.
        if bbl and bbl not in garage_bbls:
            pip_bbl, pip_method = _pip_and_nearest(
                s["lat"], s["lon"], max_edge_ft=50,
            )
            if pip_bbl and pip_bbl in garage_bbls:
                # PIP found a target building — use it instead
                bbl = pip_bbl
                method = pip_method
                stats["pip_recheck"] += 1
            elif pip_bbl and pip_bbl == bbl:
                # PIP confirms charger is at this non-target building
                # (PLUTO under-reports garagearea). Keep as extra garage.
                pass
            else:
                # PIP found a different non-target BBL or nothing — ambiguous
                bbl = None
                method = None
            cache[key] = {"bbl": bbl, "method": method,
                          "address": s.get("address"), "name": s.get("name")}
            time.sleep(0.15)

        if method and method in stats:
            stats[method] += 1
        elif method is None and bbl is None:
            stats["unmatched"] += 1

        if not bbl:
            unmatched.append(s)
            continue

        charger_map.setdefault(bbl, []).append(s)

        # If BBL isn't in our pre-expanded garage set, add it as an extra
        # (charger is confirmed at this building via PIP, but PLUTO doesn't
        # report garagearea — e.g. 100 Jay St has a real garage entrance
        # on York St but garagearea=0 in PLUTO)
        if bbl not in garage_bbls and bbl not in seen_extra_bbls:
            r = pluto_by_bbl.get(bbl)
            if r:
                norm = _normalize_pluto_record(r)
                if norm:
                    extra_garages.append(norm)
                    seen_extra_bbls.add(bbl)

        if (i + 1) % 25 == 0:
            print(f"    matched {i+1}/{len(stations)}...")

    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)

    return charger_map, extra_garages, unmatched, stats


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
# 4b. FDNY violations from OATH/ECB hearings dataset (jz4z-kudi)
#
# Why OATH and not the FDNY-specific datasets? The FDNY-named datasets on NYC
# Open Data (ktas-47y7, avgm-ztsb) contain only 1-44 records each. The actual
# FDNY violation universe (~926K records) is in the OATH ECB hearings dataset
# because that's where Fire Code violations get adjudicated. We filter by
# issuing_agency='FIRE DEPARTMENT OF NYC' and the relevant fire-protection
# charge codes.
#
# Key fields:
#   compliance_status: 'All Terms Met' (resolved), 'Compliance Due' (still
#     unfixed), 'Both Due' (unfixed + owes money), 'Penalty Due' (fixed, owes)
#   hearing_result: 'IN VIOLATION' (proven), 'DEFAULTED' (no-show), etc.
#
# Most charges per ticket have a charge_1 / charge_2 / charge_3 / charge_4
# field — a single inspection visit can result in multiple charges.
# ---------------------------------------------------------------------------

# Charge codes that map to fire suppression / fire protection systems.
# BF12 = "FAIL TO MAINTAIN SPK STD SUPP SYST" (sprinkler/standpipe direct)
# BF20 = "INSPECTION AND TESTING" (general fire system test failure)
FDNY_FIRE_CODES = ("BF12", "BF20")
FDNY_FIRE_DESC_KEYWORDS = (
    "FIRE PROTECTION SYSTEM",
    "INSPECTION AND TESTING",
    "FAIL TO CONDUCT REQUIRED TEST",
    "FAIL TO MAINTAIN SPK",
    "PORTABLE FIRE EXTINGUISHER",
    "STANDPIPE",
)

# Excluded keyword: false-alarm management is technically a fire system charge
# but doesn't speak to suppression readiness. Filter it out.
FDNY_FIRE_DESC_EXCLUDE = ("UNNECESSARY UNWARRANTED ALARM", "UNWANTED ALARM")


def _is_fire_suppression_charge(rec):
    """Return True if any charge on this ticket is fire-suppression-relevant."""
    for i in ("1", "2", "3", "4"):
        code = (rec.get(f"charge_{i}_code") or "").upper()
        desc = (rec.get(f"charge_{i}_code_description") or "").upper()
        if not code and not desc:
            continue
        if any(ex in desc for ex in FDNY_FIRE_DESC_EXCLUDE):
            continue
        if code in FDNY_FIRE_CODES:
            return True
        if any(kw in desc for kw in FDNY_FIRE_DESC_KEYWORDS):
            return True
    return False


def _fdny_bbl(rec):
    """Construct BBL from FDNY violation location fields."""
    boro_name = (rec.get("violation_location_borough") or "").upper()
    boro_code = {"MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3",
                 "QUEENS": "4", "STATEN ISLAND": "5"}.get(boro_name, "0")
    block = (rec.get("violation_location_block_no") or "").zfill(5)
    lot = (rec.get("violation_location_lot_no") or "").lstrip("0").zfill(4)
    return f"{boro_code}{block}{lot}"


def _is_open_fdny(rec):
    """Open = building still has work to do (compliance not met)."""
    return (rec.get("compliance_status") or "") in ("Compliance Due", "Both Due")


def _is_resolved_fdny(rec):
    return (rec.get("compliance_status") or "") == "All Terms Met"


def bulk_fdny_violations(garage_bbls):
    """Download all FDNY violations from OATH dataset, filter to fire-protection
    charges affecting the garage BBL set, and return a BBL -> [{...}] map."""
    print("  Downloading FDNY violations from OATH/ECB hearings dataset...")
    base_url = "https://data.cityofnewyork.us/resource/jz4z-kudi.json"
    where = "issuing_agency='FIRE DEPARTMENT OF NYC'"
    select = ("ticket_number,violation_date,violation_location_borough,"
              "violation_location_block_no,violation_location_lot_no,"
              "hearing_result,compliance_status,balance_due,penalty_imposed,"
              "charge_1_code,charge_1_code_description,"
              "charge_2_code,charge_2_code_description,"
              "charge_3_code,charge_3_code_description,"
              "charge_4_code,charge_4_code_description")
    raw = _bulk_paginate(base_url, where, select, "FDNY violations")
    print(f"    Downloaded {len(raw)} FDNY violations total")

    fire_recs = [r for r in raw if _is_fire_suppression_charge(r)]
    print(f"    {len(fire_recs)} are fire-suppression-relevant")

    by_bbl = {}
    open_count = resolved_count = 0
    for rec in fire_recs:
        bbl = _fdny_bbl(rec)
        if bbl not in garage_bbls:
            continue
        # Trim record to what's useful downstream
        trimmed = {
            "date": (rec.get("violation_date") or "")[:10],
            "compliance_status": rec.get("compliance_status") or "",
            "hearing_result": rec.get("hearing_result") or "",
            "is_open": _is_open_fdny(rec),
            "is_resolved": _is_resolved_fdny(rec),
            "charges": [
                {"code": rec.get(f"charge_{i}_code") or "",
                 "desc": rec.get(f"charge_{i}_code_description") or ""}
                for i in ("1", "2", "3", "4")
                if rec.get(f"charge_{i}_code") or rec.get(f"charge_{i}_code_description")
            ],
        }
        by_bbl.setdefault(bbl, []).append(trimmed)
        if trimmed["is_open"]:
            open_count += 1
        elif trimmed["is_resolved"]:
            resolved_count += 1

    print(f"    Matched {len(by_bbl)} garages with FDNY fire-protection violations")
    print(f"    {open_count} open (unresolved) | {resolved_count} resolved (closed)")
    return by_bbl


# ---------------------------------------------------------------------------
# 5. Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("FETCH DATA — downloading raw data from NYC Open Data + DOB")
    print("=" * 60)

    print("\nLoading target garages (G-class + non-G with garagearea>=1000)...")
    garages = load_target_garages()
    print(f"  {len(garages)} garages")

    print("\nLoading EV charger stations...")
    stations = load_charger_stations()
    print(f"  {len(stations)} stations")

    print("\nMatching chargers via PAD -> PIP -> nearest-edge cascade...")
    charger_map, extra_garages, unmatched, match_stats = match_chargers_to_garages(garages, stations)
    print(f"  match methods: {match_stats}")
    print(f"  {len(charger_map)} buildings have chargers")
    print(f"  {len(extra_garages)} extra (matched to BBLs not in pre-expanded set)")
    print(f"  {len(unmatched)} chargers unmatched (will appear as floating dots)")

    all_garages = garages + extra_garages
    print(f"  Total buildings: {len(all_garages)}")

    garage_bbls = {g["bbl"] for g in all_garages}

    print("\nBulk downloading DOB data...")
    sprinkler_map = bulk_sprinkler_permits(garage_bbls)
    print(f"  Total: {sum(1 for v in sprinkler_map.values() if v)} garages have sprinkler permits")

    violation_map = bulk_violations(garage_bbls)
    print(f"  Total: {sum(1 for v in violation_map.values() if v)} garages have safety violations")

    print("\nBulk downloading FDNY fire-protection violations from OATH/ECB...")
    fdny_map = bulk_fdny_violations(garage_bbls)

    # v1.3 DOB signals. Implemented in patch_cache_v1_3 so the same code path
    # works for both full-fetch and incremental patch flows.
    print("\nBulk downloading DOB ECB + DOB NOW + LL2604 (v1.3 signals)...")
    from patch_cache_v1_3 import pull_dob_ecb, pull_dob_now, pull_ll2604_active
    dob_ecb_map = pull_dob_ecb(garage_bbls)
    dob_now_map = pull_dob_now(garage_bbls)
    ll2604_map = pull_ll2604_active(garage_bbls)

    # Save cache
    cache = {
        "fetched": time.strftime("%Y-%m-%d %H:%M:%S"),
        "garages": all_garages,
        "charger_map": charger_map,
        "unmatched_chargers": unmatched,
        "match_stats": match_stats,
        "fdny_violation_map": fdny_map,
        "sprinkler_map": sprinkler_map,
        "violation_map": violation_map,
        "dob_ecb_map": dob_ecb_map,
        "dob_now_map": dob_now_map,
        "ll2604_map": ll2604_map,
    }

    cache_path = DATA_DIR / "cached_data.json"
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    print(f"\nCached data saved to {cache_path}")
    print(f"  {len(all_garages)} garages, {len(charger_map)} with chargers")
    print(f"  Run score_garages.py to generate risk scores (instant, no API calls)")


if __name__ == "__main__":
    main()
