"""
One-off cache patch for v1.3: add DOB ECB + DOB NOW + narrow LL2604 signals
to existing cached_data.json without re-running the full 20-min fetch.

Adds to cache:
  - dob_ecb_map   : BBL -> [active+uncured ECB Class 1/2 records with fire-relevant descriptions or categories]
  - dob_now_map   : BBL -> [active DOB NOW records on parking-structure/fire-system devices]
  - ll2604_map    : BBL -> [active LL2604 records]  (replaces legacy violation_map signal for this slice)

Future clean runs via fetch_data.py will produce the same shape.
"""

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).parent
CACHE_PATH = DATA_DIR / "cached_data.json"


def _normalize_boro_block_lot_to_bbl(boro, block, lot):
    """BBL = boro(1) + block(5) + lot(4).  Datasets have inconsistent lot padding."""
    block5 = str(block or "").zfill(5)
    lot4 = str(lot or "").lstrip("0").zfill(4)
    return f"{boro}{block5}{lot4}"


def _chunk(iterable, n):
    it = list(iterable)
    for i in range(0, len(it), n):
        yield it[i : i + n]


def _socrata_fetch(base_url, where, select, limit=50000, timeout=90):
    """Fetch one Socrata query. Not paginated — `limit` should exceed result size."""
    params = {"$where": where, "$limit": str(limit)}
    if select:
        params["$select"] = select
    url = base_url + "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


# ---------------------------------------------------------------------------
# DOB ECB Violations (6bgk-3dad) — primary source of Class 1/2 hazard signal
# ---------------------------------------------------------------------------

ECB_URL = "https://data.cityofnewyork.us/resource/6bgk-3dad.json"
ECB_SELECT = (
    "boro,block,lot,ecb_violation_number,ecb_violation_status,"
    "certification_status,severity,violation_type,issue_date,"
    "violation_description"
)
ECB_BAD_CERT = {
    "NO COMPLIANCE RECORDED",
    "CERTIFICATE DISAPPROVED",
    "REINSPECTION SHOWS STILL IN VIOLATION",
    "CERTIFICATE PENDING",
    None,
}
FIRE_KEYWORDS = (
    "UNSAFE", "FIRE", "EXIT", "EGRESS", "ALARM",
    "SPRINKLER", "STANDPIPE", "SMOKE", "SUPPRESS",
)
FIREMAN_SVC_KEYWORDS = (
    "FIREMAN", "PHASE I", "PHASE 1", "FIRE SERVICE", "DOOR LOCK",
)


def _classify_ecb(r):
    """Return one of:
      'construction_fire'  — Class 1 Construction with fire keyword in description
      'construction_other' — Class 1 Construction without fire keyword
      'elevator_fireman'   — Class 1 Elevator with fireman-service keyword
      'elevator_other'     — Class 1 Elevator without fireman-service keyword
      'class2_fire'        — Class 2 with fire keyword (regardless of category)
      None                 — dropped (not fire-relevant enough)
    """
    sev = (r.get("severity") or "").upper()
    is_class1 = sev in ("CLASS - 1", "HAZARDOUS")
    is_class2 = sev == "CLASS - 2"
    if not (is_class1 or is_class2):
        return None
    cat = r.get("violation_type") or ""
    desc = (r.get("violation_description") or "").upper()
    has_fire_kw = any(k in desc for k in FIRE_KEYWORDS)
    has_fireman_kw = any(k in desc for k in FIREMAN_SVC_KEYWORDS)

    if is_class1:
        if cat == "Construction":
            return "construction_fire" if has_fire_kw else "construction_other"
        if cat == "Elevators":
            return "elevator_fireman" if has_fireman_kw else "elevator_other"
        # Treat "Unknown" Class 1 as construction_other — small volume, catch-all
        if cat == "Unknown":
            return "construction_other"
        # Plumbing/Boilers/other: include only when description mentions fire
        return "construction_other" if has_fire_kw else None

    # Class 2 — only keep if fire-keyword
    if has_fire_kw:
        return "class2_fire"
    return None


def pull_dob_ecb(garage_bbls, chunk_size=40):
    """Pull active+uncured ECB records on our garage BBLs, classify per FDNY expert."""
    print(f"Pulling DOB ECB (6bgk-3dad) for {len(garage_bbls)} garages in chunks of {chunk_size}...")
    bbls_sorted = sorted(garage_bbls)
    all_rows = []
    for i, chunk in enumerate(_chunk(bbls_sorted, chunk_size)):
        clauses = [
            f"(boro='{b[0]}' AND block='{b[1:6]}' AND lot='{b[6:10]}')"
            for b in chunk
        ]
        where = " OR ".join(clauses)
        rows = _socrata_fetch(ECB_URL, where, ECB_SELECT)
        all_rows.extend(rows)
        if i % 15 == 0:
            print(f"  chunk {i+1}/{(len(bbls_sorted)+chunk_size-1)//chunk_size}: {len(all_rows)} so far")
    print(f"  raw: {len(all_rows)}")

    # Filter: active + uncured
    active = [
        r for r in all_rows
        if r.get("ecb_violation_status") == "ACTIVE"
        and r.get("certification_status") in ECB_BAD_CERT
    ]
    print(f"  active + uncured: {len(active)}")

    # Classify
    by_bbl = {}
    dropped = 0
    for r in active:
        cls = _classify_ecb(r)
        if not cls:
            dropped += 1
            continue
        bbl = _normalize_boro_block_lot_to_bbl(r.get("boro"), r.get("block"), r.get("lot"))
        trimmed = {
            "class": cls,
            "severity": r.get("severity"),
            "category": r.get("violation_type"),
            "date": (r.get("issue_date") or "")[:10],
            "ecb_number": r.get("ecb_violation_number"),
            "desc_snippet": (r.get("violation_description") or "")[:180],
        }
        by_bbl.setdefault(bbl, []).append(trimmed)
    kept = sum(len(v) for v in by_bbl.values())
    print(f"  fire-relevant kept: {kept} across {len(by_bbl)} garages  (dropped {dropped} non-fire)")
    return by_bbl


# ---------------------------------------------------------------------------
# DOB NOW Safety Violations (855j-jady) — LL126 parking structure + fire systems
# ---------------------------------------------------------------------------

DOB_NOW_URL = "https://data.cityofnewyork.us/resource/855j-jady.json"
DOB_NOW_SELECT = (
    "bbl,violation_issue_date,violation_number,violation_type,"
    "violation_remarks,violation_status,device_type,house_number,street"
)
DOB_NOW_RELEVANT_DEVICES = {
    "Parking Structures",
    "Sprinklers",
    "Emergency Power",
    "Photoluminescent",
    "Structurally Compromised Buildings",
}


def _classify_dob_now(r):
    """Return 'ps_unsafe' | 'ps_initl' | 'sprinkler' | 'emergency_power' |
    'photoluminescent' | 'structurally_compromised' | None"""
    dev = r.get("device_type") or ""
    vtype = (r.get("violation_type") or "").upper()
    if dev == "Parking Structures":
        if "UNSAFE" in vtype:
            return "ps_unsafe"
        if "INITL" in vtype:
            return "ps_initl"
        return "ps_other"
    if dev == "Sprinklers":
        return "sprinkler"
    if dev == "Emergency Power":
        return "emergency_power"
    if dev == "Photoluminescent":
        return "photoluminescent"
    if dev == "Structurally Compromised Buildings":
        return "structurally_compromised"
    return None


def pull_dob_now(garage_bbls, chunk_size=80):
    """Pull active DOB NOW records on our garages. 855j-jady has a bbl field
    so we can use bbl IN (...) — simpler than boro/block/lot composite."""
    print(f"Pulling DOB NOW (855j-jady) for {len(garage_bbls)} garages...")
    bbls_sorted = sorted(garage_bbls)
    all_rows = []
    for i, chunk in enumerate(_chunk(bbls_sorted, chunk_size)):
        in_clause = ",".join(f"'{b}'" for b in chunk)
        where = f"bbl IN ({in_clause})"
        rows = _socrata_fetch(DOB_NOW_URL, where, DOB_NOW_SELECT)
        all_rows.extend(rows)
        if i % 15 == 0:
            print(f"  chunk {i+1}/{(len(bbls_sorted)+chunk_size-1)//chunk_size}: {len(all_rows)} so far")
    print(f"  raw: {len(all_rows)}")

    active = [r for r in all_rows if r.get("violation_status") == "Active"
              and (r.get("device_type") or "") in DOB_NOW_RELEVANT_DEVICES]
    print(f"  active + fire-system device: {len(active)}")

    by_bbl = {}
    for r in active:
        cls = _classify_dob_now(r)
        if not cls:
            continue
        bbl = str(r.get("bbl") or "").split(".")[0].zfill(10)
        if bbl not in garage_bbls:
            continue
        trimmed = {
            "class": cls,
            "device_type": r.get("device_type"),
            "violation_type": r.get("violation_type"),
            "date": (r.get("violation_issue_date") or "")[:10],
            "remarks": (r.get("violation_remarks") or "")[:160],
        }
        by_bbl.setdefault(bbl, []).append(trimmed)
    kept = sum(len(v) for v in by_bbl.values())
    print(f"  classified: {kept} records across {len(by_bbl)} garages")
    return by_bbl


# ---------------------------------------------------------------------------
# LL2604 active subset (3h2n-5cm9) — kept because no ECB number, can't migrate
# ---------------------------------------------------------------------------

LEGACY_URL = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"


def pull_ll2604_active(garage_bbls):
    """LL2604 citywide is only ~3K records. Pull all of them and filter locally."""
    print("Pulling LL2604 active records (3h2n-5cm9)...")
    where = "violation_type like '%LL2604%' AND violation_category like 'V-%'"
    select = "boro,block,lot,violation_type,issue_date,violation_category"
    rows = _socrata_fetch(LEGACY_URL, where, select, limit=10000)
    print(f"  raw active LL2604: {len(rows)}")

    by_bbl = {}
    for r in rows:
        bbl = _normalize_boro_block_lot_to_bbl(r.get("boro"), r.get("block"), r.get("lot"))
        if bbl not in garage_bbls:
            continue
        subtype = "sprinkler" if "LL2604S" in (r.get("violation_type") or "") else \
                  "emergency_power" if "LL2604E" in (r.get("violation_type") or "") else \
                  "photoluminescent"
        trimmed = {
            "class": f"ll2604_{subtype}",
            "violation_type": r.get("violation_type","").strip(),
            "date": (r.get("issue_date") or "")[:10],
        }
        by_bbl.setdefault(bbl, []).append(trimmed)
    kept = sum(len(v) for v in by_bbl.values())
    print(f"  kept: {kept} records across {len(by_bbl)} garages")
    return by_bbl


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("PATCH CACHE v1.3 — add DOB ECB + DOB NOW + LL2604 signals")
    print("=" * 60)

    if not CACHE_PATH.exists():
        raise SystemExit("No cached_data.json — run fetch_data.py first.")
    with open(CACHE_PATH) as f:
        cache = json.load(f)

    garage_bbls = {g["bbl"] for g in cache["garages"]}
    print(f"Loaded cache fetched {cache['fetched']}  ({len(garage_bbls)} garages)")

    dob_ecb_map = pull_dob_ecb(garage_bbls)
    dob_now_map = pull_dob_now(garage_bbls)
    ll2604_map = pull_ll2604_active(garage_bbls)

    cache["dob_ecb_map"] = dob_ecb_map
    cache["dob_now_map"] = dob_now_map
    cache["ll2604_map"] = ll2604_map
    cache["v1_3_patched"] = time.strftime("%Y-%m-%d %H:%M:%S")

    # Write atomically
    tmp = CACHE_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(cache, f, indent=2)
    tmp.replace(CACHE_PATH)

    print("\nDone. Cache patched.")
    print(f"  dob_ecb_map:  {len(dob_ecb_map)} garages")
    print(f"  dob_now_map:  {len(dob_now_map)} garages")
    print(f"  ll2604_map:   {len(ll2604_map)} garages")


if __name__ == "__main__":
    main()
