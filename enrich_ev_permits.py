"""Patch risk_scores_all.json with pending_ev_permit flags based on DOB NOW
Electrical Permit Applications (dm9a-ab7w) that mention EV chargers.

A BBL is flagged `pending_ev_permit=True` when:
  - At least one electrical permit on that BBL has a keyword-matching
    job_description (EV CHARG, EVSE, CHARGING STATION, ELECTRIC VEHICLE CHARG)
  - The latest such permit was filed in 2025 or later (older permits whose
    installs never appeared in AFDC are more likely canceled or completed-
    but-private, not "pending")
  - The BBL does NOT have an AFDC existing charger (has_chargers=False in
    the scored record — we only want forward-looking signal here, not
    retroactive audit)

Runs in the CI pipeline between score_garages.py and build_map.py.

Raw permit records are written to dob_ev_permits.json for transparency.
Deeper audit (existing chargers WITH/WITHOUT permit paper trail, plus
markdown summary) still lives in research/fetch_ev_permits.py.
"""
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).parent
DOB_NOW = "https://data.cityofnewyork.us/resource/dm9a-ab7w.json"

# Tier 1 — keywords unambiguously referring to EV (car) charging. Accept
# any description matching one of these.
EV_SPECIFIC_KEYWORDS = (
    "EV CHARGER", "EV CHARGING", "EV CHARGE", "EV STATION",
    "EVSE", "ELECTRIC VEHICLE CHARG", "E.V. CHARG",
)
# Tier 2 — generic "charging station" phrasing. Accept only if the
# description does NOT also mention bike/ebike/micromobility. Without this
# gate the generic phrasing swept up JOCO / LYFT / CitiBike e-bike dock
# permits filed inside parking garages, inflating the pending-install flag
# with a different category of vehicle.
EV_GENERIC_KEYWORDS = ("CHARGING STATION", "CHARGING STATIONS")
BIKE_NEGATIVE_KEYWORDS = (
    "JOCO", "LYFT", "CITIBIKE", "CITI BIKE",
    "BIKE", "BICYCLE", "EBIKE", "E-BIKE", "E BIKE",
    "DOCKLESS", "MICROMOBIL",
)
RECENT_CUTOFF = "2025"  # only flag BBLs whose latest permit is 2025+


def _matches_ev(text):
    if not text:
        return None
    up = text.upper()
    for kw in EV_SPECIFIC_KEYWORDS:
        if kw in up:
            return kw
    for kw in EV_GENERIC_KEYWORDS:
        if kw in up:
            if any(bk in up for bk in BIKE_NEGATIVE_KEYWORDS):
                return None
            return kw
    return None


def _fetch_paginated(url, where, limit=50000):
    out = []
    offset = 0
    while True:
        qs = urllib.parse.urlencode({
            "$where": where, "$limit": limit, "$offset": offset,
        })
        req = urllib.request.Request(
            f"{url}?{qs}", headers={"User-Agent": "ev-fire-risk-enrich"}
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            chunk = json.loads(resp.read())
        out.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return out


def pull_permits():
    print("Pulling DOB NOW Electrical EV-keyword permits (dm9a-ab7w)...")
    # Cast the net wide in SoQL (union of specific + generic keywords), then
    # apply the bike-negative filter locally in _matches_ev. Simpler than
    # replicating a NOT-LIKE chain in SoQL.
    where = "(" + " OR ".join(
        f"upper(job_description) like '%{kw}%'"
        for kw in EV_SPECIFIC_KEYWORDS + EV_GENERIC_KEYWORDS
    ) + ")"
    rows = _fetch_paginated(DOB_NOW, where)
    kept = []
    for r in rows:
        kw = _matches_ev(r.get("job_description"))
        bbl = str(r.get("gis_bbl") or "").strip()
        if not kw or len(bbl) != 10 or not bbl.isdigit():
            continue
        kept.append({
            "bbl": bbl,
            "filing_date": (r.get("filing_date") or "")[:10],
            "filing_status": r.get("filing_status"),
            "job_status": r.get("job_status"),
            "filing_number": r.get("filing_number"),
            "keyword": kw,
            "job_description": (r.get("job_description") or "")[:300],
        })
    print(f"  {len(rows)} raw, {len(kept)} kept")
    # Persist for transparency / downstream use
    with open(DATA_DIR / "dob_ev_permits.json", "w") as f:
        json.dump(kept, f, indent=2)
    return kept


def enrich(permits):
    path = DATA_DIR / "risk_scores_all.json"
    data = json.load(open(path))
    scored = data["results"]
    afdc_bbls = {r["bbl"] for r in scored if r.get("has_chargers")}

    # Group permits by BBL, track latest date + count
    from collections import defaultdict
    by_bbl = defaultdict(list)
    for p in permits:
        by_bbl[p["bbl"]].append(p)

    flagged = 0
    for r in scored:
        bbl = r["bbl"]
        if bbl in afdc_bbls:
            # Already has AFDC existing charger — not "pending" from our lens
            r["pending_ev_permit"] = False
            continue
        perms = by_bbl.get(bbl, [])
        if not perms:
            r["pending_ev_permit"] = False
            continue
        latest = max((p.get("filing_date") or "" for p in perms), default="")
        if not latest or latest[:4] < RECENT_CUTOFF:
            r["pending_ev_permit"] = False
            continue
        r["pending_ev_permit"] = True
        r["pending_ev_permit_count"] = len(perms)
        r["pending_ev_permit_latest"] = latest
        flagged += 1

    # Always ensure these fields exist even when False so downstream JS
    # doesn't need undefined checks
    for r in scored:
        r.setdefault("pending_ev_permit_count", 0)
        r.setdefault("pending_ev_permit_latest", "")

    print(f"Flagged {flagged} BBLs with pending_ev_permit=True "
          f"(latest permit {RECENT_CUTOFF}+, no AFDC existing charger)")

    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {path}")


def main():
    permits = pull_permits()
    enrich(permits)


if __name__ == "__main__":
    main()
