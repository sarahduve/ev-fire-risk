"""Patch risk_scores_all.json with PIPS (Periodic Inspection of Parking
Structures, Local Law 126) filing status, joined on BBL.

Source: NYC DOB self-publishes the PIPS dataset on GitHub Pages —
https://raw.githubusercontent.com/NYCDOB/ParkingStructures/gh-pages/data/ParkingStructureInspections_statusPending.csv
(not on Open Data / Socrata).

This is richer than the dob_now_ps_status field already present (which is
derived from DOB NOW Safety Violations device_type='Parking Structures' —
that captures only UNSAFE and INITL via the violations channel). PIPS has
the full universe DOB considers parking structures and the full status
set: SAFE / SREM / UNSAFE / NO REPORT FILED / STATUS PENDING.

Adds the following per-BBL fields:
  pips_in_universe (bool)        — True if BBL appears in PIPS
  pips_filing_status (str)       — SAFE/SREM/UNSAFE/NO_REPORT_FILED/STATUS_PENDING/''
  pips_unsafe (bool)             — Filing Status == UNSAFE
  pips_no_report_filed (bool)    — Filing Status == NO REPORT FILED
  pips_srem (bool)               — Filing Status == SREM
  pips_inspection_date (str)     — Inspection Date (YYYY-MM-DD or '')
  pips_sub_cycle (str)           — '1A' / '1B' / '1C' (geographic sub-cycle)
  pips_fisp (bool)               — also subject to Local Law 11 facade cycle
  pips_city_owned (bool)         — Y in PIPS City Owned column
  pips_structure_id (str)        — DOB-assigned structure ID (for deep links)

Also writes pips_audit.json with: pips_only_bbls (PIPS records not in our
universe — our coverage gaps) and ours_only_bbls (our records not in PIPS
— buildings DOB doesn't consider parking structures).

Runs in CI between enrich_ev_permits.py and tests.py.
"""
import csv
import io
import json
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent
PIPS_CSV_URL = (
    "https://raw.githubusercontent.com/NYCDOB/ParkingStructures/"
    "gh-pages/data/ParkingStructureInspections_statusPending.csv"
)

BORO_TO_DIGIT = {
    "MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3",
    "QUEENS": "4", "STATEN ISLAND": "5",
}


def _build_bbl(borough_text, block, lot):
    digit = BORO_TO_DIGIT.get((borough_text or "").strip().upper())
    if not digit:
        return None
    try:
        b, l = int(block), int(lot)
    except (ValueError, TypeError):
        return None
    return digit + str(b).zfill(5) + str(l).zfill(4)


def _parse_status(s):
    """Map raw Filing Status to canonical values."""
    s = (s or "").strip().upper()
    if s == "NO REPORT FILED":
        return "NO_REPORT_FILED"
    if s == "STATUS PENDING":
        return "STATUS_PENDING"
    if s in {"SAFE", "SREM", "UNSAFE"}:
        return s
    return ""


def _normalize_date(d):
    """PIPS dates arrive as M/D/YYYY. Convert to YYYY-MM-DD."""
    d = (d or "").strip()
    if not d:
        return ""
    try:
        m, day, y = d.split("/")
        return f"{y}-{int(m):02d}-{int(day):02d}"
    except (ValueError, AttributeError):
        return ""


def fetch_pips():
    print(f"Fetching PIPS CSV from {PIPS_CSV_URL}")
    req = urllib.request.Request(
        PIPS_CSV_URL, headers={"User-Agent": "ev-fire-risk-enrich"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        text = resp.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    print(f"  {len(rows)} raw PIPS rows")

    # Build BBL -> "best" record. When the same BBL has multiple filings
    # (e.g., one per sub-structure or amended report), keep the one with
    # the worst status. UNSAFE > SREM > NO_REPORT_FILED > STATUS_PENDING > SAFE.
    severity = {
        "UNSAFE": 5, "SREM": 4, "NO_REPORT_FILED": 3,
        "STATUS_PENDING": 2, "SAFE": 1, "": 0,
    }
    by_bbl = {}
    skipped = 0
    for r in rows:
        bbl = _build_bbl(r.get("Borough"), r.get("Block"), r.get("Lot"))
        if not bbl:
            skipped += 1
            continue
        status = _parse_status(r.get("Filing Status"))
        existing = by_bbl.get(bbl)
        if existing is None or severity[status] > severity[existing["_status"]]:
            by_bbl[bbl] = {
                "_status": status,
                "raw": r,
            }
    print(f"  {len(by_bbl)} unique BBLs (skipped {skipped} unparseable)")
    return by_bbl


def enrich(pips_by_bbl):
    path = DATA_DIR / "risk_scores_all.json"
    data = json.load(open(path))
    scored = data["results"]

    flagged_unsafe = 0
    flagged_no_report = 0
    flagged_srem = 0
    in_universe_count = 0
    matched_bbls = set()

    for r in scored:
        bbl = r["bbl"]
        pips = pips_by_bbl.get(bbl)
        if not pips:
            r["pips_in_universe"] = False
            r["pips_filing_status"] = ""
            r["pips_unsafe"] = False
            r["pips_no_report_filed"] = False
            r["pips_srem"] = False
            r["pips_inspection_date"] = ""
            r["pips_sub_cycle"] = ""
            r["pips_fisp"] = False
            r["pips_city_owned"] = False
            r["pips_structure_id"] = ""
            continue

        matched_bbls.add(bbl)
        in_universe_count += 1
        status = pips["_status"]
        raw = pips["raw"]

        r["pips_in_universe"] = True
        r["pips_filing_status"] = status
        r["pips_unsafe"] = status == "UNSAFE"
        r["pips_no_report_filed"] = status == "NO_REPORT_FILED"
        r["pips_srem"] = status == "SREM"
        r["pips_inspection_date"] = _normalize_date(raw.get("Inspection Date"))
        r["pips_sub_cycle"] = (raw.get("PIPS Sub-Cycle") or "").strip()
        r["pips_fisp"] = (raw.get("FISP") or "").strip().upper() == "Y"
        r["pips_city_owned"] = (raw.get("City Owned") or "").strip().upper() == "Y"
        r["pips_structure_id"] = (raw.get("Parking Structure ID") or "").strip()

        if r["pips_unsafe"]:
            flagged_unsafe += 1
        if r["pips_no_report_filed"]:
            flagged_no_report += 1
        if r["pips_srem"]:
            flagged_srem += 1

    print(f"PIPS join: {in_universe_count}/{len(scored)} of our records have a PIPS filing")
    print(f"  UNSAFE: {flagged_unsafe} | SREM: {flagged_srem} | NO_REPORT_FILED: {flagged_no_report}")

    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {path}")

    # Audit: PIPS BBLs we don't have, and ours that aren't in PIPS
    pips_only = sorted(set(pips_by_bbl.keys()) - matched_bbls)
    ours_only = sorted({r["bbl"] for r in scored} - matched_bbls)

    audit = {
        "metadata": {
            "pips_total_unique_bbls": len(pips_by_bbl),
            "ours_total": len(scored),
            "matched": len(matched_bbls),
            "pips_only_count": len(pips_only),
            "ours_only_count": len(ours_only),
        },
        "status_distribution_in_pips_only": dict(Counter(
            pips_by_bbl[b]["_status"] for b in pips_only
        )),
        "dof_class_distribution_in_pips_only": dict(Counter(
            (pips_by_bbl[b]["raw"].get("DOF Bldg Classification Description") or "").strip()
            for b in pips_only
        ).most_common(20)),
        "pips_only_bbls": [
            {
                "bbl": b,
                "address": pips_by_bbl[b]["raw"].get("Address"),
                "borough": pips_by_bbl[b]["raw"].get("Borough"),
                "filing_status": pips_by_bbl[b]["_status"],
                "dof_class": pips_by_bbl[b]["raw"].get("DOF Bldg Classification Description"),
                "structure_id": pips_by_bbl[b]["raw"].get("Parking Structure ID"),
            } for b in pips_only
        ],
        "ours_only_bbls": [
            {
                "bbl": b,
                "address": next((r["address"] for r in scored if r["bbl"] == b), ""),
                "bldgclass": next((r["bldgclass"] for r in scored if r["bbl"] == b), ""),
                "garage_type": next((r["garage_type"] for r in scored if r["bbl"] == b), ""),
                "risk_score": next((r["risk_score"] for r in scored if r["bbl"] == b), 0),
            } for b in ours_only
        ],
    }
    audit_path = DATA_DIR / "pips_audit.json"
    with open(audit_path, "w") as f:
        json.dump(audit, f, indent=2)
    print(f"Wrote {audit_path}")


def main():
    pips_by_bbl = fetch_pips()
    enrich(pips_by_bbl)


if __name__ == "__main__":
    main()
