"""Fetch OSM parking lots/garages for NYC from the Overpass API -> osm_parking.json.

Overpass is a free, volunteer-run service that regularly returns 429/504 under load,
so try each mirror a few times with backoff. If every attempt fails, keep the
previously committed osm_parking.json (it changes slowly) rather than failing the
whole monthly update.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

OUT = "osm_parking.json"
MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]
ATTEMPTS_PER_MIRROR = 2
BBOX = "40.49,-74.26,40.92,-73.70"
QUERY = f"""[out:json][timeout:90];
(node["amenity"="parking"]["parking"~"underground|multi-storey|surface"]({BBOX});
way["amenity"="parking"]["parking"~"underground|multi-storey|surface"]({BBOX});
relation["amenity"="parking"]["parking"~"underground|multi-storey|surface"]({BBOX}););
out center tags;"""
# Overpass rejects the default Python-urllib UA with HTTP 406
HEADERS = {"User-Agent": "ev-fire-risk/1.0 (https://github.com/sarahduve/ev-fire-risk)"}


def fetch(url):
    data = urllib.parse.urlencode({"data": QUERY}).encode()
    req = urllib.request.Request(url, data=data, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=150) as resp:
        return json.loads(resp.read())


def parse(result):
    parsed = []
    for e in result.get("elements", []):
        tags = e.get("tags", {})
        if e["type"] == "node":
            lat, lon = e.get("lat"), e.get("lon")
        else:
            c = e.get("center", {})
            lat, lon = c.get("lat"), c.get("lon")
        if lat and lon:
            parsed.append({"osm_id": e["id"], "parking_type": tags.get("parking", ""),
                           "name": tags.get("name", ""), "lat": lat, "lon": lon})
    return parsed


def main():
    for url in MIRRORS:
        for attempt in range(1, ATTEMPTS_PER_MIRROR + 1):
            try:
                parsed = parse(fetch(url))
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                print(f"{url} attempt {attempt} failed: {e}")
                time.sleep(30 * attempt)
                continue
            if not parsed:
                # An empty result is almost certainly an Overpass error, not real data
                print(f"{url} returned 0 elements; treating as a failure")
                continue
            with open(OUT, "w") as f:
                json.dump({"total": len(parsed), "elements": parsed}, f, indent=2)
            print(f"Fetched {len(parsed)} OSM parking entries from {url}")
            return 0

    if os.path.exists(OUT):
        print(f"::warning::All Overpass mirrors failed; keeping existing {OUT}")
        return 0
    print(f"::error::All Overpass mirrors failed and no existing {OUT} to fall back on")
    return 1


if __name__ == "__main__":
    sys.exit(main())
