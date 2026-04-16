# NYC Parking Garage EV Fire Risk Map

**[View the live map](https://sarahduve.github.io/ev-fire-risk/)** | **[Methodology & sources](https://sarahduve.github.io/ev-fire-risk/methodology.html)**

An interactive risk assessment of ~6,200 parking buildings across New York City, scored by their vulnerability to electric vehicle battery fires in enclosed structures. Covers both standalone parking garages and residential / office / institutional buildings with dedicated parking space.

## Why this exists

As EVs become more common in cities, a new fire risk is emerging in parking garages: lithium-ion battery fires that burn hotter (~5,000°F vs ~1,500°F for gas cars), last longer (60-90+ minutes vs ~30 minutes), and can reignite days later. Roughly 56% of all documented EV battery fires happen while the vehicle is parked — making parking structures the highest-exposure environment.

NYC has thousands of parking garages, many built decades before EVs existed. Some have added EV charging infrastructure without upgrading fire suppression systems. Others have no evidence of fire suppression maintenance in over 35 years of DOB records. This tool maps and scores every commercial parking garage in the city to identify which ones are most vulnerable.

**Important context:** EVs catch fire [20-60x less frequently](https://theicct.org/clearing-the-air-evs-could-bring-lower-fire-risk-oct24/) than gas vehicles. The risk per vehicle is very low. But when an EV fire does happen in an enclosed structure — especially an older one without modern fire suppression — the consequences can be severe. The [August 2024 Incheon, South Korea incident](https://fortune.com/asia/2024/08/07/exploding-mercedes-benz-ev-parking-garage-bans-south-korea/) (140 vehicles destroyed, 23 hospitalized, 8 hours to extinguish) demonstrated this.

## What it shows

~6,200 buildings scored 0-100 based on six factors:

- **Structural age (0-30 pts)** — Buildings built before 1968 (pre-modern NYC building code) score highest.
- **Sprinkler system evidence (0-30 pts)** — Cross-references DOB sprinkler permits with FDNY fire-protection violations to determine whether a system exists. Buildings required by Local Law 26 or Local Law 16 to have sprinklers with no evidence of compliance score highest.
- **DOB safety violations (0-20 pts)** — DOB violations for unsafe buildings, structural compromises, immediate emergencies, and sprinkler deficiencies. Tiered by severity.
- **FDNY fire protection compliance (0-25 pts)** — Open (unresolved) FDNY fire-protection violations from OATH/ECB hearings. Time-weighted: older open violations indicate persistent non-compliance.
- **EV charger presence (0-15 pts)** — 250+ buildings with chargers (AFDC stations). Chargers concentrate vehicles at high state of charge. DC fast chargers weighted 3x.
- **Multi-story structure (0-10 pts)** — More floors = harder evacuation, heat rises. Underground garages identified via PLUTO basement codes + OpenStreetMap.

See the [full methodology](https://sarahduve.github.io/ev-fire-risk/methodology.html) for scoring details, data source documentation, and limitations.

## Scoring history

- **v1 (2026-04)** — Building selection rebuilt around PLUTO's `garagearea` field. Previous versions scored standalone G-class garages plus any non-G building with an EV charger match, which made the non-G subset definitionally "has EV charger." v1 scores every NYC building with `garagearea >= 1000 sqft` regardless of EV presence, catching the ~5,000 under-apartment / under-office / under-hospital garages that PLUTO classifies by the building above. Charger-to-building matching rewritten around NYC Planning Labs Geosearch (PAD-backed address lookup) with ArcGIS MapPLUTO point-in-polygon as fallback. Sprinkler factor rebuilt as evidence-based (DOB permits + FDNY confirmation + retrofit mandate flags). New FDNY fire-protection compliance factor added from OATH/ECB hearings data (time-weighted open violations).

## Data sources

| Source | What it provides |
|--------|-----------------|
| [NYC PLUTO](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks) | Building class, year built, floors, basement, location (857K records bulk downloaded) |
| [DOB Permit Issuance](https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a) | Historical sprinkler permits (pre-2021) |
| [DOB NOW Build](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4) | Recent sprinkler permits (2021+) |
| [DOB Violations](https://data.cityofnewyork.us/Housing-Development/DOB-Violations/3h2n-5cm9) | DOB safety violations by property |
| [OATH/ECB Hearings](https://data.cityofnewyork.us/City-Government/OATH-Hearings-Division-Case-Status/jz4z-kudi) | FDNY fire-protection violations (sprinkler maintenance, inspection/testing failures, compliance status) |
| [AFDC Alt Fuel Stations](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/) | EV charger locations, port counts, facility types |
| [OpenStreetMap](https://www.openstreetmap.org/) via [Overpass API](https://overpass-api.de/) | Parking type classification (underground/multi-storey/surface) |

v1 also uses [NYC Planning Labs Geosearch](https://geosearch.planninglabs.nyc/) (PAD-backed) for charger address resolution and the [NYC ArcGIS MapPLUTO](https://a841-dotweb01.nyc.gov/arcgis/rest/services/GAZETTEER/MapPLUTO/MapServer/0) endpoint for spatial fallback. Both are free and keyless.

All NYC Open Data is free and keyless (Socrata API). AFDC requires a free [NREL API key](https://developer.nrel.gov/signup/).

## Running it yourself

```bash
git clone https://github.com/sarahduve/ev-fire-risk.git
cd ev-fire-risk
pip install -r requirements.txt

# Step 1: Fetch raw data (~10 min)
# Downloads PLUTO bulk data, queries DOB permits/violations, resolves chargers
# via PAD. Saves to cached_data.json so you don't have to re-fetch for scoring.
export NREL_API_KEY="your-key-here"
python3 fetch_data.py

# Step 2: Score garages (instant — reads from cache)
# Re-run freely when tweaking scoring weights, labels, or formula
python3 score_garages.py

# Step 3: Build the interactive map (instant)
python3 build_map.py

open ev_fire_risk_map.html
```

Requires Python 3.10+ and `shapely` (for point-in-polygon matching fallback).

The fetch/score separation means you only re-run the slow API step when data is stale. Scoring and map changes are instant.

## Key limitations

- **Fire suppression data measures maintenance, not capability.** DOB digital records start ~1990. "No permits on record" may mean no system, or a system with no recorded maintenance in 35+ years. Either way, it almost certainly doesn't meet the [2022 NFPA OH2 standard](https://nfsa.org/2024/04/30/fire-protection-for-parking-garages/) (33% higher sprinkler density). See [methodology](https://sarahduve.github.io/ev-fire-risk/methodology.html#fire-suppression-maintenance-0-25-points) for details.
- **Scoring formula is calibrated for parking-dominant buildings but applied uniformly.** An old apartment building with 5,000 sqft of ground-floor parking gets scored the same way as a standalone parking structure. The "no sprinkler permit on record" penalty (+25) is particularly likely to trigger for older non-G-class buildings where permit history may relate to non-parking portions. Interpret non-G-class scores with this caveat.
- **Charger data is a lower bound.** AFDC is voluntary and self-reported. Listed garage stations undercount the true number.
- **Underground classification is incomplete.** PLUTO basement codes + OSM identify ~200 underground garages. The true number is higher. [INRIX](https://inrix.com/products/parking-data-software/) has better data but requires enterprise licensing.
- **`garagearea` is self-reported on PLUTO.** Some small ground-floor garages may be under-reported; some large ones may conflate parking with storage/service space.
- **This is not a structural assessment.** A high score means risk *factors* are present — it does not mean a fire will occur or that the building is unsafe.

## License

MIT
