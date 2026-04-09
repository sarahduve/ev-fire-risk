# NYC Parking Garage EV Fire Risk Map

**[View the live map](https://sarahduve.github.io/ev-fire-risk/)** | **[Methodology & sources](https://sarahduve.github.io/ev-fire-risk/methodology.html)**

An interactive risk assessment of 1,907 parking garages across New York City, scored by their vulnerability to electric vehicle battery fires in enclosed structures.

## Why this exists

As EVs become more common in cities, a new fire risk is emerging in parking garages: lithium-ion battery fires that burn hotter (~5,000°F vs ~1,500°F for gas cars), last longer (60-90+ minutes vs ~30 minutes), and can reignite days later. Roughly 56% of all documented EV battery fires happen while the vehicle is parked — making parking structures the highest-exposure environment.

NYC has thousands of parking garages, many built decades before EVs existed. Some have added EV charging infrastructure without upgrading fire suppression systems. Others have no evidence of fire suppression maintenance in over 35 years of DOB records. This tool maps and scores every commercial parking garage in the city to identify which ones are most vulnerable.

**Important context:** EVs catch fire [20-60x less frequently](https://theicct.org/clearing-the-air-evs-could-bring-lower-fire-risk-oct24/) than gas vehicles. The risk per vehicle is very low. But when an EV fire does happen in an enclosed structure — especially an older one without modern fire suppression — the consequences can be severe. The [August 2024 Incheon, South Korea incident](https://fortune.com/asia/2024/08/07/exploding-mercedes-benz-ev-parking-garage-bans-south-korea/) (140 vehicles destroyed, 23 hospitalized, 8 hours to extinguish) demonstrated this.

## What it shows

1,907 garages scored 0-100 based on five factors:

- **Structural age (0-30 pts)** — Garages built before 1968 (pre-modern NYC building code) score highest.
- **Fire suppression maintenance (0-25 pts)** — Cross-referenced against DOB permit records for sprinkler work. Garages with no maintenance on record, or whose last permit predates the [2022 NFPA density increase](https://nfsa.org/2024/04/30/fire-protection-for-parking-garages/), score higher.
- **Safety violations (0-20 pts)** — DOB violations for unsafe buildings, structural compromises, immediate emergencies, and sprinkler deficiencies. Tiered by severity.
- **EV charger presence (0-15 pts)** — 257 garages with chargers (271 stations from [AFDC](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/)). Chargers concentrate vehicles at high state of charge. DC fast chargers weighted 3x.
- **Multi-story structure (0-10 pts)** — More floors = harder evacuation, heat rises. 156 underground garages identified via PLUTO basement codes + OpenStreetMap.

See the [full methodology](https://sarahduve.github.io/ev-fire-risk/methodology.html) for scoring details, data source documentation, and limitations.

## Data sources

| Source | What it provides |
|--------|-----------------|
| [NYC PLUTO](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks) | Building class, year built, floors, basement, location (857K records bulk downloaded) |
| [DOB Permit Issuance](https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a) | Historical sprinkler permits (pre-2021) |
| [DOB NOW Build](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4) | Recent sprinkler permits (2021+) |
| [DOB Violations](https://data.cityofnewyork.us/Housing-Development/DOB-Violations/3h2n-5cm9) | Safety violations by property |
| [AFDC Alt Fuel Stations](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/) | EV charger locations, port counts, facility types |
| [OpenStreetMap](https://www.openstreetmap.org/) via [Overpass API](https://overpass-api.de/) | Parking type classification (underground/multi-storey/surface) |

All NYC Open Data is free and keyless (Socrata API). AFDC requires a free [NREL API key](https://developer.nrel.gov/signup/).

## Running it yourself

```bash
git clone https://github.com/sarahduve/ev-fire-risk.git
cd ev-fire-risk

# Full pipeline: downloads PLUTO bulk data, queries DOB, matches chargers
# Takes ~30 min (DOB API calls are the bottleneck)
export NREL_API_KEY="your-key-here"
python3 build_all_garages.py

# Build the interactive map
python3 build_map.py

open ev_fire_risk_map.html
```

Requires Python 3.10+ with no external dependencies (stdlib only).

## Key limitations

- **Fire suppression data measures maintenance, not capability.** DOB digital records start ~1990. "No permits on record" may mean no system, or a system with no recorded maintenance in 35+ years. Either way, it almost certainly doesn't meet the [2022 NFPA OH2 standard](https://nfsa.org/2024/04/30/fire-protection-for-parking-garages/) (33% higher sprinkler density). See [methodology](https://sarahduve.github.io/ev-fire-risk/methodology.html#fire-suppression-maintenance-0-25-points) for details.
- **Charger data is a lower bound.** AFDC is voluntary and self-reported. Our 271 garage stations likely undercount the true number.
- **Underground classification is incomplete.** PLUTO basement codes + OSM identify 156 underground garages. The true number is higher. [INRIX](https://inrix.com/products/parking-data-software/) has better data but requires enterprise licensing.
- **This is not a structural assessment.** A high score means risk *factors* are present — it does not mean a fire will occur or that the garage is unsafe.

## License

MIT
