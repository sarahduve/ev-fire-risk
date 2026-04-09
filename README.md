# NYC Parking Garage EV Fire Risk Map

**[View the live map](https://sarahduve.github.io/ev-fire-risk/)**

An interactive risk assessment of ~1,800 parking garages across New York City, scored by their vulnerability to electric vehicle battery fires in enclosed structures.

## Why this exists

As EVs become more common in cities, a new fire risk is emerging in parking garages: lithium-ion battery fires that burn hotter (~5,000°F vs ~1,500°F for gas cars), last longer (60-90+ minutes vs ~30 minutes), and can reignite days later. Roughly 56% of all documented EV battery fires happen while the vehicle is parked — making parking structures the highest-exposure environment.

NYC has thousands of parking garages, many built decades before EVs existed. Some have added EV charging infrastructure without upgrading fire suppression systems. Others have no sprinkler permits on record at all. This tool maps and scores every commercial parking garage in the city to identify which ones are most vulnerable.

**Important context:** EVs catch fire 20-60x *less frequently* than gas vehicles. The risk per vehicle is very low. But when an EV fire does happen in an enclosed structure — especially an older one without modern fire suppression — the consequences can be severe. This project identifies where that worst-case scenario is most likely to cause damage.

## What it shows

Each garage is scored 0-100 based on:

- **Structural age** — Garages built before 1968 (pre-modern NYC building code) score highest. These structures have had the most time to deteriorate and were designed for lighter vehicles and different fire profiles.
- **Fire suppression status** — Cross-referenced against NYC DOB permit records for sprinkler work. Garages with no sprinkler permits on record, or whose last permit predates 2010, score higher.
- **Safety violations** — DOB violation records for unsafe building designations, structural compromises, immediate emergencies, and sprinkler deficiencies. Violations are tiered by severity.
- **EV charger presence** — Garages with EV chargers concentrate vehicles that are actively charging (higher thermal stress) and are more likely to have EVs parked for extended periods at high state of charge.
- **Below-grade levels** — Underground garages have limited ventilation, harder firefighter access, and greater toxic gas accumulation risk. Identified via PLUTO basement codes and OpenStreetMap.

## Data sources

| Source | What it provides | Update frequency |
|--------|-----------------|-----------------|
| [NYC PLUTO](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks) | Building class, year built, floors, basement, location for all NYC properties | Quarterly |
| [DOB Permit Issuance](https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a) | Historical sprinkler permits (pre-2021) | Ongoing |
| [DOB NOW Build](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4) | Recent sprinkler permits (2021+) | Ongoing |
| [DOB Violations](https://data.cityofnewyork.us/Housing-Development/DOB-Violations/3h2n-5cm9) | Safety violations by property | Ongoing |
| [AFDC Alternative Fuel Stations](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/) | EV charger locations, port counts, facility types | Ongoing |
| [OpenStreetMap](https://www.openstreetmap.org/) | Parking type classification (underground/multi-storey/surface) | Community-maintained |

All data is publicly available. No API keys are required for NYC Open Data (Socrata). The AFDC API requires a free NREL API key.

## Running it yourself

```bash
# Clone
git clone https://github.com/sarahduve/ev-fire-risk.git
cd ev-fire-risk

# Score all garages (~30-60 min, makes ~4,000 API calls to NYC Open Data)
python3 build_all_garages.py

# Build the interactive map
python3 build_map.py

# Open it
open ev_fire_risk_map.html
```

Requires Python 3.10+ with no external dependencies (uses only stdlib).

## Limitations

- **Sprinkler data is incomplete.** DOB digital records start around 1990. A garage with "no sprinkler permits" may have a perfectly functional system installed before records went digital.
- **EV charger locations are approximate.** A charger listed at an address may be inside the garage, in an adjacent lot, or curbside. We use spatial matching (500ft radius) which can misattribute.
- **Underground classification is incomplete.** PLUTO basement codes + OSM cover ~137 underground garages. The true number in Manhattan alone is likely much higher.
- **This is not a structural assessment.** A high score means a garage has risk *factors* — it does not mean the garage is unsafe or will collapse. Only a licensed engineer conducting a physical inspection can make that determination.
- **Scores reflect data availability, not just risk.** A garage with more DOB records will have more data points to score against. Absence of violations may mean a clean record or may mean the building hasn't been inspected.

## License

MIT
