"""Inject risk score data into the map HTML template."""
import json
from pathlib import Path

DATA_DIR = Path(__file__).parent

with open(DATA_DIR / "risk_scores_all.json") as f:
    data = json.load(f)

with open(DATA_DIR / "map.html") as f:
    html = f.read()

html = html.replace("RISK_DATA_PLACEHOLDER", json.dumps(data))

for name in ["ev_fire_risk_map.html", "index.html"]:
    with open(DATA_DIR / name, "w") as f:
        f.write(html)

print(f"Map built: {DATA_DIR / 'ev_fire_risk_map.html'} + index.html")
print(f"  {data['total_scored']} garages plotted")
print(f"  Open in browser: file://{DATA_DIR / 'ev_fire_risk_map.html'}")
