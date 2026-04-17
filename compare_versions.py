"""
Compare two risk_scores_all.json files (e.g., v1.2 vs v1.3).
Report rank churn, biggest score moves, tier crossings.

Usage:
    python3 compare_versions.py OLD.json NEW.json [--out research/version_comparison.md]
"""

import argparse
import json
import sys
from pathlib import Path


def tier(score):
    if score >= 70: return "High"
    if score >= 50: return "Elevated"
    if score >= 30: return "Moderate"
    return "Low"


def load_results(path):
    with open(path) as f:
        d = json.load(f)
    return d["results"], d.get("generated", "unknown")


def build_rank_map(results):
    """results are assumed sorted high-to-low by risk_score.
    Assign rank 1 = worst. Ties share the lower rank (dense)."""
    sorted_r = sorted(results, key=lambda r: -r["risk_score"])
    return {r["bbl"]: i + 1 for i, r in enumerate(sorted_r)}


def compare(old_path, new_path, out_path):
    old_results, old_gen = load_results(old_path)
    new_results, new_gen = load_results(new_path)
    old_by_bbl = {r["bbl"]: r for r in old_results}
    new_by_bbl = {r["bbl"]: r for r in new_results}
    old_rank = build_rank_map(old_results)
    new_rank = build_rank_map(new_results)

    shared = sorted(set(old_by_bbl) & set(new_by_bbl))
    only_old = sorted(set(old_by_bbl) - set(new_by_bbl))
    only_new = sorted(set(new_by_bbl) - set(old_by_bbl))

    # Diffs
    diffs = []
    for bbl in shared:
        o, n = old_by_bbl[bbl], new_by_bbl[bbl]
        diffs.append({
            "bbl": bbl,
            "addr": n.get("address", o.get("address", "")),
            "boro": n.get("borough", o.get("borough", "")),
            "old_score": o["risk_score"],
            "new_score": n["risk_score"],
            "d_score": n["risk_score"] - o["risk_score"],
            "old_rank": old_rank[bbl],
            "new_rank": new_rank[bbl],
            "d_rank": old_rank[bbl] - new_rank[bbl],  # positive = moved up (worse)
            "old_tier": tier(o["risk_score"]),
            "new_tier": tier(n["risk_score"]),
            "old_reasons": o.get("reasons", []),
            "new_reasons": n.get("reasons", []),
        })

    # Top-50 rank churn
    old_top50 = set(b for b, r in old_rank.items() if r <= 50)
    new_top50 = set(b for b, r in new_rank.items() if r <= 50)
    top50_churned_out = old_top50 - new_top50  # were in top 50 old, not in new
    top50_churned_in = new_top50 - old_top50
    top50_stable = old_top50 & new_top50

    # Biggest score jumps
    jumps_up = sorted(diffs, key=lambda d: -d["d_score"])[:20]
    jumps_down = sorted(diffs, key=lambda d: d["d_score"])[:10]

    # Tier crossings (only into worse tier)
    tier_order = {"Low": 0, "Moderate": 1, "Elevated": 2, "High": 3}
    crossings_up = [d for d in diffs if tier_order[d["new_tier"]] > tier_order[d["old_tier"]]]
    crossings_down = [d for d in diffs if tier_order[d["new_tier"]] < tier_order[d["old_tier"]]]

    # Tier distribution
    def distribution(diffs, key):
        d = {"High": 0, "Elevated": 0, "Moderate": 0, "Low": 0}
        for r in diffs:
            d[tier(r[key])] += 1
        return d
    old_dist = distribution(diffs, "old_score")
    new_dist = distribution(diffs, "new_score")

    # Write markdown
    lines = []
    add = lines.append
    add(f"# Version comparison: {Path(old_path).name} → {Path(new_path).name}\n")
    add(f"Generated {old_gen} (old) vs {new_gen} (new)\n")
    add(f"Shared BBLs: {len(shared)}  |  only-old: {len(only_old)}  |  only-new: {len(only_new)}\n")

    add("\n## Tier distribution\n")
    add("| Tier | Old | New | Δ |")
    add("|---|---:|---:|---:|")
    for t in ["High", "Elevated", "Moderate", "Low"]:
        d_t = new_dist[t] - old_dist[t]
        add(f"| {t} | {old_dist[t]} | {new_dist[t]} | {d_t:+d} |")

    add("\n## Tier crossings\n")
    add(f"- **Into a worse tier:** {len(crossings_up)} buildings\n")
    add(f"- **Into a better tier:** {len(crossings_down)} buildings\n")

    add("### Worst crossings (biggest tier jumps up)")
    add("| BBL | Address | Boro | Old→New Tier | Old→New Score |")
    add("|---|---|---|---|---|")
    for d in sorted(crossings_up, key=lambda x: -x["d_score"])[:20]:
        add(f"| {d['bbl']} | {d['addr']} | {d['boro']} | {d['old_tier']}→{d['new_tier']} | {d['old_score']}→{d['new_score']} |")

    add("\n## Top-50 rank churn\n")
    add(f"- **Stable in top-50 (both versions):** {len(top50_stable)}")
    add(f"- **Dropped out of top-50:** {len(top50_churned_out)}")
    add(f"- **New to top-50:** {len(top50_churned_in)}\n")

    add("### New to top-50")
    add("| New rank | Old rank | BBL | Address | Boro | Score (old→new) |")
    add("|---:|---:|---|---|---|---|")
    new_in_top = sorted([d for d in diffs if d["bbl"] in top50_churned_in],
                        key=lambda x: x["new_rank"])
    for d in new_in_top:
        add(f"| {d['new_rank']} | {d['old_rank']} | {d['bbl']} | {d['addr']} | {d['boro']} | {d['old_score']}→{d['new_score']} |")

    add("\n### Dropped out of top-50")
    add("| Old rank | New rank | BBL | Address | Boro | Score (old→new) |")
    add("|---:|---:|---|---|---|---|")
    dropped = sorted([d for d in diffs if d["bbl"] in top50_churned_out],
                     key=lambda x: x["old_rank"])
    for d in dropped:
        add(f"| {d['old_rank']} | {d['new_rank']} | {d['bbl']} | {d['addr']} | {d['boro']} | {d['old_score']}→{d['new_score']} |")

    add("\n## Biggest score jumps\n")
    add("### Up (worse in new version)")
    add("| Δ | Old→New | Old rank→New rank | BBL | Address | Boro |")
    add("|---:|---|---|---|---|---|")
    for d in jumps_up:
        add(f"| {d['d_score']:+d} | {d['old_score']}→{d['new_score']} | {d['old_rank']}→{d['new_rank']} | {d['bbl']} | {d['addr']} | {d['boro']} |")

    add("\n### Down (better in new version)")
    add("| Δ | Old→New | Old rank→New rank | BBL | Address | Boro |")
    add("|---:|---|---|---|---|---|")
    for d in jumps_down:
        add(f"| {d['d_score']:+d} | {d['old_score']}→{d['new_score']} | {d['old_rank']}→{d['new_rank']} | {d['bbl']} | {d['addr']} | {d['boro']} |")

    # Optional: spotlight on 2425 Sedgwick if present
    add("\n## Spotlight: buildings of interest\n")
    of_interest = [("2032360170", "2425 Sedgwick Ave (Bronx) — News12 reported DOB vacate")]
    for bbl, note in of_interest:
        if bbl in shared:
            d = next(x for x in diffs if x["bbl"] == bbl)
            add(f"### {d['addr']} ({d['boro']}) — {note}")
            add(f"- **Score:** {d['old_score']} → {d['new_score']} (Δ{d['d_score']:+d})")
            add(f"- **Tier:** {d['old_tier']} → {d['new_tier']}")
            add(f"- **Rank:** {d['old_rank']} → {d['new_rank']}")
            add("\n**Old reasons:**")
            for r in d["old_reasons"]:
                add(f"- {r}")
            add("\n**New reasons:**")
            for r in d["new_reasons"]:
                add(f"- {r}")
            add("")

    text = "\n".join(lines)
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(text)
        print(f"Wrote {out_path}")
    else:
        print(text)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("old_json")
    p.add_argument("new_json")
    p.add_argument("--out", default=None)
    args = p.parse_args()
    compare(args.old_json, args.new_json, args.out)


if __name__ == "__main__":
    main()
