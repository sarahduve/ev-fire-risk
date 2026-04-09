"""
DEPRECATED: Use fetch_data.py + score_garages.py instead.

This monolithic script has been split into:
  - fetch_data.py: downloads raw data, saves cached_data.json (~30 min)
  - score_garages.py: reads cache, applies scoring (instant)

This file is kept for reference but should not be used directly.
"""
import sys
print('This script is deprecated. Use:')
print('  python3 fetch_data.py     # fetch raw data (slow)')
print('  python3 score_garages.py  # score garages (instant)')
print('  python3 build_map.py      # build map (instant)')
sys.exit(1)

