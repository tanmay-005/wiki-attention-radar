#!/usr/bin/env bash
# Step 2.3: proves sum_reducer.py is a valid combiner (T-M2), plus the other MR pipe tests.
# Runs anywhere with Python 3 + pytest (inside edge: cd /workspace && bash tests/mr/test_combiner.sh)
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 -m pytest tests/mr tests/unit/test_title.py -v
