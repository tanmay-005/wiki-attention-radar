#!/usr/bin/env bash
# Run a Spark job on YARN instead of local mode (step 4.15).
# Runs INSIDE edge:  bash scripts/spark_yarn.sh spark/jobs/s2_spikes_global.py [args...]
#
# The driver stays in edge (client mode), so the Spark UI is still http://localhost:4040.
# Executors run in the NodeManager container, which does not have /workspace mounted,
# so the project's Python packages are shipped as a zip with --py-files.
# Sized for the 4 GB NodeManager: 2 executors x (1 GB + overhead) + a 512 MB YARN AM.
set -euo pipefail
cd /workspace
ZIP=/tmp/radar_py.zip
rm -f "$ZIP"
python3 - <<'PY'
import zipfile, pathlib
with zipfile.ZipFile("/tmp/radar_py.zip", "w") as z:
    for top in ("spark", "enrichment"):
        for p in pathlib.Path(top).rglob("*.py"):
            z.write(p)
    z.write("mapreduce/common_title.py", "common_title.py")
PY
exec spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 1 --executor-memory 1g --driver-memory 2g \
  --conf spark.yarn.am.memory=512m \
  --conf spark.pyspark.python=python3 \
  --conf spark.sql.session.timeZone=UTC \
  --py-files "$ZIP" "$@"
