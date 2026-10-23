#!/usr/bin/env python3
"""Pull the numbers we compare out of a saved `mapred streaming` log and append a CSV row.

    python3 benchmarks/job_metrics.py <log> <csv> <label>
"""
import csv
import re
import sys
from pathlib import Path

FIELDS = {
    "map_output_records": "Map output records",
    "map_output_bytes": "Map output bytes",
    "map_output_materialized_bytes": "Map output materialized bytes",
    "combine_input_records": "Combine input records",
    "combine_output_records": "Combine output records",
    "reduce_input_records": "Reduce input records",
    "reduce_shuffle_bytes": "Reduce shuffle bytes",
    "spilled_records": "Spilled Records",
    "cpu_ms": "CPU time spent (ms)",
    "hdfs_bytes_written": "HDFS: Number of bytes written",
    "kept_rows": "kept",
}


def grab(text, name):
    m = re.findall(rf"^\s*{re.escape(name)}=(\d+)\s*$", text, re.M)
    return int(m[-1]) if m else ""


def main():
    log, out, label = sys.argv[1:4]
    text = Path(log).read_text(errors="replace")
    row = {"run": label}
    for k, name in FIELDS.items():
        row[k] = grab(text, name)
    row["wall_seconds"] = grab(text, "WALL_SECONDS")
    row["succeeded"] = "completed successfully" in text
    new = not Path(out).exists()
    with open(out, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)
    print(", ".join(f"{k}={v}" for k, v in row.items()))


if __name__ == "__main__":
    main()
