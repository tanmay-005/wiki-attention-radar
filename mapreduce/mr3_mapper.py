#!/usr/bin/env python3
"""MR3 mapper (top-N): daily rows (MR2 output) -> sort keys.

Input : <lang>\t<title>\t<day>\t<views>
Output: <lang>\t<day>\t<views>\t<title>     (key = lang, day, views; value = title)

The job sorts keys with KeyFieldBasedComparator "-k1,1 -k2,2 -k3,3nr", so each
reducer sees one (lang, day) group at a time with the most-viewed titles first.
"""
import os
import sys


def main() -> None:
    min_views = int(os.environ.get("TOPN_MIN_VIEWS", "100"))  # drop the long tail early
    bad = 0
    out = sys.stdout
    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 4 or not parts[3].isdigit():
            bad += 1
            continue
        lang, title, day, views = parts
        if int(views) < min_views:
            continue
        out.write(f"{lang}\t{day}\t{views}\t{title}\n")
    if bad:
        sys.stderr.write(f"reporter:counter:Radar,bad_lines,{bad}\n")


if __name__ == "__main__":
    main()
