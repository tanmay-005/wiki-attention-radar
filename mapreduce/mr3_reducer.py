#!/usr/bin/env python3
"""MR3 reducer (top-N): keep the first N rows of every (lang, day) group.

Input arrives sorted by lang, day, then views descending (secondary sort done by
Hadoop, not here). Output: <lang>\t<day>\t<rank>\t<views>\t<title>
"""
import os
import sys


def main() -> None:
    n = int(os.environ.get("TOPN", "100"))
    group, rank = None, 0
    out = sys.stdout
    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 4:
            continue
        lang, day, views, title = parts
        g = (lang, day)
        if g != group:
            group, rank = g, 0
        rank += 1
        if rank <= n:
            out.write(f"{lang}\t{day}\t{rank}\t{views}\t{title}\n")


if __name__ == "__main__":
    main()
