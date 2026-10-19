#!/usr/bin/env python3
"""Smoke-test reducer: sums counts for consecutive equal keys."""
import sys

current, total = None, 0
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    if key != current:
        if current is not None:
            print(f"{current}\t{total}")
        current, total = key, 0
    total += int(value)
if current is not None:
    print(f"{current}\t{total}")
