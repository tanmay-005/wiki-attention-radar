#!/usr/bin/env python3
"""Smoke-test mapper: emits <word>\t1 for each input line."""
import sys

for line in sys.stdin:
    word = line.strip()
    if word:
        print(f"{word}\t1")
