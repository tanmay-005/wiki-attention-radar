#!/usr/bin/env python3
"""Sum reducer, also used as the combiner (summing is associative and commutative).

Input lines are sorted by key. The key is everything before the LAST tab and the
value is the number after it, so the same script works for MR1 (4 key fields)
and MR2 (3 key fields). Output keeps the same key/value format, which is what a
combiner must do.
"""
import sys


def main() -> None:
    current, total, bad = None, 0, 0
    out = sys.stdout
    for line in sys.stdin:
        line = line.rstrip("\n")
        key, sep, value = line.rpartition("\t")
        if not sep or not value.isdigit():
            bad += 1
            continue
        if key != current:
            if current is not None:
                out.write(f"{current}\t{total}\n")
            current, total = key, 0
        total += int(value)
    if current is not None:
        out.write(f"{current}\t{total}\n")
    if bad:
        sys.stderr.write(f"reporter:counter:Radar,reducer_bad_lines,{bad}\n")


if __name__ == "__main__":
    main()
