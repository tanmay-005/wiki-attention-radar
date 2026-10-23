#!/usr/bin/env python3
"""MR2 mapper: hourly rows (MR1 output) -> daily rows.

Input : <lang>\t<platform>\t<title>\t<hour YYYY-MM-DDTHH>\t<views>
Output: <lang>\t<title>\t<day YYYY-MM-DD>\t<views>      (key = first 3 fields)

Desktop and mobile are merged here; the sum reducer adds them up per day.
"""
import sys


def main() -> None:
    bad = 0
    out = sys.stdout
    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 5 or not parts[4].isdigit() or len(parts[3]) < 10:
            bad += 1
            continue
        lang, _platform, title, hour, views = parts
        out.write(f"{lang}\t{title}\t{hour[:10]}\t{views}\n")
    if bad:
        sys.stderr.write(f"reporter:counter:Radar,bad_lines,{bad}\n")


if __name__ == "__main__":
    main()
