#!/usr/bin/env python3
"""MR4 map-only search: every hourly row for one title (optionally one language).

Reads MR1 output: <lang>\t<platform>\t<title>\t<hour>\t<views>
SEARCH_TITLE (underscores or spaces both fine) and optional SEARCH_LANG come from -cmdenv.
There is no reducer: matching rows are written straight to the output.
"""
import os
import sys

from common_title import normalize_title


def main() -> None:
    want = normalize_title(os.environ.get("SEARCH_TITLE", ""))
    want_lang = os.environ.get("SEARCH_LANG", "")
    if not want:
        sys.stderr.write("SEARCH_TITLE is not set\n")
        sys.exit(2)
    hits = 0
    out = sys.stdout
    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 5 and parts[2] == want and (not want_lang or parts[0] == want_lang):
            out.write(line if line.endswith("\n") else line + "\n")
            hits += 1
    sys.stderr.write(f"reporter:counter:Radar,search_hits,{hits}\n")


if __name__ == "__main__":
    main()
