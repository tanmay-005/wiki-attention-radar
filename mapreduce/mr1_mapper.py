#!/usr/bin/env python3
"""MR1 mapper: raw hourly pageview lines -> clean rows for our languages.

Input  (one hourly dump file per mapper, gz is not splittable):
    <domain_code> <page_title> <views> <response_size>
Output (key = first 4 fields, value = views):
    <lang>\t<platform>\t<title>\t<hour>\t<views>

The hour comes from the input file name, e.g. pageviews-20240603-120000.gz -> 2024-06-03T12.
Languages come from the RADAR_LANGS environment variable (set with -cmdenv).
Counters are reported once at the end (group "Radar").
"""
import os
import re
import sys

from common_title import is_content_page, normalize_title, parse_domain

FILE_RE = re.compile(r"pageviews-(\d{4})(\d{2})(\d{2})-(\d{2})0000")


def current_hour() -> str:
    path = (os.environ.get("mapreduce_map_input_file")
            or os.environ.get("map_input_file")
            or os.environ.get("MR_INPUT_FILE", ""))
    m = FILE_RE.search(path)
    if not m:
        sys.stderr.write(f"cannot read hour from input file name: {path!r}\n")
        sys.exit(2)
    y, mo, d, h = m.groups()
    return f"{y}-{mo}-{d}T{h}"


def counter(name: str, n: int) -> None:
    if n:
        sys.stderr.write(f"reporter:counter:Radar,{name},{n}\n")


def main() -> None:
    allowed = set(os.environ.get("RADAR_LANGS", "en,hi,ta,ja,pt").split(","))
    hour = current_hour()
    c = dict(lines_in=0, kept=0, other_domain=0, non_content=0, bad_lines=0)
    out = sys.stdout
    for line in sys.stdin:
        c["lines_in"] += 1
        parts = line.rstrip("\n").split(" ")
        if len(parts) != 4 or not parts[2].isdigit():
            c["bad_lines"] += 1
            continue
        domain, raw_title, views, _size = parts
        dom = parse_domain(domain, allowed)
        if dom is None:
            c["other_domain"] += 1
            continue
        lang, platform = dom
        if not is_content_page(lang, raw_title):
            c["non_content"] += 1
            continue
        title = normalize_title(raw_title)
        if not title:
            c["non_content"] += 1
            continue
        out.write(f"{lang}\t{platform}\t{title}\t{hour}\t{views}\n")
        c["kept"] += 1
    for k, v in c.items():
        counter(k, v)


if __name__ == "__main__":
    main()
