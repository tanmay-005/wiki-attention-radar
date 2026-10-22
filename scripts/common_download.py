"""Shared download helpers for the Wikimedia downloaders.

- Streams to <file>.part, verifies, then renames. A final file therefore always
  means "complete and verified", so re-runs simply skip it.
- Retries with exponential back-off on network errors and 5xx/429.
- Uses the User-Agent from config/settings.yaml (Wikimedia requires one).
"""
from __future__ import annotations

import csv
import gzip
import os
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

import requests

CHUNK = 1024 * 1024  # 1 MB


@dataclass
class Result:
    url: str
    dest: Path
    status: str          # downloaded | skipped | missing | failed
    bytes: int = 0
    seconds: float = 0.0
    error: str = ""


def verify_gzip(path: Path) -> None:
    """Decompress the whole file in chunks; raises if truncated or corrupt."""
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    with open(path, "rb") as f:
        while True:
            block = f.read(CHUNK)
            if not block:
                break
            d.decompress(block)
            # multi-member gzip support
            while d.unused_data:
                rest = d.unused_data
                d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                d.decompress(rest)
    if not d.eof:
        raise IOError("gzip stream truncated")


def verify_text(path: Path) -> None:
    """Basic sanity check for plain-text/CSV downloads."""
    with open(path, "rb") as f:
        head = f.read(512).lstrip().lower()
    if not head:
        raise IOError("empty file")
    if head.startswith(b"<!doctype") or head.startswith(b"<html"):
        raise IOError("got an HTML page instead of data")


def download(url: str, dest: Path, user_agent: str, kind: str = "gzip",
             retries: int = 5, timeout=(30, 300)) -> Result:
    """Download url -> dest unless dest already exists. kind: 'gzip' | 'text'."""
    if dest.exists() and dest.stat().st_size > 0:
        return Result(url, dest, "skipped", dest.stat().st_size)

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    headers = {"User-Agent": user_agent}
    last_err = ""

    for attempt in range(1, retries + 1):
        t0 = time.time()
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
                if r.status_code == 404:
                    return Result(url, dest, "missing", error="HTTP 404")
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length") or 0)
                written = 0
                with open(part, "wb") as f:
                    for block in r.iter_content(CHUNK):
                        if block:
                            f.write(block)
                            written += len(block)
            if expected and written != expected:
                raise IOError(f"incomplete download {written}/{expected} bytes")
            if kind == "gzip":
                verify_gzip(part)
            else:
                verify_text(part)
            os.replace(part, dest)
            return Result(url, dest, "downloaded", written, time.time() - t0)
        except Exception as e:  # noqa: BLE001 - we log and retry everything
            last_err = f"{type(e).__name__}: {e}"
            if part.exists():
                part.unlink()
            if attempt < retries:
                wait = 2 ** attempt
                print(f"   retry {attempt}/{retries - 1} for {dest.name} in {wait}s ({last_err})", flush=True)
                time.sleep(wait)

    return Result(url, dest, "failed", error=last_err)


def append_log(log_path: Path, results: list[Result]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    new = not log_path.exists()
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["time", "status", "file", "bytes", "seconds", "url", "error"])
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        for r in results:
            w.writerow([now, r.status, r.dest.name, r.bytes, f"{r.seconds:.1f}", r.url, r.error])


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def summarize(results: list[Result]) -> int:
    """Print a summary and return an exit code (0 = all present)."""
    by = {}
    for r in results:
        by.setdefault(r.status, []).append(r)
    total = sum(r.bytes for r in by.get("downloaded", []))
    secs = sum(r.seconds for r in by.get("downloaded", []))
    print("\n== Summary ==")
    for s in ("downloaded", "skipped", "missing", "failed"):
        print(f"  {s:<10} {len(by.get(s, []))}")
    if total:
        print(f"  data       {human(total)} in {secs/60:.1f} min of transfer")
    bad = by.get("missing", []) + by.get("failed", [])
    for r in bad:
        print(f"  !! {r.status}: {r.dest.name} ({r.error})")
    return 1 if bad else 0
