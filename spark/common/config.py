"""Loads config/settings.yaml for every script, job and the dashboard.

Usage:
    from spark.common.config import load, conn
    cfg = load()
    c = conn()          # connection settings for the current environment
"""
from __future__ import annotations

import datetime as dt
import os
from functools import lru_cache
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = REPO_ROOT / "config" / "settings.yaml"


@lru_cache(maxsize=1)
def load(path: str | os.PathLike | None = None) -> dict:
    """Return settings as a dict. Override the file with RADAR_SETTINGS."""
    p = Path(path or os.environ.get("RADAR_SETTINGS", DEFAULT_PATH))
    with open(p, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    _validate(cfg)
    return cfg


def env_name() -> str:
    """'in_docker' (default inside containers) or 'on_host' (dashboard on Windows)."""
    return os.environ.get("RADAR_ENV", "in_docker")


def conn(cfg: dict | None = None) -> dict:
    cfg = cfg or load()
    name = env_name()
    if name not in cfg["connections"]:
        raise ValueError(f"RADAR_ENV={name!r} not in settings.connections")
    return cfg["connections"][name]


def days_between(start: dt.date, end: dt.date) -> list[dt.date]:
    """Inclusive list of dates."""
    return [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]


def hourly_days(cfg: dict | None = None, dev: bool = False) -> list[dt.date]:
    cfg = cfg or load()
    if dev:
        return [_as_date(d) for d in cfg["dev"]["hourly_days"]]
    return days_between(_as_date(cfg["period"]["hourly_start"]), _as_date(cfg["period"]["hourly_end"]))


def country_days(cfg: dict | None = None, dev: bool = False) -> list[dt.date]:
    cfg = cfg or load()
    if dev:
        return [_as_date(d) for d in cfg["dev"]["country_days"]]
    return days_between(_as_date(cfg["period"]["country_start"]), _as_date(cfg["period"]["country_end"]))


def _as_date(v) -> dt.date:
    return v if isinstance(v, dt.date) else dt.date.fromisoformat(str(v))


def _validate(cfg: dict) -> None:
    for key in ("period", "dev", "languages", "spikes", "paths", "wikimedia", "connections"):
        if key not in cfg:
            raise KeyError(f"settings.yaml is missing '{key}'")
    p = cfg["period"]
    hs, de = _as_date(p["hourly_start"]), _as_date(p["detect_start"])
    need = cfg["spikes"]["global_hourly"]["baseline_hours"] // 24
    if (de - hs).days < need:
        raise ValueError(f"detect_start must be >= {need} days after hourly_start (baseline window)")
    if "YOUR_EMAIL_HERE" in cfg["wikimedia"]["user_agent"]:
        import sys
        print("WARNING: set wikimedia.user_agent in config/settings.yaml to include your email", file=sys.stderr)


if __name__ == "__main__":
    c = load()
    print("languages:", c["languages"])
    print("environment:", env_name(), conn(c))
    print("dev hourly days:", [str(d) for d in hourly_days(c, dev=True)])
    print("full hourly days:", len(hourly_days(c)), "| country days:", len(country_days(c)))
