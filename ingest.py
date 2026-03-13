#!/usr/bin/env python3
"""
ingest.py — Load CSV campaign finance data into SQLite for the Flask app.

Usage:
    python3 ingest.py              # ingest 2026 data (default)
    python3 ingest.py --year 2024  # ingest a different election year
    python3 ingest.py --db path/to/other.db

This script imports data-loading functions from build.py (they are pure,
side-effect-free functions that read files and return dicts). It then writes
all candidate and city-map data into a SQLite database, replacing any
existing rows for the target year so re-running is idempotent.
"""

import argparse
import sqlite3
import sys
from pathlib import Path

# Import reusable data-loading and helper functions from build.py.
# build.py's module-level code is safe to import (only defines constants,
# strings, and functions; main() is guarded by __name__ == "__main__").
from build import (
    load_races,
    load_statewide_races,
    load_places,
    build_city_map,
    shapefile_to_geojson,
    _slugify,
    _candidate_slug,
    _fmt_label,
)

DEFAULT_DB   = Path(__file__).parent / "data" / "chaser.db"
DEFAULT_YEAR = 2026

SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    year        INTEGER NOT NULL,
    chamber     TEXT    NOT NULL,
    district    TEXT    NOT NULL,
    race_slug   TEXT    NOT NULL,
    label       TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    party       TEXT,
    committee   TEXT,
    status      TEXT,
    raised      REAL    NOT NULL DEFAULT 0,
    spent       REAL    NOT NULL DEFAULT 0,
    coh         REAL    NOT NULL DEFAULT 0,
    beg         REAL    NOT NULL DEFAULT 0,
    loans       REAL    NOT NULL DEFAULT 0,
    vsl         TEXT,
    incumbent   INTEGER NOT NULL DEFAULT 0,
    slug        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS city_map (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    year        INTEGER NOT NULL,
    chamber     TEXT    NOT NULL,
    district    TEXT    NOT NULL,
    city        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cand_race   ON candidates(year, race_slug);
CREATE INDEX IF NOT EXISTS idx_cand_slug   ON candidates(slug);
CREATE INDEX IF NOT EXISTS idx_city_lookup ON city_map(year, chamber, district);
"""


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def ingest(year: int, db_path: Path) -> None:
    print(f"Loading candidate data...")
    races = load_races()
    senate_count = sum(len(v["candidates"]) for v in races["Senate"].values())
    house_count  = sum(len(v["candidates"]) for v in races["House"].values())
    print(f"  Senate: {len(races['Senate'])} districts, {senate_count} candidates")
    print(f"  House:  {len(races['House'])} districts, {house_count} candidates")

    print("Loading statewide data...")
    statewide = load_statewide_races()
    sw_count  = sum(len(v["candidates"]) for v in statewide.values())
    print(f"  {len(statewide)} offices, {sw_count} candidates")

    print("Loading GeoJSON + places for city map...")
    gj_senate = shapefile_to_geojson("Senate")
    gj_house  = shapefile_to_geojson("House")
    places    = load_places()
    city_map  = build_city_map(gj_senate, gj_house, places)
    s_mapped  = sum(1 for v in city_map["Senate"].values() if v)
    h_mapped  = sum(1 for v in city_map["House"].values()  if v)
    print(f"  Senate: {s_mapped} districts with cities, House: {h_mapped}")

    conn = init_db(db_path)

    # Wipe existing rows for this year so re-runs are idempotent
    conn.execute("DELETE FROM candidates WHERE year = ?", (year,))
    conn.execute("DELETE FROM city_map   WHERE year = ?", (year,))

    cand_rows = []

    # Legislative candidates
    for chamber in ("Senate", "House"):
        for dist, data in races.get(chamber, {}).items():
            race_slug = f"{chamber.lower()}-{dist}"
            label     = _fmt_label(data["label"])
            for cand in data["candidates"]:
                slug = _candidate_slug(cand["name"], f"{chamber}-{dist}")
                cand_rows.append((
                    year, chamber, dist, race_slug, label,
                    cand["name"], cand.get("party"), cand.get("committee"),
                    cand.get("status"),
                    cand.get("raised", 0.0), cand.get("spent", 0.0),
                    cand.get("coh",    0.0), cand.get("beg",   0.0),
                    cand.get("loans",  0.0), cand.get("vsl"),
                    1 if cand.get("incumbent") else 0,
                    slug,
                ))

    # Statewide candidates
    for office, data in statewide.items():
        race_slug = _slugify(office)
        label     = _fmt_label(office)
        for cand in data["candidates"]:
            slug = _candidate_slug(cand["name"], office)
            cand_rows.append((
                year, office, race_slug, race_slug, label,
                cand["name"], cand.get("party"), cand.get("committee"),
                cand.get("status"),
                cand.get("raised", 0.0), cand.get("spent", 0.0),
                cand.get("coh",    0.0), 0.0,
                cand.get("loans",  0.0), cand.get("vsl"),
                0,  # incumbent not tracked for statewide
                slug,
            ))

    conn.executemany(
        """INSERT INTO candidates
           (year, chamber, district, race_slug, label, name, party, committee,
            status, raised, spent, coh, beg, loans, vsl, incumbent, slug)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        cand_rows,
    )

    # City map rows
    city_rows = []
    for chamber in ("Senate", "House"):
        for dist, cities in city_map.get(chamber, {}).items():
            for city in cities:
                city_rows.append((year, chamber, dist, city))

    conn.executemany(
        "INSERT INTO city_map (year, chamber, district, city) VALUES (?,?,?,?)",
        city_rows,
    )

    conn.commit()
    conn.close()

    total = len(cand_rows)
    print(f"\n✓ Ingested {total} candidates + {len(city_rows)} city-map rows "
          f"into {db_path} (year={year})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest CSV data into SQLite for Flask app")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR,
                        help=f"Election year (default: {DEFAULT_YEAR})")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help=f"SQLite database path (default: {DEFAULT_DB})")
    args = parser.parse_args()
    ingest(args.year, args.db)


if __name__ == "__main__":
    main()
