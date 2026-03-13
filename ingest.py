#!/usr/bin/env python3
"""
ingest.py — Load processed CSV campaign finance data into SQLite for the Flask app.

This script is the bridge between the data pipeline (scraper.py → build.py)
and the web application (app.py).  It reads the same candidate data structures
that build.py produces and writes them into a SQLite database that Flask can
query efficiently at request time.

Re-running ingest.py is safe and idempotent: all existing rows for the target
year are deleted before re-insertion, so the database always reflects the
latest CSV data.

Data pipeline order:
    1. python3 scraper.py              # scrape TRACER → data/*.csv
    2. python3 ingest.py               # load CSVs → data/chaser.db
    3. python3 app.py (or gunicorn)    # serve the Flask app

Database tables created (if not already present):
    candidates  — one row per candidate per election year
    city_map    — one row per (district, city) pair per year

    See SCHEMA below for full column definitions.

Usage:
    python3 ingest.py              # ingest 2026 data into data/chaser.db
    python3 ingest.py --year 2024  # ingest a different election year
    python3 ingest.py --db /path/to/other.db   # use a custom database path
"""

import argparse
import sqlite3
import sys
from pathlib import Path

# Import reusable data-loading and helper functions from build.py.
# build.py's module-level code is safe to import: it only defines constants
# and functions.  The actual build work is guarded by __name__ == "__main__".
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_DB   = Path(__file__).parent / "data" / "chaser.db"
DEFAULT_YEAR = 2026

# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    year        INTEGER NOT NULL,       -- election year, e.g. 2026
    chamber     TEXT    NOT NULL,       -- "Senate", "House", or office name for statewide
    district    TEXT    NOT NULL,       -- district number ("3") or race_slug for statewide
    race_slug   TEXT    NOT NULL,       -- URL slug, e.g. "senate-3" or "governor"
    label       TEXT    NOT NULL,       -- human-readable label, e.g. "Senate District 3"
    name        TEXT    NOT NULL,       -- TRACER format: "LAST, FIRST MIDDLE"
    party       TEXT,                   -- "Democratic", "Republican", "Unaffiliated", etc.
    committee   TEXT,                   -- campaign committee name
    status      TEXT,                   -- "Active" or "Terminated"
    raised      REAL    NOT NULL DEFAULT 0,  -- total monetary contributions
    spent       REAL    NOT NULL DEFAULT 0,  -- total monetary expenditures
    coh         REAL    NOT NULL DEFAULT 0,  -- end-of-period cash on hand
    beg         REAL    NOT NULL DEFAULT 0,  -- beginning-of-period cash on hand
    loans       REAL    NOT NULL DEFAULT 0,  -- total loans received (self-funding)
    vsl         TEXT,                   -- "Y"/"N" — accepted voluntary spending limit
    incumbent   INTEGER NOT NULL DEFAULT 0,  -- 1 if current seat-holder, 0 otherwise
    slug        TEXT    NOT NULL        -- URL slug for candidate detail page
);

CREATE TABLE IF NOT EXISTS city_map (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    year        INTEGER NOT NULL,
    chamber     TEXT    NOT NULL,       -- "Senate" or "House"
    district    TEXT    NOT NULL,       -- district number string
    city        TEXT    NOT NULL        -- city/place name from Census TIGER data
);

-- Indexes to speed up the Flask app's most common queries
CREATE INDEX IF NOT EXISTS idx_cand_race   ON candidates(year, race_slug);
CREATE INDEX IF NOT EXISTS idx_cand_slug   ON candidates(slug);
CREATE INDEX IF NOT EXISTS idx_city_lookup ON city_map(year, chamber, district);
"""


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------

def init_db(path: Path) -> sqlite3.Connection:
    """Open (or create) the SQLite database and apply the schema.

    Creates the parent directory if needed, applies the SCHEMA SQL
    (CREATE TABLE IF NOT EXISTS — safe to re-run), commits, and returns
    an open connection with row_factory set for dict-like row access.

    Args:
        path: Filesystem path to the .db file.

    Returns:
        Open sqlite3.Connection ready for reading/writing.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Main ingest function
# ---------------------------------------------------------------------------

def ingest(year: int, db_path: Path) -> None:
    """Load all candidate and city-map data into the SQLite database.

    Steps:
        1. Load legislative races (Senate + House) via build.load_races()
        2. Load statewide races via build.load_statewide_races()
        3. Build city→district map via GeoJSON + place centroids
        4. Open / initialize the database
        5. Delete all existing rows for `year` (idempotent re-run)
        6. Insert all candidate rows in a single executemany() call
        7. Insert all city_map rows in a single executemany() call
        8. Commit and close

    Args:
        year:    Election year to use as the primary key partition.
        db_path: Path to the target SQLite file.
    """
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

    # Wipe existing rows for this year so re-runs don't create duplicates
    conn.execute("DELETE FROM candidates WHERE year = ?", (year,))
    conn.execute("DELETE FROM city_map   WHERE year = ?", (year,))

    # ------------------------------------------------------------------
    # Build candidate rows
    # ------------------------------------------------------------------
    cand_rows = []

    # Legislative candidates (Senate + House)
    for chamber in ("Senate", "House"):
        for dist, data in races.get(chamber, {}).items():
            race_slug = f"{chamber.lower()}-{dist}"   # e.g. "senate-3"
            label     = _fmt_label(data["label"])      # title-case
            for cand in data["candidates"]:
                # Generate a unique URL slug for the candidate detail page
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

    # Statewide candidates (Governor, AG, etc.)
    for office, data in statewide.items():
        # For statewide: both chamber and district store the office name/slug
        race_slug = _slugify(office)
        label     = _fmt_label(office)
        for cand in data["candidates"]:
            slug = _candidate_slug(cand["name"], office)
            cand_rows.append((
                year, office, race_slug, race_slug, label,
                cand["name"], cand.get("party"), cand.get("committee"),
                cand.get("status"),
                cand.get("raised", 0.0), cand.get("spent", 0.0),
                cand.get("coh",    0.0), 0.0,    # statewide has no beg balance
                cand.get("loans",  0.0), cand.get("vsl"),
                0,    # incumbent flag not tracked for statewide offices
                slug,
            ))

    conn.executemany(
        """INSERT INTO candidates
           (year, chamber, district, race_slug, label, name, party, committee,
            status, raised, spent, coh, beg, loans, vsl, incumbent, slug)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        cand_rows,
    )

    # ------------------------------------------------------------------
    # Build city_map rows
    # ------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest CSV data into SQLite for the CHASERMap Flask app"
    )
    parser.add_argument(
        "--year", type=int, default=DEFAULT_YEAR,
        help=f"Election year to ingest (default: {DEFAULT_YEAR})"
    )
    parser.add_argument(
        "--db", type=Path, default=DEFAULT_DB,
        help=f"SQLite database path (default: {DEFAULT_DB})"
    )
    args = parser.parse_args()
    ingest(args.year, args.db)


if __name__ == "__main__":
    main()
