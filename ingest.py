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
import csv
import json
import sqlite3
import sys
from pathlib import Path

# finance_builder is optional — quarterly charts are skipped if the finance
# CSVs haven't been downloaded yet.
try:
    from finance_builder import load_quarterly_data as _load_quarterly_data
    _HAS_FINANCE = True
except ImportError:
    _HAS_FINANCE = False

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

DEFAULT_DB       = Path(__file__).parent / "data" / "chaser.db"
DEFAULT_YEAR     = 2026
CONTACTS_CSV     = Path(__file__).parent / "data" / "tracer_2026_contacts.csv"

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
    slug        TEXT    NOT NULL,       -- URL slug for candidate detail page
    -- Contact / TRACER detail fields (populated by ingest --contacts)
    phone           TEXT,              -- candidate or committee phone number
    email           TEXT,              -- candidate email address
    web             TEXT,              -- website display text (protocol stripped)
    web_href        TEXT,              -- website full href for <a> tag
    tracer_org_id   TEXT,              -- TRACER internal OrgID
    tracer_cand_id  TEXT,              -- TRACER CandidateID
    tracer_comm_id  TEXT,              -- TRACER CommitteeID
    date_filed      TEXT,              -- date candidate declared / filed
    date_terminated TEXT,              -- date committee was terminated (if any)
    complaint_count INTEGER DEFAULT 0, -- number of complaints on file
    complaints_json TEXT,              -- JSON array of complaint objects
    filings_json    TEXT,              -- JSON array of recent filing objects
    quarters_raised_json TEXT,         -- JSON object: { "2024-Q1": 500.0, ... }
    quarters_spent_json  TEXT          -- JSON object: { "2024-Q1": 300.0, ... }
);

CREATE TABLE IF NOT EXISTS city_map (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    year        INTEGER NOT NULL,
    chamber     TEXT    NOT NULL,       -- "Senate" or "House"
    district    TEXT    NOT NULL,       -- district number string
    city        TEXT    NOT NULL        -- city/place name from Census TIGER data
);

-- Individual contribution records, loaded from all finance CSVs.
-- Covers ALL Colorado committees, not just tracked candidates.
-- donor_last / donor_first / donor_city / donor_state stored UPPER-CASE
-- to allow consistent case-insensitive prefix search via LIKE 'SMITH%'.
CREATE TABLE IF NOT EXISTS contributions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    date           TEXT    NOT NULL,           -- "YYYY-MM-DD"
    amount         REAL    NOT NULL DEFAULT 0,
    co_id          TEXT    NOT NULL,           -- TRACER committee object ID
    committee_name TEXT    NOT NULL DEFAULT '',
    candidate_name TEXT             DEFAULT '',-- as reported in CSV
    race_slug      TEXT             DEFAULT '',-- denormalized for direct linking
    cand_slug      TEXT             DEFAULT '',-- denormalized for direct linking
    donor_last     TEXT    NOT NULL DEFAULT '',
    donor_first    TEXT    NOT NULL DEFAULT '',
    donor_city     TEXT    NOT NULL DEFAULT '',
    donor_state    TEXT    NOT NULL DEFAULT '',
    donor_type     TEXT    NOT NULL DEFAULT '' -- I/L/B/C/O from classify_type()
);

CREATE INDEX IF NOT EXISTS idx_contrib_donor ON contributions(donor_last, donor_first, donor_city, donor_state);
CREATE INDEX IF NOT EXISTS idx_contrib_co_id ON contributions(co_id);
CREATE INDEX IF NOT EXISTS idx_contrib_date  ON contributions(date);

-- FTS5 virtual table for infix/full-text donor name search.
-- content= makes it a "content table" backed by contributions so we don't
-- duplicate the text data; it must be rebuilt after each bulk insert.
CREATE VIRTUAL TABLE IF NOT EXISTS donor_fts USING fts5(
    donor_last, donor_first, donor_city, donor_state,
    content='contributions', content_rowid='id'
);

-- Indexes to speed up the Flask app's most common queries
CREATE INDEX IF NOT EXISTS idx_cand_race   ON candidates(year, race_slug);
CREATE INDEX IF NOT EXISTS idx_cand_slug   ON candidates(slug);
CREATE INDEX IF NOT EXISTS idx_city_lookup ON city_map(year, chamber, district);
"""


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------

def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add any contact columns that don't yet exist in the candidates table.

    SQLite does not support ALTER TABLE ... ADD COLUMN IF NOT EXISTS, so we
    attempt each ALTER TABLE individually and silently swallow the
    OperationalError that fires when the column is already present.

    This makes re-running ingest.py safe on an existing database that was
    created before the contact columns were added to SCHEMA.
    """
    contact_columns = [
        ("phone",           "TEXT"),
        ("email",           "TEXT"),
        ("web",             "TEXT"),
        ("web_href",        "TEXT"),
        ("tracer_org_id",   "TEXT"),
        ("tracer_cand_id",  "TEXT"),
        ("tracer_comm_id",  "TEXT"),
        ("date_filed",      "TEXT"),
        ("date_terminated", "TEXT"),
        ("complaint_count",       "INTEGER DEFAULT 0"),
        ("complaints_json",       "TEXT"),
        ("filings_json",          "TEXT"),
        ("quarters_raised_json",  "TEXT"),
        ("quarters_spent_json",   "TEXT"),
    ]
    for col_name, col_type in contact_columns:
        try:
            conn.execute(
                f"ALTER TABLE candidates ADD COLUMN {col_name} {col_type}"
            )
        except Exception:
            pass  # column already exists — safe to continue
    conn.commit()


def init_db(path: Path) -> sqlite3.Connection:
    """Open (or create) the SQLite database and apply the schema.

    Creates the parent directory if needed, applies the SCHEMA SQL
    (CREATE TABLE IF NOT EXISTS — safe to re-run), runs _migrate_schema()
    to add any new columns to existing databases, commits, and returns
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
    _migrate_schema(conn)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Contact data loader
# ---------------------------------------------------------------------------

def _load_contacts(path: Path = CONTACTS_CSV) -> dict[str, dict]:
    """Read the contacts CSV produced by ``scraper.py --contacts``.

    Returns a dict keyed by committee name (case-insensitive, stripped) whose
    values are plain dicts with all contact/TRACER fields ready to UPDATE into
    the candidates table.

    Returns an empty dict if the contacts CSV does not exist so that ingest
    works normally even when the --contacts scrape hasn't been run yet.

    Args:
        path: Filesystem path to tracer_2026_contacts.csv.

    Returns:
        { "committee name": { phone, email, web, web_href, tracer_org_id,
                               tracer_cand_id, tracer_comm_id, date_filed,
                               date_terminated, complaint_count,
                               complaints_json, filings_json } }
    """
    if not path.exists():
        return {}

    contacts: dict[str, dict] = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row.get("CommitteeName") or "").strip().lower()
            if not key:
                continue
            # Normalise the CO SOS redaction placeholder so it is treated as absent
            phone = row.get("Phone", "").strip()
            if phone == "999-999-9999":
                phone = ""
            contacts[key] = {
                "phone":           phone,
                "email":           row.get("Email", "").strip(),
                "web":             row.get("Web", "").strip(),
                "web_href":        row.get("WebHref", "").strip(),
                "tracer_org_id":   row.get("OrgID", "").strip(),
                "tracer_cand_id":  row.get("CandidateID", "").strip(),
                "tracer_comm_id":  row.get("CommitteeID", "").strip(),
                "date_filed":      row.get("DateFiled", "").strip(),
                "date_terminated": row.get("DateTerminated", "").strip(),
                "complaint_count": int(row.get("ComplaintCount") or 0),
                "complaints_json": row.get("ComplaintsJSON", "").strip() or "[]",
                "filings_json":    row.get("FilingsJSON",   "").strip() or "[]",
            }
    return contacts


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

    # ------------------------------------------------------------------
    # Merge contact data (if tracer_2026_contacts.csv exists)
    # ------------------------------------------------------------------
    contacts = _load_contacts()
    if contacts:
        matched = 0
        for row in conn.execute(
            "SELECT id, committee FROM candidates WHERE year = ? AND committee IS NOT NULL",
            (year,),
        ).fetchall():
            key = (row["committee"] or "").strip().lower()
            c = contacts.get(key)
            if not c:
                continue
            conn.execute(
                """UPDATE candidates SET
                       phone           = ?,
                       email           = ?,
                       web             = ?,
                       web_href        = ?,
                       tracer_org_id   = ?,
                       tracer_cand_id  = ?,
                       tracer_comm_id  = ?,
                       date_filed      = ?,
                       date_terminated = ?,
                       complaint_count = ?,
                       complaints_json = ?,
                       filings_json    = ?
                   WHERE id = ?""",
                (
                    c["phone"],   c["email"],   c["web"],   c["web_href"],
                    c["tracer_org_id"], c["tracer_cand_id"], c["tracer_comm_id"],
                    c["date_filed"], c["date_terminated"],
                    c["complaint_count"], c["complaints_json"], c["filings_json"],
                    row["id"],
                ),
            )
            matched += 1
        print(f"  Merged contact data for {matched} candidates "
              f"({len(contacts)} committees in contacts CSV)")
    else:
        print("  (No contacts CSV found — run scraper.py --contacts to add contact data)")

    # ------------------------------------------------------------------
    # Merge quarterly fundraising/expenditure data (if finance CSVs exist)
    # ------------------------------------------------------------------
    if _HAS_FINANCE:
        from finance_builder import FINANCE_DIR
        if any(FINANCE_DIR.glob("*ContributionData*.csv")):
            print("Loading quarterly finance data (this may take a minute)...")
            quarterly = _load_quarterly_data()
            q_matched = 0
            for row in conn.execute(
                "SELECT id, committee FROM candidates WHERE year = ? AND committee IS NOT NULL",
                (year,),
            ).fetchall():
                key = (row["committee"] or "").strip().upper()
                q = quarterly.get(key)
                if not q:
                    continue
                conn.execute(
                    """UPDATE candidates
                           SET quarters_raised_json = ?,
                               quarters_spent_json  = ?
                       WHERE id = ?""",
                    (
                        json.dumps(q["raised"], separators=(",", ":")),
                        json.dumps(q["spent"],  separators=(",", ":")),
                        row["id"],
                    ),
                )
                q_matched += 1
            print(f"  Merged quarterly data for {q_matched} candidates "
                  f"({len(quarterly)} committees in finance CSVs)")
        else:
            print("  (No finance CSVs found — skipping quarterly chart data)")
    else:
        print("  (finance_builder not available — skipping quarterly chart data)")

    # ------------------------------------------------------------------
    # Load individual contribution records (all committees, all years)
    # ------------------------------------------------------------------
    if _HAS_FINANCE:
        from finance_builder import CONTRIBUTIONS_CSVS, parse_date, parse_amount, classify_type
        if CONTRIBUTIONS_CSVS:
            print("Loading individual contribution records...")
            # Build committee name → (race_slug, cand_slug) lookup for linking
            comm_to_slugs: dict[str, tuple] = {}
            for row in conn.execute(
                "SELECT committee, race_slug, slug FROM candidates WHERE committee IS NOT NULL"
            ).fetchall():
                comm_to_slugs[row["committee"].strip().upper()] = (row["race_slug"], row["slug"])

            # Wipe and rebuild the contributions table completely each run
            conn.execute("DELETE FROM contributions")

            batch: list[tuple] = []
            BATCH_SIZE = 5_000
            total_contrib = 0

            for csv_path in CONTRIBUTIONS_CSVS:
                print(f"  {csv_path.name}…", end=" ", flush=True)
                file_count = 0
                with open(csv_path, newline="", encoding="latin-1") as f:
                    import csv as _csv
                    for row in _csv.DictReader(f):
                        if row["Amended"].strip().upper() == "Y":
                            continue
                        dt = parse_date(row["ContributionDate"])
                        if dt is None:
                            continue
                        amount = parse_amount(row["ContributionAmount"])
                        co_id  = row["CO_ID"].strip()
                        cname  = row["CommitteeName"].strip()
                        cname_up = cname.upper()
                        slugs  = comm_to_slugs.get(cname_up, ("", ""))
                        batch.append((
                            dt.strftime("%Y-%m-%d"),
                            amount,
                            co_id,
                            cname,
                            row["CandidateName"].strip(),
                            slugs[0],  # race_slug
                            slugs[1],  # cand_slug
                            row["LastName"].strip().upper(),
                            row["FirstName"].strip().upper(),
                            row["City"].strip().upper(),
                            row["State"].strip().upper(),
                            classify_type(row["ContributorType"]),
                        ))
                        file_count += 1
                        if len(batch) >= BATCH_SIZE:
                            conn.executemany(
                                """INSERT INTO contributions
                                   (date, amount, co_id, committee_name, candidate_name,
                                    race_slug, cand_slug,
                                    donor_last, donor_first, donor_city, donor_state, donor_type)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                batch,
                            )
                            batch.clear()
                print(f"{file_count:,} rows")
                total_contrib += file_count

            if batch:
                conn.executemany(
                    """INSERT INTO contributions
                       (date, amount, co_id, committee_name, candidate_name,
                        race_slug, cand_slug,
                        donor_last, donor_first, donor_city, donor_state, donor_type)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
            print(f"  Contributions: {total_contrib:,} records loaded")
            # Rebuild the FTS5 index so donor name search stays in sync.
            # 'delete-all' + 'rebuild' is the idiomatic way to refresh a
            # content-table FTS index after a bulk replace.
            print("  Rebuilding FTS5 donor index…")
            conn.execute("INSERT INTO donor_fts(donor_fts) VALUES('delete-all')")
            conn.execute("INSERT INTO donor_fts(donor_fts) VALUES('rebuild')")
            print("  FTS5 index ready")
        else:
            print("  (No finance CSVs found — skipping contribution records)")

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
