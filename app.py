#!/usr/bin/env python3
"""
app.py — Flask web application for CHASERMap.

This is the main entry point for the web server. It:
  - Connects to the SQLite database populated by ingest.py
  - Builds JSON payloads from candidate/race data for the frontend
  - Renders Jinja2 templates for each page
  - Exposes formatting helpers (from build.py) as Jinja2 template filters

Routes:
    GET /                     Homepage with search, leaderboard, and race explorer
    GET /map/                 Static self-contained Leaflet district map
    GET /races/<slug>/        Race detail page (all candidates + city list)
    GET /candidates/<slug>/   Individual candidate detail page

Database:
    Reads from data/chaser.db (SQLite), which must be created by running:
        python3 ingest.py

Development server:
    python3 app.py           # runs on http://localhost:5001
    PORT=8080 python3 app.py # override port via environment variable

Production:
    gunicorn app:app         # used by Render / other WSGI hosts
"""

import json
import sqlite3
from pathlib import Path

from flask import Flask, abort, g, render_template, send_from_directory

# Import pure formatting/helper functions from build.py so templates can use
# them as Jinja2 filters without duplicating logic.
from build import (
    _burn_rate,
    _fmt_dollars,
    _fmt_label,
    _fmt_name,
)

# ---------------------------------------------------------------------------
# App-level constants
# ---------------------------------------------------------------------------

# The active election year.  Update this when moving to a new cycle.
CURRENT_YEAR = 2026

# Absolute path to the SQLite database file.  Using Path(__file__).parent
# ensures this works regardless of the working directory when the server
# is launched.
DATABASE = Path(__file__).parent / "data" / "chaser.db"

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Jinja2 filters
# ---------------------------------------------------------------------------
# Register formatting helpers so they can be used inside templates with the
# pipe syntax, e.g. {{ candidate.raised | fmt_dollars }}.

app.jinja_env.filters["fmt_dollars"] = _fmt_dollars
app.jinja_env.filters["fmt_name"] = _fmt_name
app.jinja_env.filters["fmt_label"] = _fmt_label
# burn_rate in build.py takes (spent, raised) but the template passes them as
# (raised, spent), so we swap the argument order here with a lambda.
app.jinja_env.filters["burn_rate"] = lambda raised, spent: _burn_rate(spent, raised)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    """Return (or create) the per-request SQLite connection stored in Flask's g.

    Flask's application-context object `g` is fresh for every HTTP request,
    so this lazily opens a connection on first access and caches it for the
    duration of that request.  The connection is closed in close_db() below.

    row_factory = sqlite3.Row makes each row behave like a dict, so templates
    can access columns by name (e.g. cand["name"]) instead of by index.
    """
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    """Close the SQLite connection at the end of each request.

    Registered with Flask's teardown mechanism so it runs automatically even
    if the request raised an exception (exc is the exception or None).
    """
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------------------
# Search data builder (homepage)
# ---------------------------------------------------------------------------

def _build_search_json(db: sqlite3.Connection, year: int):
    """Build the three JSON blobs used by the homepage search widget.

    The homepage JavaScript receives these as inline <script> variables and
    uses them for instant client-side search without additional API calls.

    Args:
        db:   Open SQLite connection (with row_factory = sqlite3.Row).
        year: The election year to query (e.g. 2026).

    Returns:
        A tuple of three JSON strings:
          races_json      — { "Senate": { "3": { label, candidates: [...] } }, "House": {...} }
                            Candidates within each race are sorted by raised DESC so the top
                            fundraiser appears first in autocomplete results.
          statewide_json  — { "Governor": { candidates: [...] }, ... }
                            Keyed by office name string (the chamber field for statewide rows).
          city_map_json   — { "Senate": { "3": ["Denver", "Aurora", ...] }, "House": {...} }
                            Used to map a city search query to the correct district.
    """

    # Fetch all legislative candidates (Senate + House) for this year, sorted
    # so the highest fundraiser in each race is first (helps autocomplete).
    rows = db.execute(
        """SELECT chamber, district, race_slug, label, name, party, committee, raised
           FROM candidates
           WHERE year = ? AND chamber IN ('Senate', 'House')
           ORDER BY chamber, district, raised DESC""",
        (year,),
    ).fetchall()

    # Build a nested dict: chamber → district → {label, candidates list}
    # e.g. races["Senate"]["3"] = {"label": "Senate District 3", "candidates": [...]}
    races: dict = {"Senate": {}, "House": {}}
    for row in rows:
        ch, dist = row["chamber"], row["district"]
        if dist not in races[ch]:
            # First candidate seen for this district — initialize the entry
            races[ch][dist] = {"label": row["label"], "candidates": []}
        races[ch][dist]["candidates"].append(
            {"name": row["name"], "party": row["party"], "committee": row["committee"]}
        )

    # Fetch all cities mapped to each district (populated by ingest.py via
    # the geographic centroid check in build.py's build_city_map()).
    city_rows = db.execute(
        "SELECT chamber, district, city FROM city_map WHERE year = ?",
        (year,),
    ).fetchall()

    # Restructure into chamber → district → [cities] for easy JS lookup
    city_map: dict = {"Senate": {}, "House": {}}
    for r in city_rows:
        city_map[r["chamber"]].setdefault(r["district"], []).append(r["city"])

    # Statewide candidates use the office name (e.g. "Governor") as the
    # chamber field.  Filter everything that isn't Senate/House.
    sw_rows = db.execute(
        """SELECT chamber, race_slug, label, name, party, committee, raised
           FROM candidates
           WHERE year = ? AND chamber NOT IN ('Senate', 'House')
           ORDER BY chamber, raised DESC""",
        (year,),
    ).fetchall()

    # Build statewide dict: office → {candidates: [...]}
    statewide: dict = {}
    for row in sw_rows:
        office = row["chamber"]
        if office not in statewide:
            statewide[office] = {"candidates": []}
        statewide[office]["candidates"].append(
            {"name": row["name"], "party": row["party"], "committee": row["committee"]}
        )

    # Serialize to JSON strings for direct injection into <script> tags
    return (
        json.dumps(races),
        json.dumps(statewide),
        json.dumps(city_map),
    )


# ---------------------------------------------------------------------------
# Homepage data builder
# ---------------------------------------------------------------------------

def _build_homepage_data(db: sqlite3.Connection, year: int) -> tuple[str, str]:
    """Build JSON for the homepage leaderboard and all-races explorer.

    These blobs power the two main homepage sections below the search bar:
      - The "Top Fundraisers" leaderboard (top 25 active candidates by raised)
      - The "All Races" tab table with per-race D/R/Total breakdowns

    Args:
        db:   Open SQLite connection.
        year: Election year to query.

    Returns:
        leaderboard_json — JSON array of up to 25 objects:
            { name, party, label, chamber, district, raised, slug }
            Only Active candidates are included; sorted by raised DESC.

        races_agg_json — JSON array, one object per race:
            { race_slug, label, chamber, district, total_raised,
              d_raised, r_raised, cand_count }
            Ordered by chamber then district (integer-cast so 10 sorts after 9).
            This stable ordering lets the JS client split the array into
            House / Senate / Statewide tabs without re-sorting.
    """

    # Top-25 active candidates across all chambers, for the leaderboard widget
    leaderboard_rows = db.execute(
        """SELECT name, party, label, chamber, district, raised, slug
           FROM candidates
           WHERE year = ? AND status = 'Active'
           ORDER BY raised DESC
           LIMIT 25""",
        (year,),
    ).fetchall()
    # Convert sqlite3.Row objects to plain dicts so json.dumps can serialize them
    leaderboard = [dict(r) for r in leaderboard_rows]

    # Per-race aggregates: sum raised split by party, plus a candidate count.
    # CASE WHEN filters contributions to only the relevant party before summing.
    # CAST(district AS INTEGER) ensures "10" sorts after "9" rather than after "1".
    agg_rows = db.execute(
        """SELECT
             race_slug,
             label,
             chamber,
             district,
             SUM(raised)                                                  AS total_raised,
             SUM(CASE WHEN party = 'Democratic' THEN raised ELSE 0 END)  AS d_raised,
             SUM(CASE WHEN party = 'Republican' THEN raised ELSE 0 END)  AS r_raised,
             COUNT(*)                                                      AS cand_count
           FROM candidates
           WHERE year = ?
           GROUP BY race_slug
           ORDER BY chamber, CAST(district AS INTEGER)""",
        (year,),
    ).fetchall()
    races_agg = [dict(r) for r in agg_rows]

    return json.dumps(leaderboard), json.dumps(races_agg)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def homepage():
    """Render the homepage.

    Fetches all data needed for:
      - The unified search bar (races, candidates, cities)
      - The top-25 fundraiser leaderboard
      - The all-races D vs R breakdown table

    All data is serialized to JSON and injected into the template as inline
    <script> variables so the page works without any additional API calls
    after the initial load.
    """
    db = get_db()
    races_json, statewide_json, city_map_json = _build_search_json(db, CURRENT_YEAR)
    leaderboard_json, races_agg_json = _build_homepage_data(db, CURRENT_YEAR)
    return render_template(
        "home.html",
        races_json=races_json,           # search: legislative races by district
        statewide_json=statewide_json,   # search: statewide offices
        city_map_json=city_map_json,     # search: city → district lookup
        leaderboard_json=leaderboard_json,  # top-25 leaderboard widget
        races_agg_json=races_agg_json,   # all-races D/R/Total table
        active="home",                   # highlights the "Home" nav link
    )


@app.route("/map/")
def map_page():
    """Serve the self-contained Leaflet district map.

    map/index.html is generated by build.py and contains all data and
    JavaScript inline — it does not make any API calls back to the Flask app.
    send_from_directory serves it directly from the map/ directory with the
    correct MIME type and caching headers.
    """
    return send_from_directory("map", "index.html")


@app.route("/races/<slug>/")
def race_page(slug):
    """Render the detail page for a single race (legislative district or statewide office).

    URL pattern: /races/senate-3/  or  /races/governor/

    Args:
        slug: The race_slug value stored in the candidates table.
              Legislative format: "{chamber.lower()}-{district}"  e.g. "senate-3"
              Statewide format:   slugified office name            e.g. "governor"

    Returns 404 if no candidates match the slug + year.

    Template context:
        candidates    — List of sqlite3.Row objects for all candidates in the race,
                        ordered by raised DESC.
        cities        — List of city names whose centroids fall in this district
                        (empty for statewide races).
        label         — Human-readable race name, e.g. "Senate District 3".
        subtitle      — Secondary heading, e.g. "Colorado 2026" or "Statewide — Colorado 2026".
        chamber       — "Senate", "House", or office name string for statewide.
        district      — District number string (e.g. "3") or race slug for statewide.
        is_legislative — True for Senate/House races; used by template to conditionally
                         show the city list section.
    """
    db = get_db()

    # Fetch all candidates for this race, ranked by fundraising
    candidates = db.execute(
        """SELECT * FROM candidates
           WHERE year = ? AND race_slug = ?
           ORDER BY raised DESC""",
        (CURRENT_YEAR, slug),
    ).fetchall()

    if not candidates:
        abort(404)

    # Use the first (highest-fundraising) candidate's row for race metadata
    first = candidates[0]
    chamber = first["chamber"]
    district = first["district"]
    label = _fmt_label(first["label"])  # title-case the district label

    # Legislative districts have a city list; statewide offices do not
    is_legislative = chamber in ("Senate", "House")
    if is_legislative:
        city_rows = db.execute(
            "SELECT city FROM city_map WHERE year = ? AND chamber = ? AND district = ?",
            (CURRENT_YEAR, chamber, district),
        ).fetchall()
        cities = [r["city"] for r in city_rows]
        subtitle = f"Colorado {CURRENT_YEAR}"
    else:
        cities = []
        subtitle = f"Statewide &mdash; Colorado {CURRENT_YEAR}"

    return render_template(
        "race.html",
        candidates=candidates,
        cities=cities,
        label=label,
        subtitle=subtitle,
        chamber=chamber,
        district=district,
        is_legislative=is_legislative,
        active="",  # no nav link is highlighted for race pages
    )


@app.route("/candidates/<slug>/")
def candidate_page(slug):
    """Render the detail page for a single candidate.

    URL pattern: /candidates/john-smith-senate-3/

    Args:
        slug: The candidate's unique slug (generated by _candidate_slug() in build.py).
              Format: "{first}-{last}-{chamber-district}"

    Returns 404 if no matching candidate is found for the current year.

    Template context:
        cand — sqlite3.Row with all columns from the candidates table.
    """
    db = get_db()
    cand = db.execute(
        "SELECT * FROM candidates WHERE slug = ? AND year = ?",
        (slug, CURRENT_YEAR),
    ).fetchone()
    if not cand:
        abort(404)
    return render_template("candidate.html", cand=cand, active="")


# ---------------------------------------------------------------------------
# Entry point (dev server)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    # Allow overriding port via PORT environment variable (Render sets this)
    port = int(os.environ.get("PORT", 5001))
    # debug=True enables auto-reload on file changes and the Werkzeug debugger.
    # Never run with debug=True in production — use gunicorn instead.
    app.run(debug=True, port=port)
