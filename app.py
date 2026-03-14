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

from flask import Flask, abort, g, jsonify, render_template, request, send_from_directory

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
# from_json: parse a JSON string stored in the DB back into a Python object so
# templates can iterate over it with {% for item in cand.complaints_json | from_json %}
app.jinja_env.filters["from_json"] = lambda s: json.loads(s) if s else []


# ---------------------------------------------------------------------------
# Chart data helpers
# ---------------------------------------------------------------------------

def _qsort_key(label: str) -> tuple:
    """Sort key for "YYYY-Q#" quarter strings, e.g. ("2024", "Q1") → (2024, 1)."""
    parts = label.split("-")
    year = int(parts[0]) if parts[0].isdigit() else 0
    q    = int(parts[1][1]) if len(parts) > 1 and parts[1].startswith("Q") else 0
    return (year, q)


def _candidate_chart_data(cand) -> dict:
    """Return chart-ready raised/spent arrays for the candidate detail page.

    Args:
        cand: sqlite3.Row with quarters_raised_json and quarters_spent_json columns.

    Returns:
        { "labels": [...], "raised": [...], "spent": [...] }
        All lists are in chronological quarter order.
        Returns empty dict if no quarterly data is stored.
    """
    raised_q = json.loads(cand["quarters_raised_json"] or "{}") if cand["quarters_raised_json"] else {}
    spent_q  = json.loads(cand["quarters_spent_json"]  or "{}") if cand["quarters_spent_json"]  else {}
    if not raised_q and not spent_q:
        return {}
    all_keys = sorted(set(raised_q) | set(spent_q), key=_qsort_key)

    # Trim leading quarters where both raised and spent are zero
    start = next(
        (i for i, k in enumerate(all_keys)
         if raised_q.get(k, 0.0) > 0 or spent_q.get(k, 0.0) > 0),
        len(all_keys),
    )
    all_keys = all_keys[start:]
    if not all_keys:
        return {}

    return {
        "labels": all_keys,
        "raised": [round(raised_q.get(k, 0.0), 2) for k in all_keys],
        "spent":  [round(spent_q.get(k, 0.0),  2) for k in all_keys],
    }


_CHART_TOP_N = 3          # max candidates shown per party in the race timeline chart
_INDIE_THRESHOLD = 4      # hide non-D/R candidates when total candidates exceeds this

_MAJOR_PARTIES = {"Democratic", "Republican"}


def _race_chart_data(candidates) -> dict:
    """Return chart-ready per-candidate cumulative fundraising for the race page.

    Only the top N candidates by total raised are included per party.
    Independent / third-party candidates are hidden when the total number of
    candidates in the race exceeds _INDIE_THRESHOLD, to keep the chart readable
    in busy primaries while still showing them in small fields.

    Args:
        candidates: List of sqlite3.Row objects for all candidates in the race,
                    expected to already be ordered by raised DESC.

    Returns:
        {
          "labels": ["2024-Q1", "2024-Q2", ...],   # union of all quarters, sorted
          "series": [
            { "name": "Jane Smith", "party": "Democratic", "data": [0, 500, 1200, ...] },
            ...
          ]
        }
        Returns empty dict if no candidate has quarterly data.
    """
    total_candidates = len(candidates)
    hide_independents = total_candidates > _INDIE_THRESHOLD

    # Collect quarterly data for every candidate, keeping track of how many
    # we've already accepted per party so we can cap at _CHART_TOP_N.
    # candidates is already ordered by raised DESC, so the first N encountered
    # per party are the top N fundraisers.
    party_count: dict[str, int] = {}
    per_cand = []
    all_keys: set = set()

    for c in candidates:
        party = c["party"] or "Other"
        # Skip independents / third-party when the field is large
        if hide_independents and party not in _MAJOR_PARTIES:
            continue
        count = party_count.get(party, 0)
        if count >= _CHART_TOP_N:
            continue  # already have enough candidates for this party
        q = json.loads(c["quarters_raised_json"] or "{}") if c["quarters_raised_json"] else {}
        if not q:
            continue  # no quarterly data — skip rather than waste a slot
        party_count[party] = count + 1
        per_cand.append((c, q))
        all_keys |= set(q.keys())

    if not all_keys:
        return {}

    labels = sorted(all_keys, key=_qsort_key)

    # Trim leading quarters where no candidate has any contribution in that quarter
    start = next(
        (i for i, k in enumerate(labels)
         if any(q.get(k, 0.0) > 0 for _, q in per_cand)),
        len(labels),
    )
    labels = labels[start:]
    if not labels:
        return {}

    series = []
    for c, q in per_cand:
        cumulative = []
        running = 0.0
        for k in labels:
            running += q.get(k, 0.0)
            cumulative.append(round(running, 2))
        if running == 0.0:
            continue
        series.append({
            "name":   _fmt_name(c["name"]),
            "party":  c["party"] or "",
            "status": c["status"] or "Active",
            "data":   cumulative,
        })

    if not series:
        return {}
    return {"labels": labels, "series": series}


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

    race_chart = _race_chart_data(candidates)

    return render_template(
        "race.html",
        candidates=candidates,
        cities=cities,
        label=label,
        subtitle=subtitle,
        chamber=chamber,
        district=district,
        is_legislative=is_legislative,
        race_chart=json.dumps(race_chart, separators=(",", ":")),
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
    cand_chart = _candidate_chart_data(cand)
    return render_template(
        "candidate.html",
        cand=cand,
        cand_chart=json.dumps(cand_chart, separators=(",", ":")),
        active="",
    )


# ---------------------------------------------------------------------------
# Donor search + detail
# ---------------------------------------------------------------------------

@app.route("/search/donors")
def search_donors():
    """Return JSON donor matches for typeahead and the donor search page.

    Query params:
      q        — search string, matched as infix against donor_last + donor_first
                 via the FTS5 donor_fts index (falls back to LIKE prefix if FTS
                 is unavailable).
      year     — optional 4-digit year filter (e.g. "2026")
      type     — optional donor_type filter ("I", "B", "C", "L", "O")
      page     — 1-based page number (default 1)
      per_page — results per page (default 25, max 100)

    Returns JSON:
      { results: [{donor_last, donor_first, donor_city, donor_state, n, total}],
        page, per_page, has_more }
    """
    q        = request.args.get("q",        "").strip().upper()
    year     = request.args.get("year",     "").strip()
    dtype    = request.args.get("type",     "").strip().upper()
    try:
        page     = max(1, int(request.args.get("page",     1)))
        per_page = min(100, max(1, int(request.args.get("per_page", 25))))
    except ValueError:
        page, per_page = 1, 25

    if len(q) < 2:
        return jsonify({"results": [], "page": page, "per_page": per_page, "has_more": False})

    db     = get_db()
    offset = (page - 1) * per_page
    fetch  = per_page + 1  # fetch one extra to determine has_more

    # Build optional extra WHERE clauses for year and donor_type filters
    extra_clauses = []
    extra_params  = []
    if year:
        extra_clauses.append("strftime('%Y', date) = ?")
        extra_params.append(year)
    if dtype:
        extra_clauses.append("donor_type = ?")
        extra_params.append(dtype)
    extra_sql = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    # Try FTS5 first (infix search); fall back to prefix LIKE if not available
    try:
        # FTS5 MATCH uses the query as a prefix term by default; appending '*'
        # ensures prefix matching even for mid-string tokens.
        fts_term = q.replace('"', '""') + "*"
        rows = db.execute(
            f"""SELECT c.donor_last, c.donor_first, c.donor_city, c.donor_state,
                       COUNT(*) AS n, SUM(c.amount) AS total
                FROM donor_fts f
                JOIN contributions c ON c.id = f.rowid
                WHERE donor_fts MATCH ?{extra_sql}
                GROUP BY c.donor_last, c.donor_first, c.donor_city, c.donor_state
                ORDER BY total DESC
                LIMIT ? OFFSET ?""",
            (fts_term, *extra_params, fetch, offset),
        ).fetchall()
    except Exception:
        # FTS5 table may not exist yet (pre-ingest); fall back to LIKE prefix
        parts = q.split(None, 1)
        if len(parts) == 1:
            rows = db.execute(
                f"""SELECT donor_last, donor_first, donor_city, donor_state,
                           COUNT(*) AS n, SUM(amount) AS total
                    FROM contributions
                    WHERE donor_last LIKE ? || '%'{extra_sql}
                    GROUP BY donor_last, donor_first, donor_city, donor_state
                    ORDER BY total DESC
                    LIMIT ? OFFSET ?""",
                (parts[0], *extra_params, fetch, offset),
            ).fetchall()
        else:
            rows = db.execute(
                f"""SELECT donor_last, donor_first, donor_city, donor_state,
                           COUNT(*) AS n, SUM(amount) AS total
                    FROM contributions
                    WHERE donor_last LIKE ? || '%'
                      AND donor_first LIKE ? || '%'{extra_sql}
                    GROUP BY donor_last, donor_first, donor_city, donor_state
                    ORDER BY total DESC
                    LIMIT ? OFFSET ?""",
                (parts[0], parts[1], *extra_params, fetch, offset),
            ).fetchall()

    has_more = len(rows) > per_page
    return jsonify({
        "results":  [dict(r) for r in rows[:per_page]],
        "page":     page,
        "per_page": per_page,
        "has_more": has_more,
    })


@app.route("/donors/")
def donor_page():
    """Donor search landing page and individual donor detail page.

    Search mode  — /donors/?q=Smith
        Renders a search page with a query box and results list.
        Uses the same /search/donors API internally.

    Detail mode  — /donors/?last=SMITH&first=JOHN&city=DENVER&state=CO
        Shows a single donor's full contribution history.
        Returns 404 if no contributions match.
    """
    q     = request.args.get("q",     "").strip()
    last  = request.args.get("last",  "").strip().upper()
    first = request.args.get("first", "").strip().upper()
    city  = request.args.get("city",  "").strip().upper()
    state = request.args.get("state", "").strip().upper()

    db = get_db()

    # ── Search mode ────────────────────────────────────────────────────────
    if q and not last:
        year  = request.args.get("year",  "").strip()
        dtype = request.args.get("type",  "").strip().upper()
        try:
            page     = max(1, int(request.args.get("page", 1)))
            per_page = min(100, max(1, int(request.args.get("per_page", 25))))
        except ValueError:
            page, per_page = 1, 25

        results  = []
        has_more = False
        if len(q) >= 2:
            # Reuse the search_donors logic directly against the DB
            import urllib.request as _ur
            # Call the internal helper rather than making an HTTP round-trip
            extra_clauses, extra_params = [], []
            if year:
                extra_clauses.append("strftime('%Y', date) = ?")
                extra_params.append(year)
            if dtype:
                extra_clauses.append("donor_type = ?")
                extra_params.append(dtype)
            extra_sql = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""
            offset = (page - 1) * per_page
            fetch  = per_page + 1
            qu = q.strip().upper()
            try:
                fts_term = qu.replace('"', '""') + "*"
                rows = db.execute(
                    f"""SELECT c.donor_last, c.donor_first, c.donor_city, c.donor_state,
                               COUNT(*) AS n, SUM(c.amount) AS total
                        FROM donor_fts f
                        JOIN contributions c ON c.id = f.rowid
                        WHERE donor_fts MATCH ?{extra_sql}
                        GROUP BY c.donor_last, c.donor_first, c.donor_city, c.donor_state
                        ORDER BY total DESC
                        LIMIT ? OFFSET ?""",
                    (fts_term, *extra_params, fetch, offset),
                ).fetchall()
            except Exception:
                parts = qu.split(None, 1)
                if len(parts) == 1:
                    rows = db.execute(
                        f"""SELECT donor_last, donor_first, donor_city, donor_state,
                                   COUNT(*) AS n, SUM(amount) AS total
                            FROM contributions
                            WHERE donor_last LIKE ? || '%'{extra_sql}
                            GROUP BY donor_last, donor_first, donor_city, donor_state
                            ORDER BY total DESC
                            LIMIT ? OFFSET ?""",
                        (parts[0], *extra_params, fetch, offset),
                    ).fetchall()
                else:
                    rows = db.execute(
                        f"""SELECT donor_last, donor_first, donor_city, donor_state,
                                   COUNT(*) AS n, SUM(amount) AS total
                            FROM contributions
                            WHERE donor_last LIKE ? || '%'
                              AND donor_first LIKE ? || '%'{extra_sql}
                            GROUP BY donor_last, donor_first, donor_city, donor_state
                            ORDER BY total DESC
                            LIMIT ? OFFSET ?""",
                        (parts[0], parts[1], *extra_params, fetch, offset),
                    ).fetchall()
            has_more = len(rows) > per_page
            results  = [dict(r) for r in rows[:per_page]]

        return render_template(
            "donor_search.html",
            q=q, year=year if q else "", dtype=dtype if q else "",
            results=results, page=page, per_page=per_page, has_more=has_more,
            active="",
        )

    # ── Detail mode ────────────────────────────────────────────────────────
    if not last:
        # No params at all — show empty search page
        return render_template("donor_search.html", q="", year="", dtype="",
                               results=[], page=1, per_page=25, has_more=False,
                               active="")

    contribs = db.execute(
        """SELECT * FROM contributions
           WHERE donor_last = ? AND donor_first = ?
             AND donor_city = ? AND donor_state = ?
           ORDER BY date DESC""",
        (last, first, city, state),
    ).fetchall()

    if not contribs:
        abort(404)

    total = sum(r["amount"] for r in contribs)
    committee_count = len({r["committee_name"] for r in contribs})

    display_name = " ".join(p.title() for p in [first, last] if p) or last.title()
    display_loc  = (city.title() + ", " + state) if city else state

    return render_template(
        "donor.html",
        donor_name=display_name,
        donor_location=display_loc,
        total=total,
        committee_count=committee_count,
        contributions=contribs,
        active="",
    )


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
