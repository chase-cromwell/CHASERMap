#!/usr/bin/env python3
"""
app.py — Flask web application for CHASERMap.

Routes:
    /                     Homepage with search
    /map/                 Leaflet map (static file)
    /races/<slug>/        Race detail page
    /candidates/<slug>/   Candidate detail page
"""

import json
import sqlite3
from pathlib import Path

from flask import Flask, abort, g, render_template, send_from_directory

from build import (
    _burn_rate,
    _fmt_dollars,
    _fmt_label,
    _fmt_name,
)

CURRENT_YEAR = 2026
DATABASE = Path(__file__).parent / "data" / "chaser.db"

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Jinja2 filters
# ---------------------------------------------------------------------------

app.jinja_env.filters["fmt_dollars"] = _fmt_dollars
app.jinja_env.filters["fmt_name"] = _fmt_name
app.jinja_env.filters["fmt_label"] = _fmt_label
app.jinja_env.filters["burn_rate"] = lambda raised, spent: _burn_rate(spent, raised)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------------------
# Search data builder (homepage)
# ---------------------------------------------------------------------------

def _build_search_json(db: sqlite3.Connection, year: int):
    """Return (races_json, statewide_json, city_map_json) strings for the homepage."""

    # All candidates for this year, legislative only
    rows = db.execute(
        """SELECT chamber, district, race_slug, label, name, party, committee, raised
           FROM candidates
           WHERE year = ? AND chamber IN ('Senate', 'House')
           ORDER BY chamber, district, raised DESC""",
        (year,),
    ).fetchall()

    # City map
    city_rows = db.execute(
        "SELECT chamber, district, city FROM city_map WHERE year = ?",
        (year,),
    ).fetchall()
    city_map: dict = {"Senate": {}, "House": {}}
    for r in city_rows:
        city_map[r["chamber"]].setdefault(r["district"], []).append(r["city"])

    # Build races dict grouped by chamber → district
    races: dict = {"Senate": {}, "House": {}}
    for row in rows:
        ch, dist = row["chamber"], row["district"]
        if dist not in races[ch]:
            races[ch][dist] = {"label": row["label"], "candidates": []}
        races[ch][dist]["candidates"].append(
            {"name": row["name"], "party": row["party"], "committee": row["committee"]}
        )

    # Statewide candidates (chamber = office name, e.g. 'Governor')
    sw_rows = db.execute(
        """SELECT chamber, race_slug, label, name, party, committee, raised
           FROM candidates
           WHERE year = ? AND chamber NOT IN ('Senate', 'House')
           ORDER BY chamber, raised DESC""",
        (year,),
    ).fetchall()
    statewide: dict = {}
    for row in sw_rows:
        office = row["chamber"]
        if office not in statewide:
            statewide[office] = {"candidates": []}
        statewide[office]["candidates"].append(
            {"name": row["name"], "party": row["party"], "committee": row["committee"]}
        )

    return (
        json.dumps(races),
        json.dumps(statewide),
        json.dumps(city_map),
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def homepage():
    db = get_db()
    races_json, statewide_json, city_map_json = _build_search_json(db, CURRENT_YEAR)
    return render_template(
        "home.html",
        races_json=races_json,
        statewide_json=statewide_json,
        city_map_json=city_map_json,
        active="home",
    )


@app.route("/map/")
def map_page():
    return send_from_directory("map", "index.html")


@app.route("/races/<slug>/")
def race_page(slug):
    db = get_db()
    candidates = db.execute(
        """SELECT * FROM candidates
           WHERE year = ? AND race_slug = ?
           ORDER BY raised DESC""",
        (CURRENT_YEAR, slug),
    ).fetchall()
    if not candidates:
        abort(404)

    first = candidates[0]
    chamber = first["chamber"]
    district = first["district"]
    label = _fmt_label(first["label"])

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
        active="",
    )


@app.route("/candidates/<slug>/")
def candidate_page(slug):
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
    app.run(debug=True)
