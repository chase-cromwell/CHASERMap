#!/usr/bin/env python3
"""
TRACERMap build script
Reads data/tracer_2026_all_districts.csv + Colorado legislative district GeoJSON
and generates map/index.html — a self-contained embeddable map.

Usage:
    python3 build.py
"""

import csv
import io
import json
import re
import urllib.request
import zipfile
from pathlib import Path
import shapefile

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
MAP_DIR  = Path(__file__).parent / "map"

CSV_FILE         = DATA_DIR / "tracer_2026_all_districts.csv"
STATEWIDE_CSV    = DATA_DIR / "tracer_2026_statewide.csv"
OUTPUT_HTML      = MAP_DIR  / "index.html"

SHAPEFILE_URLS = {
    "Senate": "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_08_sldu_500k.zip",
    "House":  "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_08_sldl_500k.zip",
}
GEOJSON_CACHE = {
    "Senate": DATA_DIR / "co_senate_districts.json",
    "House":  DATA_DIR / "co_house_districts.json",
}
# Field name in the shapefile attributes that holds the district number code
DISTRICT_FIELD = {
    "Senate": "SLDUST",
    "House":  "SLDLST",
}

PLACES_URL   = "https://www2.census.gov/geo/tiger/TIGER2022/PLACE/tl_2022_08_place.zip"
PLACES_CACHE      = DATA_DIR / "co_places.json"
LEGISLATORS_FILE  = DATA_DIR / "co_legislators.xlsx"

# ---------------------------------------------------------------------------
# Incumbent detection
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(r'\b(JR\.?|SR\.?|II|III|IV|V)\b\.?', re.IGNORECASE)

# Incumbents who file under a nickname differing from their legal first name.
# Maps (chamber, district) -> the first name as it appears in TRACER.
_NICKNAME_FIRST_NAMES: dict[tuple, str] = {
    ("Senate", "11"): "THOMAS",   # Tony Exum Sr. files as Thomas E. Exum Sr.
    ("House",  "40"): "NIKKI",    # Naquetta Ricks files as Nikki Ricks
}

# Incumbents not in the Excel file (e.g. mid-session appointments, corrections)
# Maps (chamber, district) -> TRACER last name (upper, suffix-stripped)
_EXTRA_INCUMBENTS: dict[tuple, str] = {
    ("House", "33"): "NGUYEN",    # Kenny Nguyen (HD-33)
}


def load_incumbents() -> dict:
    """
    Load current CO legislators from the official Excel export.
    Returns {(chamber, district): (last_upper, first_upper)}.
    """
    try:
        import openpyxl
    except ImportError:
        print("  \u26a0  openpyxl not installed — no incumbent flags (pip install openpyxl)")
        return {}
    if not LEGISLATORS_FILE.exists():
        print(f"  \u26a0  {LEGISLATORS_FILE.name} not found — no incumbent flags")
        return {}

    wb = openpyxl.load_workbook(LEGISLATORS_FILE)
    result = {}
    for sheet, chamber in [("Representatives", "House"), ("Senators", "Senate")]:
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        for row in ws.iter_rows(min_row=3, values_only=True):
            if not row[1]:
                continue
            first = row[1].upper().strip()
            last  = row[2].upper().strip()
            dist  = str(int(row[6]))
            result[(chamber, dist)] = (last, first)
    print(f"  Loaded {len(result)} incumbents from {LEGISLATORS_FILE.name}")
    return result


def is_incumbent(name: str, chamber: str, dist: str, incumbents: dict) -> bool:
    """
    Return True if the TRACER candidate name matches the current incumbent
    for (chamber, dist).  Handles:
      - Generational suffixes in TRACER (Jr./Sr./IV stripped before compare)
      - Compound last names (e.g. 'WILSON' matches incumbent 'ZAMORA WILSON')
      - Middle name used as preferred first (e.g. 'TIMOTHY JARVIS' -> JARVIS)
      - Short-form first names (e.g. 'RODNEY' matches 'ROD' via 3-char prefix)
      - Explicit nickname overrides in _NICKNAME_FIRST_NAMES
      - Extra incumbents in _EXTRA_INCUMBENTS
    """
    info = incumbents.get((chamber, dist))

    # Check _EXTRA_INCUMBENTS first (last-name-only match)
    extra_last = _EXTRA_INCUMBENTS.get((chamber, dist))

    parts = name.strip().upper().split(",", 1)
    raw_last        = parts[0].strip()
    raw_first_field = parts[1].strip() if len(parts) > 1 else ""
    tracer_last     = _SUFFIX_RE.sub("", raw_last).strip()
    tracer_first    = raw_first_field.split()[0] if raw_first_field else ""

    if extra_last and tracer_last == extra_last:
        return True

    if not info:
        return False
    leg_last, leg_first = info

    # Last-name match (handles compound names like 'ZAMORA WILSON' vs 'WILSON')
    last_match = tracer_last == leg_last or tracer_last in leg_last.split()
    if not last_match:
        return False

    # Use nickname override first name if provided, otherwise legislator first
    eff_first = _NICKNAME_FIRST_NAMES.get((chamber, dist), leg_first)
    return (
        tracer_first == eff_first or                # exact match
        tracer_first.startswith(eff_first[:3]) or   # prefix (ROD/RODNEY, CHA/CHAD)
        eff_first in raw_first_field.split()         # leg first is a middle name in TRACER
    )


# ---------------------------------------------------------------------------
# Step 1 — Load CSV into district-keyed data structure
# ---------------------------------------------------------------------------

def load_races() -> dict:
    """
    Returns:
        { "Senate": { "3": { label, candidates: [...] } }, "House": { ... } }
    """
    races = {"Senate": {}, "House": {}}
    incumbents = load_incumbents()

    with open(CSV_FILE, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            chamber = row["Chamber"]        # "Senate" | "House"
            dist    = str(int(row["DistrictNumber"]))  # "1"..."65"

            if chamber not in races:
                continue

            if dist not in races[chamber]:
                races[chamber][dist] = {
                    "label":      row["DistrictLabel"],
                    "candidates": [],
                }

            races[chamber][dist]["candidates"].append({
                "name":      row["CandName"],
                "party":     row["Party"],
                "committee": row["CommitteeName"],
                "status":    row["CandidateStatus"],
                "raised":    float(row["MonetaryContributions"] or 0),
                "spent":     float(row["MonetaryExpenditures"]  or 0),
                "coh":       float(row["EndFundsOnHand"]        or 0),
                "beg":       float(row["BegFundsOnHand"]        or 0),
                "loans":     float(row["LoansReceived"]         or 0),
                "vsl":       row["AcceptedVSL"],
                "incumbent": is_incumbent(row["CandName"], chamber, dist, incumbents),
            })

    return races


def load_statewide_races() -> dict:
    """
    Returns:
        { "Governor": {"label": "Governor", "candidates": [...]}, ... }
    Returns {} gracefully if tracer_2026_statewide.csv does not exist yet.
    """
    if not STATEWIDE_CSV.exists():
        print("  (no statewide CSV — run: python3 scraper.py --statewide)")
        return {}

    offices: dict = {}
    with open(STATEWIDE_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            office = row["DistrictLabel"]
            if office not in offices:
                offices[office] = {"label": office, "candidates": []}
            offices[office]["candidates"].append({
                "name":      row["CandName"],
                "party":     row["Party"],
                "committee": row["CommitteeName"],
                "status":    row["CandidateStatus"],
                "raised":    float(row["MonetaryContributions"] or 0),
                "spent":     float(row["MonetaryExpenditures"]  or 0),
                "coh":       float(row["EndFundsOnHand"]        or 0),
                "loans":     float(row["LoansReceived"]         or 0),
                "vsl":       row["AcceptedVSL"],
                "incumbent": False,   # statewide executive incumbents not tracked
            })

    total = sum(len(v["candidates"]) for v in offices.values())
    print(f"  Statewide: {len(offices)} offices, {total} candidates")
    return offices


# ---------------------------------------------------------------------------
# Step 2 — Download / cache GeoJSON
# ---------------------------------------------------------------------------

def shapefile_to_geojson(chamber: str, precision: int = 5) -> dict:
    """
    Download the Census cartographic boundary shapefile zip for Colorado,
    parse it with pyshp, and return a minimal GeoJSON FeatureCollection.
    Results are cached to avoid re-downloading.
    """
    cache = GEOJSON_CACHE[chamber]
    if cache.exists():
        print(f"  Using cached {cache.name}")
        with open(cache, encoding="utf-8") as f:
            return json.load(f)

    url    = SHAPEFILE_URLS[chamber]
    dfield = DISTRICT_FIELD[chamber]

    print(f"  Downloading {chamber} shapefile from Census...")
    with urllib.request.urlopen(url, timeout=60) as r:
        zip_bytes = r.read()

    print(f"  Converting shapefile → GeoJSON...")
    features = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Find the .shp, .dbf, .shx files inside the zip
        names   = zf.namelist()
        shp_name = next(n for n in names if n.endswith(".shp"))
        dbf_name = next(n for n in names if n.endswith(".dbf"))
        shx_name = next(n for n in names if n.endswith(".shx"))

        shp = io.BytesIO(zf.read(shp_name))
        dbf = io.BytesIO(zf.read(dbf_name))
        shx = io.BytesIO(zf.read(shx_name))

        sf = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)
        field_names = [f[0] for f in sf.fields[1:]]  # skip deletion flag

        def round_coords(obj):
            if isinstance(obj, list):
                if obj and isinstance(obj[0], (int, float)):
                    return [round(obj[0], precision), round(obj[1], precision)]
                return [round_coords(c) for c in obj]
            return obj

        for rec, shape in zip(sf.records(), sf.shapes()):
            props = dict(zip(field_names, rec))
            dist_code = props.get(dfield, "")
            try:
                dist_num = str(int(dist_code))
            except (ValueError, TypeError):
                continue

            geom = shape.__geo_interface__
            features.append({
                "type": "Feature",
                "properties": {"district": dist_num},
                "geometry": {
                    "type":        geom["type"],
                    "coordinates": round_coords(geom["coordinates"]),
                },
            })

    result = {"type": "FeatureCollection", "features": features}

    with open(cache, "w", encoding="utf-8") as f:
        json.dump(result, f)
    print(f"  Cached → {cache.name}  ({len(features)} polygons)")
    return result


# ---------------------------------------------------------------------------
# Step 3 — City → district map
# ---------------------------------------------------------------------------

def load_places() -> dict:
    """
    Download Colorado incorporated places from Census TIGER/Line.
    Returns {name: [lon, lat]} using each place's bounding-box centroid.
    Cached to co_places.json.
    """
    if PLACES_CACHE.exists():
        print(f"  Using cached {PLACES_CACHE.name}")
        with open(PLACES_CACHE, encoding="utf-8") as f:
            return json.load(f)

    print("  Downloading Colorado places from Census...")
    with urllib.request.urlopen(PLACES_URL, timeout=60) as r:
        zip_bytes = r.read()

    places = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names    = zf.namelist()
        shp_name = next(n for n in names if n.endswith(".shp"))
        dbf_name = next(n for n in names if n.endswith(".dbf"))
        shx_name = next(n for n in names if n.endswith(".shx"))

        sf = shapefile.Reader(
            shp=io.BytesIO(zf.read(shp_name)),
            dbf=io.BytesIO(zf.read(dbf_name)),
            shx=io.BytesIO(zf.read(shx_name)),
        )
        field_names = [f[0] for f in sf.fields[1:]]

        for rec, shape in zip(sf.records(), sf.shapes()):
            props = dict(zip(field_names, rec))
            name  = props.get("NAME", "").strip()
            if not name:
                continue
            b   = shape.bbox           # [xmin, ymin, xmax, ymax]
            lon = (b[0] + b[2]) / 2
            lat = (b[1] + b[3]) / 2
            places[name] = [lon, lat]

    with open(PLACES_CACHE, "w", encoding="utf-8") as f:
        json.dump(places, f)
    print(f"  Cached → {PLACES_CACHE.name}  ({len(places)} places)")
    return places


def _point_in_polygon(px: float, py: float, feature: dict) -> bool:
    """Ray-casting point-in-polygon for a GeoJSON Feature (Polygon or MultiPolygon).
    Tests only outer rings, which is sufficient for city-centroid lookups."""
    geom  = feature["geometry"]
    gtype = geom["type"]
    rings = (
        [geom["coordinates"][0]]          if gtype == "Polygon"      else
        [part[0] for part in geom["coordinates"]] if gtype == "MultiPolygon" else
        []
    )
    for ring in rings:
        inside, j = False, len(ring) - 1
        for i, (xi, yi) in enumerate(ring):
            xj, yj = ring[j]
            if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
                inside = not inside
            j = i
        if inside:
            return True
    return False


def build_city_map(geojson_senate: dict, geojson_house: dict, places: dict) -> dict:
    """
    For each legislative district find which place centroids fall inside it.
    Returns {Senate: {dist_num: [city, ...]}, House: {dist_num: [city, ...]}}.
    """
    city_map    = {"Senate": {}, "House": {}}
    gj_chambers = {"Senate": geojson_senate, "House": geojson_house}

    for chamber, gj in gj_chambers.items():
        for feature in gj["features"]:
            dist   = feature["properties"]["district"]
            cities = sorted(
                name for name, (lon, lat) in places.items()
                if _point_in_polygon(lon, lat, feature)
            )
            if cities:
                city_map[chamber][dist] = cities

    return city_map


# ---------------------------------------------------------------------------
# Step 4 — Generate HTML
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Colorado 2026 Legislative Fundraising</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }

/* ── Layout ─────────────────────────────────────────── */
#app { display: flex; flex-direction: column; height: 100vh; }

#topbar {
  display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  padding: 8px 14px; background: #1e293b; color: #f1f5f9;
  font-size: 13px; flex-shrink: 0; min-height: 50px;
}
#topbar h1 { font-size: 14px; font-weight: 600; color: #e2e8f0; margin-right: 4px; }

#main { display: flex; flex: 1; overflow: hidden; }
#map  { flex: 1; }

/* ── Controls ───────────────────────────────────────── */
.chamber-group { display: flex; background: #334155; border-radius: 6px; overflow: hidden; }
.chamber-btn {
  border: none; padding: 5px 12px; cursor: pointer; font-size: 12px; font-weight: 500;
  color: #94a3b8; background: transparent; transition: all .15s;
}
.chamber-btn.active { background: #3b82f6; color: #fff; }
.chamber-btn:hover:not(.active) { background: #475569; color: #e2e8f0; }

#colorMode {
  border: 1px solid #475569; background: #334155; color: #e2e8f0;
  padding: 5px 8px; border-radius: 6px; font-size: 12px; cursor: pointer;
}

/* ── Legend ─────────────────────────────────────────── */
#legend { display: flex; align-items: center; gap: 6px; margin-left: auto; }
#legend-label { font-size: 11px; color: #94a3b8; }
#legend-bar {
  width: 120px; height: 10px; border-radius: 3px;
  background: linear-gradient(to right, #e02424, #f0f4ff, #1a56db);
}
#legend-ends { display: flex; justify-content: space-between; width: 120px; font-size: 10px; color: #64748b; }

/* ── Sidebar ─────────────────────────────────────────── */
#sidebar {
  width: 0; overflow: hidden; background: #fff;
  border-left: 1px solid #e2e8f0; transition: width .25s ease;
  display: flex; flex-direction: column;
}
#sidebar.open { width: 320px; }

#sidebar-inner { width: 320px; padding: 16px; overflow-y: auto; flex: 1; }

#sidebar-header {
  display: flex; justify-content: space-between; align-items: flex-start;
  margin-bottom: 14px;
}
#sidebar-title { font-size: 15px; font-weight: 700; color: #0f172a; }
#sidebar-close {
  border: none; background: none; cursor: pointer; color: #64748b;
  font-size: 20px; line-height: 1; padding: 0 4px;
}
#sidebar-close:hover { color: #0f172a; }

#sidebar-filters { display: none; gap: 6px; margin-bottom: 12px; flex-wrap: wrap; }
#sidebar.open #sidebar-filters { display: flex; }

.summary-row {
  display: flex; gap: 8px; margin-bottom: 14px;
}
.summary-card {
  flex: 1; background: #f8fafc; border: 1px solid #e2e8f0;
  border-radius: 8px; padding: 8px 10px;
}
.summary-card .label { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: .04em; }
.summary-card .value { font-size: 14px; font-weight: 700; color: #0f172a; margin-top: 2px; }

.section-title {
  font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .06em;
  color: #64748b; margin: 14px 0 8px;
}

/* Party bar */
.party-bar-wrap { margin-bottom: 14px; }
.party-bar-labels { display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 3px; }
.party-bar-labels .dem { color: #1a56db; font-weight: 600; }
.party-bar-labels .rep { color: #e02424; font-weight: 600; }
.party-bar-track {
  height: 10px; background: #e5e7eb; border-radius: 5px; overflow: hidden; position: relative;
}
.party-bar-fill {
  position: absolute; left: 0; top: 0; height: 100%;
  background: linear-gradient(to right, #1a56db, #93c5fd);
  border-radius: 5px; transition: width .4s ease;
}

/* Candidate cards */
.cand-card {
  position: relative; overflow: hidden;
  border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px 12px; margin-bottom: 8px;
}
.cand-card::after {
  content: ''; position: absolute; top: 0; right: 0;
  width: 0; height: 0; border-style: solid; border-width: 0 22px 22px 0;
}
.cand-card.vsl-yes::after { border-color: transparent #16a34a transparent transparent; }
.cand-card.vsl-no::after  { border-color: transparent #dc2626 transparent transparent; }
.cand-card.inactive {
  background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
  border-color: #e2e8f0; opacity: 0.72;
}
.cand-card.inactive .cand-name { color: #475569; }
.cand-card.inactive .stat-cell .val { color: #475569; }
.cand-name { font-size: 13px; font-weight: 600; color: #0f172a; }
.cand-committee { font-size: 10px; color: #94a3b8; margin-bottom: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.loan-note { font-size: 9px; color: #b45309; font-weight: 600; margin-top: 3px; letter-spacing: .02em; }
.cand-meta { display: flex; align-items: center; gap: 6px; margin: 3px 0 8px; }
.party-badge {
  font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 10px;
  text-transform: uppercase; letter-spacing: .04em;
}
.badge-Democratic  { background: #dbeafe; color: #1e40af; }
.badge-Republican  { background: #fee2e2; color: #991b1b; }
.badge-Unaffiliated{ background: #f3f4f6; color: #4b5563; }
.badge-Unknown     { background: #f3f4f6; color: #4b5563; }
.badge-default     { background: #f3f4f6; color: #4b5563; }
.cand-status { font-size: 10px; color: #9ca3af; }

.stat-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 6px; margin-bottom: 8px; }
.stat-cell .lbl { font-size: 9px; color: #9ca3af; text-transform: uppercase; letter-spacing: .04em; }
.stat-cell .val { font-size: 12px; font-weight: 600; color: #0f172a; }

.raised-bar-track {
  height: 6px; background: #e5e7eb; border-radius: 3px; overflow: hidden;
}
.raised-bar-spent { height: 100%; border-radius: 3px; }

/* ── Search ──────────────────────────────────────────── */
#search-wrap { position: relative; }
#search-input {
  border: 1px solid #475569; background: #334155; color: #e2e8f0;
  padding: 5px 10px; border-radius: 6px; font-size: 12px; width: 200px; outline: none;
}
#search-input::placeholder { color: #64748b; }
#search-input:focus { border-color: #3b82f6; }
#search-results {
  display: none; position: absolute; top: calc(100% + 4px); left: 0;
  width: 300px; background: #fff; border: 1px solid #e2e8f0;
  border-radius: 8px; box-shadow: 0 4px 16px rgba(0,0,0,.15);
  z-index: 9999; overflow: hidden; max-height: 340px; overflow-y: auto;
}
#search-results.visible { display: block; }
.search-result {
  padding: 9px 12px; cursor: pointer; border-bottom: 1px solid #f1f5f9;
  display: flex; align-items: flex-start; gap: 8px;
}
.search-result:last-child { border-bottom: none; }
.search-result:hover { background: #f8fafc; }
.sr-badge {
  font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 4px;
  background: #334155; color: #e2e8f0; flex-shrink: 0; margin-top: 1px;
}
.sr-main { flex: 1; min-width: 0; }
.sr-district { font-size: 12px; font-weight: 600; color: #0f172a; }
.sr-detail { font-size: 11px; color: #64748b; margin-top: 1px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.sr-empty { padding: 12px; text-align: center; color: #94a3b8; font-size: 12px; }

/* ── Statewide buttons (topbar) ──────────────────────── */
#statewide-divider { width: 1px; height: 24px; background: #475569; flex-shrink: 0; }
#statewide-group { display: flex; gap: 6px; flex-wrap: wrap; }
.statewide-btn {
  border: 1px solid #475569; background: transparent; color: #94a3b8;
  padding: 4px 10px; border-radius: 6px; cursor: pointer;
  font-size: 12px; font-weight: 500; transition: all .15s;
}
.statewide-btn:hover { background: #334155; color: #e2e8f0; }
.statewide-btn.active { background: #7c3aed; border-color: #7c3aed; color: #fff; }

/* ── Filter toggles (statewide panel + sidebar) ──────── */
.filter-toggle {
  border: 1px solid #e2e8f0; background: #fff; color: #64748b;
  padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 11px;
  font-weight: 500; transition: all .15s; white-space: nowrap;
}
.filter-toggle:hover { background: #f1f5f9; color: #0f172a; }
.filter-toggle.active { background: #0f172a; border-color: #0f172a; color: #fff; }

/* ── Statewide panel ──────────────────────────────────── */
#statewide-panel {
  flex: 1; display: flex; flex-direction: column; overflow: hidden;
  background: #f8fafc;
}
#statewide-header {
  display: flex; align-items: center; gap: 14px; flex-shrink: 0;
  padding: 10px 20px; background: #fff; border-bottom: 1px solid #e2e8f0;
}
#statewide-back {
  border: 1px solid #e2e8f0; background: #fff; color: #475569;
  padding: 5px 12px; border-radius: 6px; cursor: pointer; font-size: 12px;
  font-weight: 500; transition: all .15s; white-space: nowrap;
}
#statewide-back:hover { background: #f1f5f9; color: #0f172a; }
#statewide-title { font-size: 16px; font-weight: 700; color: #0f172a; flex: 1; }
#statewide-metric {
  border: 1px solid #e2e8f0; background: #fff; color: #334155;
  padding: 5px 8px; border-radius: 6px; font-size: 12px; cursor: pointer;
}
#statewide-chart-wrap { flex: 1; overflow-y: auto; padding: 24px 32px; }
#statewide-chart { display: flex; flex-direction: column; gap: 12px; max-width: 900px; }

/* ── Bar chart rows ───────────────────────────────────── */
.chart-row {
  display: grid; grid-template-columns: 200px 1fr 90px;
  align-items: center; gap: 12px;
}
.chart-label {
  text-align: right; font-size: 13px; color: #1e293b;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.chart-sublabel { display: block; font-size: 10px; color: #94a3b8; margin-top: 1px; }
.chart-bar-track { height: 28px; background: #e5e7eb; border-radius: 4px; overflow: hidden; }
.chart-bar-fill { height: 100%; border-radius: 4px; transition: width .4s ease; }
.chart-bar-fill.party-D     { background: #2563eb; }
.chart-bar-fill.party-R     { background: #dc2626; }
.chart-bar-fill.party-other { background: #6366f1; }
.chart-value { font-size: 13px; font-weight: 600; color: #0f172a; white-space: nowrap; }
.chart-inactive { opacity: 0.45; }
.chart-section-header {
  font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em;
  color: #94a3b8; padding: 4px 0 2px; border-bottom: 1px solid #e5e7eb; margin-bottom: 4px;
  grid-column: 1 / -1;
}

/* Leaflet overrides */
.district-label {
  background: transparent; border: none; box-shadow: none;
  font-size: 9px; font-weight: 700; color: #1e293b;
  text-shadow: 0 0 3px #fff, 0 0 3px #fff;
  pointer-events: none;
}
.leaflet-tooltip.district-label::before { display: none; }

/* ── Incumbent badge ──────────────────────────────────── */
.incumbent-badge {
  display: inline-flex; align-items: center; justify-content: center;
  width: 15px; height: 15px; border-radius: 3px;
  background: #7c3aed; color: #fff; font-size: 9px; font-weight: 700;
  margin-left: 5px; vertical-align: middle; flex-shrink: 0; cursor: default;
}

/* ── Topbar icon buttons ────────────────────────────────── */
.icon-btn {
  border: 1px solid #475569; background: transparent; color: #94a3b8;
  padding: 5px 8px; border-radius: 6px; cursor: pointer; font-size: 14px;
  line-height: 1; transition: all .15s;
}
.icon-btn:hover { background: #334155; color: #e2e8f0; }

/* ── Hide VSL indicator when toggled off ──────────────── */
body.vsl-hidden .cand-card::after { display: none; }

/* ── Modals ───────────────────────────────────────────── */
.modal-overlay {
  display: none; position: fixed; inset: 0; background: rgba(0,0,0,.45);
  z-index: 10000; align-items: center; justify-content: center;
}
.modal-overlay.open { display: flex; }
.modal-box {
  background: #fff; border-radius: 10px; padding: 20px 24px;
  max-width: 540px; width: 92%; max-height: 80vh; overflow-y: auto;
  box-shadow: 0 8px 40px rgba(0,0,0,.25);
}
.modal-header {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 14px;
}
.modal-title { font-size: 15px; font-weight: 700; color: #0f172a; }
.modal-close {
  border: none; background: none; cursor: pointer; color: #64748b;
  font-size: 22px; line-height: 1; padding: 0 4px;
}
.modal-close:hover { color: #0f172a; }
.modal-section { margin-bottom: 14px; }
.modal-section h3 {
  font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .06em; color: #64748b; margin-bottom: 6px;
}
.modal-section p, .modal-section li { font-size: 13px; color: #374151; line-height: 1.55; }
.modal-section ul { padding-left: 16px; }
.modal-section li { margin-bottom: 5px; }
.toggle-row {
  display: flex; align-items: flex-start; gap: 10px; cursor: pointer;
  padding: 8px 0; border-bottom: 1px solid #f1f5f9;
}
.toggle-row input[type=checkbox] { margin-top: 2px; cursor: pointer; }
.toggle-row .toggle-label { font-size: 13px; color: #0f172a; font-weight: 500; }
.toggle-row .toggle-desc { font-size: 11px; color: #64748b; margin-top: 2px; }
</style>
</head>
<body>
<div id="app">

  <div id="topbar">
    <h1>Colorado 2026 Legislative Fundraising</h1>

    <div class="chamber-group">
      <button class="chamber-btn active" data-chamber="House">House</button>
      <button class="chamber-btn" data-chamber="Senate">Senate</button>
    </div>

    <div id="statewide-divider"></div>
    <div id="statewide-group"></div>

    <select id="colorMode">
      <option value="raised_margin">Raised Margin (D vs R)</option>
      <option value="coh_margin">Cash on Hand Margin</option>
      <option value="competitiveness">Competitiveness</option>
      <option value="total_raised">Total Raised</option>
      <option value="burn_rate">Burn Rate</option>
      <option value="loan_reliance">Loan Reliance</option>
    </select>

    <div id="search-wrap">
      <input id="search-input" type="text" placeholder="Search candidate or city…" autocomplete="off">
      <div id="search-results"></div>
    </div>

    <button class="icon-btn" id="info-btn" title="How this map works">ℹ</button>
    <button class="icon-btn" id="settings-btn" title="Settings">⚙</button>

    <div id="legend">
      <span id="legend-label">← R leads · D leads →</span>
      <div>
        <div id="legend-bar"></div>
        <div id="legend-ends"><span id="leg-left">R</span><span id="leg-right">D</span></div>
      </div>
    </div>
  </div>

  <div id="main">
    <div id="map"></div>
    <div id="sidebar">
      <div id="sidebar-inner">
        <div id="sidebar-header">
          <div id="sidebar-title"></div>
          <button id="sidebar-close">×</button>
        </div>
        <div id="sidebar-filters">
          <button class="filter-toggle" data-filter="inactive">Hide inactive</button>
          <button class="filter-toggle" data-filter="empty">Hide $0</button>
        </div>
        <div id="sidebar-body"></div>
      </div>
    </div>
  </div>

  <div id="statewide-panel" style="display:none">
    <div id="statewide-header">
      <button id="statewide-back">← Map</button>
      <h2 id="statewide-title"></h2>
      <select id="statewide-metric">
        <option value="raised">Total Raised</option>
        <option value="coh">Cash on Hand</option>
        <option value="spent">Total Spent</option>
        <option value="burn_rate">Burn Rate</option>
        <option value="loan_reliance">Loan Reliance</option>
      </select>
      <button class="filter-toggle" data-filter="inactive">Hide inactive</button>
      <button class="filter-toggle" data-filter="empty">Hide $0</button>
    </div>
    <div id="statewide-chart-wrap">
      <div id="statewide-chart"></div>
    </div>
  </div>

</div>

<!-- ── Info Modal ─────────────────────────────────────── -->
<div class="modal-overlay" id="info-modal">
  <div class="modal-box">
    <div class="modal-header">
      <div class="modal-title">How This Map Works</div>
      <button class="modal-close" id="info-close">×</button>
    </div>
    <div class="modal-section">
      <h3>Data Source</h3>
      <p>All financial data comes from <strong>TRACER</strong>, Colorado's campaign finance reporting system (<em>tracer.sos.colorado.gov</em>), for the 2026 November General Election cycle.</p>
    </div>
    <div class="modal-section">
      <h3>Map Color Modes</h3>
      <ul>
        <li><strong>Raised Margin</strong> — Total contributions raised by Democrats vs. Republicans. Blue = Dem advantage, Red = Rep advantage.</li>
        <li><strong>Cash on Hand Margin</strong> — Same comparison using end-of-period cash available to spend.</li>
        <li><strong>Competitiveness</strong> — How close the fundraising gap is between parties. Brighter amber = tighter race.</li>
        <li><strong>Total Raised</strong> — Combined contributions across all candidates in the district. Darker = more total money.</li>
        <li><strong>Burn Rate</strong> — Ratio of spending to fundraising. Red = spending most of what was raised.</li>
        <li><strong>Loan Reliance</strong> — Share of funds coming from candidate self-loans. Amber = heavy self-funding.</li>
      </ul>
    </div>
    <div class="modal-section">
      <h3>Candidate Card</h3>
      <ul>
        <li><strong>Raised</strong> — Total monetary contributions received this cycle.</li>
        <li><strong>Spent</strong> — Total monetary expenditures this cycle.</li>
        <li><strong>Cash on Hand</strong> — End-of-period available funds (beginning balance + raised − spent).</li>
        <li><strong>Spend bar</strong> — Visual indicator of what portion of raised funds have been spent.</li>
        <li><strong>Loan note</strong> — Candidate has self-funded part of their campaign via personal loans.</li>
        <li><strong>Colored corner triangle</strong> — Green = accepted Volunteer Spending Limits (VSL); Red = declined. VSL is a voluntary cap on spending in exchange for matching public funds eligibility.</li>
        <li><strong>★ purple badge</strong> — Incumbent: candidate currently holds this seat (indicated by non-zero beginning balance from prior cycle).</li>
      </ul>
    </div>
    <div class="modal-section">
      <h3>Sidebar Filters</h3>
      <ul>
        <li><strong>Hide inactive</strong> — Removes terminated and withdrawn candidates from view.</li>
        <li><strong>Hide $0</strong> — Removes candidates who have raised and hold $0.</li>
      </ul>
    </div>
  </div>
</div>

<!-- ── Settings Modal ──────────────────────────────────── -->
<div class="modal-overlay" id="settings-modal">
  <div class="modal-box">
    <div class="modal-header">
      <div class="modal-title">Settings</div>
      <button class="modal-close" id="settings-close">×</button>
    </div>
    <label class="toggle-row">
      <input type="checkbox" id="vsl-toggle" checked>
      <div>
        <div class="toggle-label">Show VSL indicator</div>
        <div class="toggle-desc">Displays a colored corner triangle on candidate cards showing whether they accepted Volunteer Spending Limits.</div>
      </div>
    </label>
  </div>
</div>

<script>
// ── Embedded data ─────────────────────────────────────
const RACES     = __RACES_JSON__;
const STATEWIDE = __STATEWIDE_JSON__;
const GEOJSON   = __GEOJSON_JSON__;
const CITY_MAP  = __CITY_MAP_JSON__;

// ── State ─────────────────────────────────────────────
let activeChamber         = 'House';
let activeMode            = 'raised_margin';
let activeLayer           = null;
let labelLayer            = null;
let selectedDist          = null;
const districtLayerMap    = {};  // 'House:4' → Leaflet layer
let activeStatewide       = null;   // null = map view; string = office name
let activeStatewideMetric = 'raised';
let filterHideInactive    = false;
let filterHideEmpty       = false;

// ── Leaflet setup ─────────────────────────────────────
const map = L.map('map', { zoomControl: true }).setView([38.95, -105.55], 7);

L.tileLayer('https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png', {
  attribution: '© OpenStreetMap © CARTO',
  subdomains: 'abcd', maxZoom: 19
}).addTo(map);

// ── Colour helpers ────────────────────────────────────
function lerpColor(c1, c2, t) {
  t = Math.max(0, Math.min(1, t));
  const h = c => [parseInt(c.slice(1,3),16), parseInt(c.slice(3,5),16), parseInt(c.slice(5,7),16)];
  const [r1,g1,b1] = h(c1), [r2,g2,b2] = h(c2);
  return `rgb(${Math.round(r1+(r2-r1)*t)},${Math.round(g1+(g2-g1)*t)},${Math.round(b1+(b2-b1)*t)})`;
}
function marginColor(m) {
  return m >= 0 ? lerpColor('#f0f4ff','#1a56db', m)
                : lerpColor('#f0f4ff','#e02424',-m);
}
function sum(arr, k) { return arr.reduce((s,c) => s + (c[k]||0), 0); }

// Pre-compute per-chamber max total raised for normalisation
const DISTRICT_MAX = {};
const LOAN_MAX = {};
['Senate','House'].forEach(ch => {
  DISTRICT_MAX[ch] = Math.max(1, ...Object.values(RACES[ch]||{})
    .map(d => sum(d.candidates,'raised')));
  LOAN_MAX[ch] = Math.max(1, ...Object.values(RACES[ch]||{})
    .map(d => sum(d.candidates,'loans')));
});

function districtColor(chamber, distNum, mode) {
  const data = RACES[chamber]?.[String(distNum)];
  if (!data) return '#e2e8f0';   // district not up this cycle

  const cands = data.candidates;
  const dems  = cands.filter(c => c.party === 'Democratic');
  const reps  = cands.filter(c => c.party === 'Republican');

  const Dr = sum(dems,'raised'), Rr = sum(reps,'raised');
  const Dc = sum(dems,'coh'),    Rc = sum(reps,'coh');

  switch (mode) {
    case 'raised_margin':
      if (Dr+Rr === 0) return '#d1d5db';
      return marginColor((Dr-Rr)/(Dr+Rr));

    case 'coh_margin':
      if (Dc+Rc === 0) return '#d1d5db';
      return marginColor((Dc-Rc)/(Dc+Rc));

    case 'competitiveness': {
      if (Dr+Rr === 0) return '#d1d5db';
      const score = 1 - Math.abs((Dr-Rr)/(Dr+Rr));
      return lerpColor('#e5e7eb','#f59e0b', score);
    }
    case 'total_raised': {
      const t = sum(cands,'raised') / DISTRICT_MAX[chamber];
      return lerpColor('#dbeafe','#1e3a8a', t);
    }
    case 'burn_rate': {
      const r = sum(cands,'raised');
      if (r === 0) return '#d1d5db';
      const rate = Math.min(sum(cands,'spent') / r, 1);
      return lerpColor('#d1fae5','#991b1b', rate);
    }
    case 'loan_reliance': {
      const l = sum(cands,'loans');
      if (l === 0) return '#f8fafc';
      return lerpColor('#fef9c3','#b45309', l / LOAN_MAX[chamber]);
    }
  }
  return '#d1d5db';
}

// ── Legend update ─────────────────────────────────────
const LEGEND_CONFIGS = {
  raised_margin:   { bar: 'linear-gradient(to right,#e02424,#f0f4ff,#1a56db)', label:'← R leads · D leads →', l:'R', r:'D' },
  coh_margin:      { bar: 'linear-gradient(to right,#e02424,#f0f4ff,#1a56db)', label:'← R CoH lead · D CoH lead →', l:'R', r:'D' },
  competitiveness: { bar: 'linear-gradient(to right,#e5e7eb,#f59e0b)',          label:'Competitiveness', l:'Safe', r:'Toss-up' },
  total_raised:    { bar: 'linear-gradient(to right,#dbeafe,#1e3a8a)',          label:'Total Raised', l:'$0', r:'Max' },
  burn_rate:       { bar: 'linear-gradient(to right,#d1fae5,#991b1b)',          label:'Burn Rate', l:'Low', r:'High' },
  loan_reliance:   { bar: 'linear-gradient(to right,#fef9c3,#b45309)',          label:'Loan Reliance', l:'None', r:'High' },
};
function updateLegend() {
  const cfg = LEGEND_CONFIGS[activeMode];
  document.getElementById('legend-bar').style.background   = cfg.bar;
  document.getElementById('legend-label').textContent       = cfg.label;
  document.getElementById('leg-left').textContent           = cfg.l;
  document.getElementById('leg-right').textContent          = cfg.r;
}

// ── Polygon centroid helpers (Issue #1: accurate label placement) ──────────
function polygonCentroid(ring) {
  let cx = 0, cy = 0, area = 0;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const f = ring[i][0] * ring[j][1] - ring[j][0] * ring[i][1];
    cx += (ring[i][0] + ring[j][0]) * f;
    cy += (ring[i][1] + ring[j][1]) * f;
    area += f;
  }
  area /= 2;
  if (Math.abs(area) < 1e-10) {
    return L.latLng(
      ring.reduce((s, p) => s + p[1], 0) / ring.length,
      ring.reduce((s, p) => s + p[0], 0) / ring.length
    );
  }
  return L.latLng(cy / (6 * area), cx / (6 * area));
}

function featureCentroid(feature) {
  const geom = feature.geometry;
  if (geom.type === 'Polygon') {
    return polygonCentroid(geom.coordinates[0]);
  }
  if (geom.type === 'MultiPolygon') {
    let maxArea = 0, bestRing = null;
    for (const poly of geom.coordinates) {
      const ring = poly[0];
      let area = 0;
      for (let i = 0, j = ring.length - 1; i < ring.length; j = i++)
        area += (ring[j][0] + ring[i][0]) * (ring[j][1] - ring[i][1]);
      area = Math.abs(area / 2);
      if (area > maxArea) { maxArea = area; bestRing = ring; }
    }
    return bestRing ? polygonCentroid(bestRing) : null;
  }
  return null;
}

// ── Map layer ─────────────────────────────────────────
function buildLayer(chamber) {
  if (activeLayer)  { map.removeLayer(activeLayer); activeLayer = null; }
  if (labelLayer)   { map.removeLayer(labelLayer);  labelLayer  = null; }

  // Issue #4: only render districts that have candidates this cycle
  const rawGj = GEOJSON[chamber];
  const filteredFeatures = rawGj.features.filter(f =>
    RACES[chamber]?.[f.properties.district]
  );
  const gj = { ...rawGj, features: filteredFeatures };

  activeLayer = L.geoJSON(gj, {
    style: feat => ({
      fillColor:   districtColor(chamber, feat.properties.district, activeMode),
      fillOpacity: 0.75,
      color:       '#475569',
      weight:      0.8,
    }),
    onEachFeature: (feat, layer) => {
      const dist = feat.properties.district;
      districtLayerMap[`${chamber}:${dist}`] = layer;
      layer.on({
        click:     () => openSidebar(chamber, dist),
        mouseover: e  => { e.target.setStyle({ weight: 2, color: '#0f172a' }); },
        mouseout:  e  => { activeLayer.resetStyle(e.target); },
      });
    },
  }).addTo(map);

  // Issue #1: use true polygon centroid for district labels
  labelLayer = L.layerGroup().addTo(map);
  activeLayer.eachLayer(layer => {
    const dist = layer.feature.properties.district;
    const center = featureCentroid(layer.feature) || layer.getBounds().getCenter();
    L.tooltip({ permanent: true, direction: 'center', className: 'district-label', interactive: false })
      .setContent(dist)
      .setLatLng(center)
      .addTo(labelLayer);
  });
}

function refreshColors() {
  if (!activeLayer) return;
  activeLayer.setStyle(feat => ({
    fillColor:   districtColor(activeChamber, feat.properties.district, activeMode),
    fillOpacity: 0.75,
    color:       '#475569',
    weight:      0.8,
  }));
}

// ── Sidebar ───────────────────────────────────────────
function fmt(n) {
  if (n === 0) return '$0';
  if (n >= 1_000_000) return '$' + (n/1_000_000).toFixed(2) + 'M';
  if (n >= 1_000)     return '$' + (n/1_000).toFixed(1) + 'K';
  return '$' + n.toFixed(0);
}

function partyClass(p) {
  return ['Democratic','Republican','Unaffiliated','Unknown'].includes(p) ? `badge-${p}` : 'badge-default';
}

function toTitleCase(s) {
  return s.replace(/\b\w+/g, w =>
    /^(II|III|IV|VI|VII|VIII|IX|JR|SR)$/i.test(w)
      ? w.toUpperCase()
      : w[0].toUpperCase() + w.slice(1).toLowerCase()
  );
}
function fmtName(raw) {
  const comma = raw.indexOf(',');
  if (comma === -1) return toTitleCase(raw);
  return toTitleCase(raw.slice(comma + 1).trim() + ' ' + raw.slice(0, comma).trim());
}

function openSidebar(chamber, distNum) {
  selectedDist = distNum;
  const data   = RACES[chamber]?.[String(distNum)];
  const sidebar = document.getElementById('sidebar');
  const title   = document.getElementById('sidebar-title');
  const body    = document.getElementById('sidebar-body');

  if (!data) {
    title.textContent = `${chamber} District ${distNum}`;
    body.innerHTML    = '<p style="color:#64748b;font-size:13px;margin-top:8px;">No candidates filed for 2026.</p>';
    sidebar.classList.add('open');
    return;
  }

  title.textContent = data.label;

  const cands    = data.candidates;
  const dems     = cands.filter(c => c.party === 'Democratic');
  const reps     = cands.filter(c => c.party === 'Republican');
  const Dr       = sum(dems,'raised'), Rr = sum(reps,'raised');
  const totalR   = sum(cands,'raised');
  const totalS   = sum(cands,'spent');
  const totalCoH = sum(cands,'coh');

  // Party bar pct (D share of D+R only)
  const partyTotal = Dr + Rr;
  const dPct = partyTotal > 0 ? (Dr / partyTotal * 100).toFixed(1) : 50;

  // Filter + sort: Active first, then by raised desc
  let filtered = [...cands];
  if (filterHideInactive) filtered = filtered.filter(c => c.status === 'Active');
  if (filterHideEmpty)    filtered = filtered.filter(c => c.raised > 0 || c.coh > 0);
  const sorted = filtered.sort((a,b) => {
    if (a.status === b.status) return b.raised - a.raised;
    return a.status === 'Active' ? -1 : 1;
  });

  const PARTY_COLOR = { Democratic: '#1a56db', Republican: '#e02424' };
  const candCards = sorted.map(c => {
    const spentPct       = c.raised > 0 ? Math.min(c.spent / c.raised * 100, 100).toFixed(0) : 0;
    const barColor       = PARTY_COLOR[c.party] || '#6366f1';
    const inactive       = c.status !== 'Active';
    const vslClass       = c.vsl === 'Yes' ? 'vsl-yes' : 'vsl-no';
    const incumbentBadge = c.incumbent
      ? '<span class="incumbent-badge" title="Incumbent">★</span>' : '';
    return `
    <div class="cand-card${inactive ? ' inactive' : ''} ${vslClass}">
      <div class="cand-name">${fmtName(c.name)}${incumbentBadge}</div>
      ${c.committee ? `<div class="cand-committee">${c.committee}</div>` : ''}
      <div class="cand-meta">
        <span class="party-badge ${partyClass(c.party)}">${c.party}</span>
        <span class="cand-status">${c.status}</span>
      </div>
      <div class="stat-grid">
        <div class="stat-cell"><div class="lbl">Raised</div><div class="val">${fmt(c.raised)}</div></div>
        <div class="stat-cell"><div class="lbl">Spent</div><div class="val">${fmt(c.spent)}</div></div>
        <div class="stat-cell"><div class="lbl">Cash on Hand</div><div class="val">${fmt(c.coh)}</div></div>
      </div>
      <div class="raised-bar-track">
        <div class="raised-bar-spent" style="width:${spentPct}%;background:${barColor}"></div>
      </div>
      ${c.loans > 0 ? `<div class="loan-note">+ ${fmt(c.loans)} loan</div>` : ''}
    </div>`;
  }).join('');

  body.innerHTML = `
    <div class="summary-row">
      <div class="summary-card"><div class="label">Total Raised</div><div class="value">${fmt(totalR)}</div></div>
      <div class="summary-card"><div class="label">Total Spent</div><div class="value">${fmt(totalS)}</div></div>
      <div class="summary-card"><div class="label">Cash on Hand</div><div class="value">${fmt(totalCoH)}</div></div>
    </div>

    ${partyTotal > 0 ? `
    <div class="party-bar-wrap">
      <div class="party-bar-labels">
        <span class="dem">Dem ${fmt(Dr)}</span>
        <span class="rep">${fmt(Rr)} Rep</span>
      </div>
      <div class="party-bar-track">
        <div class="party-bar-fill" style="width:${dPct}%"></div>
      </div>
    </div>` : ''}

    <div class="section-title">Candidates (${cands.length})${dems.length ? ` <span style="color:#1a56db;font-weight:700">${dems.length}D</span>` : ''}${reps.length ? ` <span style="color:#e02424;font-weight:700">${reps.length}R</span>` : ''}${cands.length - dems.length - reps.length ? ` <span style="color:#9ca3af">${cands.length - dems.length - reps.length} other</span>` : ''}</div>
    ${candCards}
  `;

  sidebar.classList.add('open');
}

document.getElementById('sidebar-close').addEventListener('click', () => {
  document.getElementById('sidebar').classList.remove('open');
  selectedDist = null;
});

// ── Statewide panel ───────────────────────────────────
function showStatewidePanel(office) {
  activeStatewide = office;
  document.querySelectorAll('.chamber-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.statewide-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.office === office));
  document.getElementById('main').style.display            = 'none';
  document.getElementById('statewide-panel').style.display = 'flex';
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('colorMode').style.display    = 'none';
  document.getElementById('search-wrap').style.display  = 'none';
  document.getElementById('legend').style.display       = 'none';
  document.getElementById('statewide-title').textContent = office;
  renderStatewideChart(office, activeStatewideMetric);
}

function hideStatewidePanel() {
  activeStatewide = null;
  document.getElementById('main').style.display            = 'flex';
  document.getElementById('statewide-panel').style.display = 'none';
  document.getElementById('colorMode').style.display    = '';
  document.getElementById('search-wrap').style.display  = '';
  document.getElementById('legend').style.display       = '';
  document.querySelectorAll('.statewide-btn').forEach(b => b.classList.remove('active'));
  document.querySelector(`.chamber-btn[data-chamber="${activeChamber}"]`).classList.add('active');
  map.invalidateSize();
}

function getMetricValue(cand, metric) {
  if (metric === 'raised')        return cand.raised;
  if (metric === 'coh')           return cand.coh;
  if (metric === 'spent')         return cand.spent;
  if (metric === 'burn_rate')     return cand.raised > 0 ? cand.spent / cand.raised : 0;
  if (metric === 'loan_reliance') return (cand.raised + cand.loans) > 0
                                    ? cand.loans / (cand.raised + cand.loans) : 0;
  return 0;
}

function renderStatewideChart(office, metric) {
  const data  = STATEWIDE[office];
  if (!data) return;
  const chart    = document.getElementById('statewide-chart');
  const isMoney  = !['burn_rate', 'loan_reliance'].includes(metric);
  let candidates = data.candidates;
  if (filterHideInactive) candidates = candidates.filter(c => c.status === 'Active');
  if (filterHideEmpty)    candidates = candidates.filter(c => c.raised > 0 || c.coh > 0);
  const withVals = candidates.map(c => ({ ...c, metricVal: getMetricValue(c, metric) }));
  const maxVal   = Math.max(1, ...withVals.map(c => c.metricVal));

  const PARTY_ORDER = { Democratic: 0, Republican: 1 };
  const sortFn = (a, b) => {
    const po = (PARTY_ORDER[a.party] ?? 2) - (PARTY_ORDER[b.party] ?? 2);
    return po !== 0 ? po : b.metricVal - a.metricVal;
  };
  const active   = withVals.filter(c => c.status === 'Active').sort(sortFn);
  const inactive = withVals.filter(c => c.status !== 'Active').sort(sortFn);

  function barClass(party) {
    if (party === 'Democratic') return 'party-D';
    if (party === 'Republican') return 'party-R';
    return 'party-other';
  }
  function fmtMetric(val) {
    return isMoney ? fmt(val) : (val * 100).toFixed(1) + '%';
  }
  function renderGroup(arr, headerText) {
    if (!arr.length) return '';
    const header = `<div class="chart-row"><div class="chart-section-header">${headerText}</div></div>`;
    const rows = arr.map(c => {
      const pct  = maxVal > 0 ? (c.metricVal / maxVal * 100).toFixed(1) : 0;
      const cls      = c.status !== 'Active' ? ' chart-inactive' : '';
      const sub      = c.committee ? `<span class="chart-sublabel">${c.committee}</span>` : '';
      const incBadge = c.incumbent ? '<span class="incumbent-badge" title="Incumbent">★</span>' : '';
      return `<div class="chart-row${cls}">
        <div class="chart-label">${fmtName(c.name)}${incBadge}${sub}</div>
        <div class="chart-bar-track">
          <div class="chart-bar-fill ${barClass(c.party)}" style="width:${pct}%"></div>
        </div>
        <div class="chart-value">${fmtMetric(c.metricVal)}</div>
      </div>`;
    }).join('');
    return header + rows;
  }

  chart.innerHTML = renderGroup(active, 'Active Candidates')
                  + renderGroup(inactive, 'Terminated / Withdrawn');
}

(function initStatewideButtons() {
  const swGroup = document.getElementById('statewide-group');
  const offices = Object.keys(STATEWIDE);
  if (!offices.length) {
    document.getElementById('statewide-divider').style.display = 'none';
    return;
  }
  offices.forEach(office => {
    const btn = document.createElement('button');
    btn.className = 'statewide-btn';
    btn.dataset.office = office;
    btn.textContent = office;
    btn.addEventListener('click', () => showStatewidePanel(office));
    swGroup.appendChild(btn);
  });
})();

document.getElementById('statewide-back').addEventListener('click', hideStatewidePanel);
document.getElementById('statewide-metric').addEventListener('change', e => {
  activeStatewideMetric = e.target.value;
  if (activeStatewide) renderStatewideChart(activeStatewide, activeStatewideMetric);
});

document.querySelectorAll('.filter-toggle[data-filter]').forEach(btn => {
  btn.addEventListener('click', () => {
    if (btn.dataset.filter === 'inactive') filterHideInactive = !filterHideInactive;
    else                                   filterHideEmpty     = !filterHideEmpty;
    // Sync all matching buttons
    document.querySelectorAll(`.filter-toggle[data-filter="${btn.dataset.filter}"]`)
      .forEach(b => b.classList.toggle('active', btn.dataset.filter === 'inactive'
                                                 ? filterHideInactive : filterHideEmpty));
    // Re-render wherever applicable
    if (activeStatewide) renderStatewideChart(activeStatewide, activeStatewideMetric);
    if (selectedDist)    openSidebar(activeChamber, selectedDist);
  });
});

// ── Controls ──────────────────────────────────────────
document.querySelectorAll('.chamber-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    if (activeStatewide) hideStatewidePanel();
    if (btn.dataset.chamber === activeChamber) return;
    document.querySelectorAll('.chamber-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    activeChamber = btn.dataset.chamber;
    document.getElementById('sidebar').classList.remove('open');
    selectedDist = null;
    buildLayer(activeChamber);
  });
});

document.getElementById('colorMode').addEventListener('change', e => {
  activeMode = e.target.value;
  updateLegend();
  refreshColors();
});

// ── Search ────────────────────────────────────────────
const searchIndex = [];
['Senate','House'].forEach(ch => {
  Object.entries(RACES[ch]||{}).forEach(([dist, data]) => {
    const cities = CITY_MAP[ch]?.[dist] || [];
    const names  = data.candidates.map(c => c.name);
    const comms  = data.candidates.map(c => c.committee).filter(Boolean);
    searchIndex.push({
      chamber: ch, distNum: dist, label: data.label, cities,
      text: [data.label, ...cities, ...names, ...comms].join(' ').toLowerCase(),
    });
  });
});

function doSearch(q) {
  if (!q.trim()) return [];
  const lq = q.toLowerCase();
  return searchIndex.filter(e => e.text.includes(lq)).slice(0, 8);
}

function renderResults(results, query) {
  const el  = document.getElementById('search-results');
  const lq  = query.toLowerCase();
  if (!results.length) {
    el.innerHTML = '<div class="sr-empty">No results</div>';
    el.classList.add('visible');
    return;
  }
  el.innerHTML = results.map(r => {
    const cands = RACES[r.chamber][r.distNum].candidates;
    const matchedCities = r.cities.filter(c => c.toLowerCase().includes(lq));
    const matchedCands  = cands.filter(c =>
      c.name.toLowerCase().includes(lq) || (c.committee||'').toLowerCase().includes(lq));
    const detail = matchedCities.length
      ? matchedCities.slice(0,3).join(', ')
      : matchedCands.length
        ? matchedCands.slice(0,2).map(c => fmtName(c.name)).join(', ')
        : (r.cities.slice(0,2).join(', ') || cands.slice(0,1).map(c => fmtName(c.name)).join(''));
    return `<div class="search-result" data-chamber="${r.chamber}" data-dist="${r.distNum}">
      <span class="sr-badge">${r.chamber === 'Senate' ? 'S' : 'H'}</span>
      <div class="sr-main">
        <div class="sr-district">${r.label}</div>
        <div class="sr-detail">${detail}</div>
      </div>
    </div>`;
  }).join('');
  el.classList.add('visible');
  el.querySelectorAll('.search-result').forEach(item => {
    item.addEventListener('mousedown', e => {
      e.preventDefault();  // prevent blur firing before click
      goToDistrict(item.dataset.chamber, item.dataset.dist);
    });
  });
}

function goToDistrict(chamber, distNum) {
  // Switch chamber if needed (synchronous — buildLayer runs inline)
  if (chamber !== activeChamber) {
    document.querySelector(`.chamber-btn[data-chamber="${chamber}"]`).click();
  }
  const layer = districtLayerMap[`${chamber}:${distNum}`];
  if (layer) map.fitBounds(layer.getBounds(), { padding: [40, 40] });
  openSidebar(chamber, distNum);
  // Clear search
  document.getElementById('search-input').value = '';
  document.getElementById('search-results').classList.remove('visible');
}

(function () {
  const input   = document.getElementById('search-input');
  const results = document.getElementById('search-results');
  let timer;
  input.addEventListener('input', () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      const q = input.value;
      if (!q.trim()) { results.classList.remove('visible'); return; }
      renderResults(doSearch(q), q);
    }, 150);
  });
  input.addEventListener('blur', () => {
    setTimeout(() => results.classList.remove('visible'), 150);
  });
  input.addEventListener('focus', () => {
    if (input.value.trim()) renderResults(doSearch(input.value), input.value);
  });
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') { input.value = ''; results.classList.remove('visible'); input.blur(); }
  });
})();

// ── Modals (Info & Settings) ───────────────────────────
function openModal(id)  { document.getElementById(id).classList.add('open'); }
function closeModal(id) { document.getElementById(id).classList.remove('open'); }

document.getElementById('info-btn').addEventListener('click', () => openModal('info-modal'));
document.getElementById('info-close').addEventListener('click', () => closeModal('info-modal'));
document.getElementById('settings-btn').addEventListener('click', () => openModal('settings-modal'));
document.getElementById('settings-close').addEventListener('click', () => closeModal('settings-modal'));

['info-modal', 'settings-modal'].forEach(id => {
  document.getElementById(id).addEventListener('click', e => {
    if (e.target === e.currentTarget) closeModal(id);
  });
});

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') ['info-modal','settings-modal'].forEach(closeModal);
});

// VSL toggle (Issue #3)
document.getElementById('vsl-toggle').addEventListener('change', e => {
  document.body.classList.toggle('vsl-hidden', !e.target.checked);
});

// ── Init ──────────────────────────────────────────────
updateLegend();
buildLayer(activeChamber);
</script>
</body>
</html>
"""


def generate_html(races: dict, statewide: dict, geojson_senate: dict,
                  geojson_house: dict, city_map: dict) -> str:
    races_json     = json.dumps(races,      separators=(',', ':'))
    statewide_json = json.dumps(statewide,  separators=(',', ':'))
    geojson_json   = json.dumps({"Senate": geojson_senate, "House": geojson_house},
                                separators=(',', ':'))
    city_map_json  = json.dumps(city_map,   separators=(',', ':'))

    html = HTML_TEMPLATE
    html = html.replace('__RACES_JSON__',     races_json)
    html = html.replace('__STATEWIDE_JSON__', statewide_json)
    html = html.replace('__GEOJSON_JSON__',   geojson_json)
    html = html.replace('__CITY_MAP_JSON__',  city_map_json)
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    MAP_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading candidate data...")
    races = load_races()
    senate_count = sum(len(v['candidates']) for v in races['Senate'].values())
    house_count  = sum(len(v['candidates']) for v in races['House'].values())
    print(f"  Senate: {len(races['Senate'])} districts, {senate_count} candidates")
    print(f"  House:  {len(races['House'])} districts, {house_count} candidates")

    print("\nLoading statewide data...")
    statewide = load_statewide_races()

    print("\nLoading GeoJSON...")
    gj_senate = shapefile_to_geojson("Senate")
    gj_house  = shapefile_to_geojson("House")
    print(f"  Senate: {len(gj_senate['features'])} district polygons")
    print(f"  House:  {len(gj_house['features'])} district polygons")

    print("\nLoading Colorado places...")
    places = load_places()

    print("\nBuilding city → district map...")
    city_map = build_city_map(gj_senate, gj_house, places)
    s_mapped = sum(1 for v in city_map["Senate"].values() if v)
    h_mapped = sum(1 for v in city_map["House"].values()  if v)
    print(f"  Senate: {s_mapped} districts with cities")
    print(f"  House:  {h_mapped} districts with cities")

    print(f"\nGenerating {OUTPUT_HTML}...")
    html = generate_html(races, statewide, gj_senate, gj_house, city_map)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = OUTPUT_HTML.stat().st_size // 1024
    print(f"✓ Done — {OUTPUT_HTML} ({size_kb} KB)")
    print(f"\nOpen in browser:  open map/index.html")
    print(f"Or serve locally: python3 -m http.server 8000 --directory map/")


if __name__ == "__main__":
    main()
