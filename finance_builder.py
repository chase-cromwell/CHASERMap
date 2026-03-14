#!/usr/bin/env python3
"""
finance_builder.py — Generate the self-contained Finance Explorer HTML page.

This script reads three Colorado campaign finance CSVs (contributions,
expenditures, loans) and produces map/finance.html — a single-file
interactive dashboard that requires no server-side calls once loaded.

The generated page has three tabs:
  1. Donor Lookup   — search all contributions by donor name, filter by
                      contributor type (Individual, Business, PAC, LLC, Other)
  2. Fundraising    — per-candidate quarterly bar/line charts (Chart.js)
  3. Expenditures   — itemized spending list for any tracked candidate

Data sources (all under data/finances/):
    *ContributionData*.csv   — all contributions to all CO committees
    *ExpenditureData*.csv    — all expenditures
    *LoanData*.csv           — loans (original + repayments)

All files matching those patterns are loaded automatically, so dropping
a new year's data into the finances/ folder is sufficient — no code
changes needed.  Files are processed in sorted (chronological) order.

Only "tracked" candidates — those with a committee that appears in either
tracer_2026_all_districts.csv or tracer_2026_statewide.csv — appear in the
timeline/expenditure views.  The donor lookup includes ALL contributions
across the entire Colorado campaign finance system for all loaded years.

Amendment handling:
    Rows with Amended=Y have been superseded and are skipped.
    Rows with Amendment=Y are the current valid version and are kept.

Usage:
    python3 finance_builder.py
"""

import csv
import json
import re
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR    = Path(__file__).parent / "data"
FINANCE_DIR = DATA_DIR / "finances"
MAP_DIR     = Path(__file__).parent / "map"
OUTPUT_HTML = MAP_DIR / "finance.html"

TRACER_LEGISLATIVE = DATA_DIR / "tracer_2026_all_districts.csv"
TRACER_STATEWIDE   = DATA_DIR / "tracer_2026_statewide.csv"

# Dynamically discover all contribution/expenditure/loan files in the
# finances folder so prior-year data is automatically included when new
# files are dropped in.  Files are sorted so they are processed in
# chronological order (e.g. 2024_* before 2025_* before 2026_*).
CONTRIBUTIONS_CSVS = sorted(FINANCE_DIR.glob("*ContributionData*.csv"))
EXPENDITURES_CSVS  = sorted(FINANCE_DIR.glob("*ExpenditureData*.csv"))
LOANS_CSVS         = sorted(FINANCE_DIR.glob("*LoanData*.csv"))

# ---------------------------------------------------------------------------
# Fiscal quarter config
# Each entry: (label, (start_month, start_day), (end_month, end_day))
# These are standard calendar quarters. Edit ranges here if Colorado's
# reporting periods differ from the calendar year.
# ---------------------------------------------------------------------------
FISCAL_QUARTERS = [
    ("Q1", (1,  1), (3,  31)),
    ("Q2", (4,  1), (6,  30)),
    ("Q3", (7,  1), (9,  30)),
    ("Q4", (10, 1), (12, 31)),
]

# ---------------------------------------------------------------------------
# Contributor type classification
# ---------------------------------------------------------------------------

def classify_type(ctype: str) -> str:
    """Map a TRACER ContributorType string to a single-character group code.

    These codes are used by the JavaScript donor-lookup filter to quickly
    toggle visibility by contributor category without re-parsing strings.

    Groups:
        "I" — Individual person
        "L" — Individual (Member of LLC) — a natural person donating via LLC
        "B" — Business entity (Corporation, Partnership, etc.)
        "C" — Committee / PAC / party organization / labor union
        "O" — Other / unrecognized type

    Args:
        ctype: The raw ContributorType string from the TRACER CSV.

    Returns:
        Single-character group code.
    """
    t = ctype.strip()
    if t == "Individual":
        return "I"
    if t.startswith("Individual (Member of LLC"):
        return "L"
    if t in ("Corporation", "Business", "Partnership", "527 Political Organization"):
        return "B"
    if t in (
        "Candidate Committee", "Political Committee", "Small Donor Committee",
        "Federal PAC", "Issue Committee", "Independent Expenditure Committee",
        "Political Party Committee", "Labor Union", "Candidate Committee",
        "Candidate",
    ):
        return "C"
    return "O"


def assign_quarter(dt: datetime) -> str:
    """Return the fiscal quarter label for a given date.

    Matches the date's (month, day) against each FISCAL_QUARTERS range.

    Args:
        dt: A datetime object.

    Returns:
        Quarter string in "YYYY-Q#" format, e.g. "2026-Q1".
        Returns "YYYY-Q?" if the date doesn't match any configured range
        (should not happen with standard calendar quarters).
    """
    md = (dt.month, dt.day)
    for label, start, end in FISCAL_QUARTERS:
        if start <= md <= end:
            return f"{dt.year}-{label}"
    return f"{dt.year}-Q?"


def parse_date(s: str) -> datetime | None:
    """Parse a date string to a datetime, returning None on failure.

    Accepts ISO 8601 format "YYYY-MM-DD".  Truncates to the first 10
    characters to handle strings with a time component.
    """
    s = s.strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None


def parse_amount(s: str) -> float:
    """Parse a dollar amount string to a float, returning 0.0 on failure.

    Handles empty strings, None, and non-numeric values gracefully.
    TRACER amounts do not include currency symbols or commas.
    """
    try:
        return float(s.strip())
    except (ValueError, AttributeError):
        return 0.0


# ---------------------------------------------------------------------------
# Load tracked committees
# ---------------------------------------------------------------------------

def load_tracked_committees() -> dict:
    """Build a lookup of committee names for all tracked candidates.

    "Tracked" means the candidate appears in either the legislative or the
    statewide TRACER CSV.  Only tracked candidates get timeline and
    expenditure detail in the Finance Explorer.

    The returned dict is keyed by UPPER-CASE committee name so it can be
    matched case-insensitively against the CommitteeName column in the
    contribution/expenditure CSVs.

    Returns:
        {
          "FRIENDS OF JANE DOE": {
            "committee_name": "Friends of Jane Doe",   # original casing
            "candidate_name": "DOE, JANE A",
            "chamber":        "House",                 # or "Statewide"
            "district":       "44",
            "district_label": "House District 44",
            "party":          "Democratic",
          },
          ...
        }
    """
    committees = {}

    def ingest(filepath, chamber_override=None):
        """Read one TRACER CSV and add its committees to the dict."""
        with open(filepath, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cn = row["CommitteeName"].strip()
                if not cn or cn == "None":
                    continue  # skip rows without a committee name
                cand = row["CandName"].strip()
                chamber = chamber_override or row.get("Chamber", "").strip()
                district = row.get("DistrictNumber", "").strip()
                label = row.get("DistrictLabel", "").strip()
                party = row.get("Party", "").strip()
                # Upper-case key for case-insensitive matching later
                committees[cn.upper()] = {
                    "committee_name": cn,
                    "candidate_name": cand,
                    "chamber": chamber,
                    "district": district,
                    "district_label": label,
                    "party": party,
                }

    ingest(TRACER_LEGISLATIVE)
    ingest(TRACER_STATEWIDE, chamber_override="Statewide")
    return committees  # keyed by UPPER committee name


# ---------------------------------------------------------------------------
# Load contributions
# ---------------------------------------------------------------------------

def load_contributions(tracked_by_name: dict, coid_map: dict) -> tuple[list, dict]:
    """Parse the contributions CSV and build donor-lookup and timeline data.

    Processes every row in 2026_ContributionData.csv.  Two outputs are built
    simultaneously:

    1. contributions_list — compact arrays for ALL contributions (all committees
       in Colorado, not just tracked candidates).  Used by the donor-lookup tab
       so users can search any donor across the full state finance system.

    2. timelines — quarterly fundraising totals for tracked candidates only.
       Used by the Fundraising tab to render Chart.js bar/line charts.

    The coid_map argument is mutated in-place: when a contribution from a
    tracked committee is encountered for the first time, its CO_ID is added
    as a key so subsequent expenditure/loan lookups can use the numeric ID
    instead of the committee name string.

    Amendment handling (TRACER-specific):
      - Amended=Y  → record was superseded; SKIP entirely.
      - Amendment=Y → this IS the current corrected record; KEEP.
      - Both N      → untouched original; KEEP.

    Args:
        tracked_by_name: { UPPER_COMMITTEE_NAME: committee_meta } from
                         load_tracked_committees()
        coid_map:        Empty dict, filled in-place with
                         { co_id: committee_meta } for tracked candidates.

    Returns:
        (contributions_list, timelines) where:

        contributions_list — list of compact 10-element arrays:
            [date_str, amount, co_id, committee_name, candidate_name,
             last, first, city, state, type_group]
            (indices documented here match the JS column references in finance.html)

        timelines — {
            co_id: {
              "co_id": ..., "committee_name": ..., "candidate_name": ...,
              "chamber": ..., "district_label": ..., "party": ...,
              "quarters": { "2026-Q1": 12500.0, "2026-Q2": 8200.0, ... }
            }, ...
          }
    """
    # Compact array layout per contribution (indices referenced in JS):
    # 0:date_str  1:amount  2:co_id  3:committee_name  4:candidate_name
    # 5:last      6:first   7:city   8:state           9:type_group
    contributions_list = []
    timelines = {}  # co_id → {meta dict + quarters accumulator}
    skipped_amended = 0
    skipped_date = 0

    # TRACER exports use latin-1 encoding (Windows-1252 compatible).
    # Iterate over all discovered contribution files (sorted chronologically).
    for csv_path in CONTRIBUTIONS_CSVS:
        print(f"  Loading contributions: {csv_path.name}")
        with open(csv_path, newline="", encoding="latin-1") as f:
            for row in csv.DictReader(f):
                # Skip superseded records — the Amended=Y flag means a later
                # amendment row (Amendment=Y) exists with the correct data.
                if row["Amended"].strip().upper() == "Y":
                    skipped_amended += 1
                    continue

                amount = parse_amount(row["ContributionAmount"])
                dt = parse_date(row["ContributionDate"])
                if dt is None:
                    skipped_date += 1
                    continue

                co_id = row["CO_ID"].strip()
                committee_name = row["CommitteeName"].strip()
                candidate_name = row["CandidateName"].strip()
                last = row["LastName"].strip()
                first = row["FirstName"].strip()
                city = row["City"].strip()
                state = row["State"].strip()
                tg = classify_type(row["ContributorType"])
                date_str = dt.strftime("%Y-%m-%d")

                # Track CO_ID → metadata for tracked candidates
                cn_upper = committee_name.upper()
                if cn_upper in tracked_by_name and co_id not in coid_map:
                    coid_map[co_id] = {**tracked_by_name[cn_upper], "co_id": co_id}

                # Compact array for donor lookup (all contributions)
                contributions_list.append([
                    date_str, amount, co_id, committee_name, candidate_name,
                    last, first, city, state, tg
                ])

                # Timeline accumulation (tracked candidates only)
                if co_id in coid_map or cn_upper in tracked_by_name:
                    effective_coid = co_id
                    if effective_coid not in timelines:
                        meta = coid_map.get(effective_coid, tracked_by_name.get(cn_upper, {}))
                        timelines[effective_coid] = {
                            "co_id": effective_coid,
                            "committee_name": committee_name,
                            "candidate_name": candidate_name,
                            "chamber": meta.get("chamber", ""),
                            "district_label": meta.get("district_label", ""),
                            "party": meta.get("party", ""),
                            "quarters": {},
                        }
                    qkey = assign_quarter(dt)
                    timelines[effective_coid]["quarters"][qkey] = (
                        timelines[effective_coid]["quarters"].get(qkey, 0.0) + amount
                    )

    print(f"  Contributions: {len(contributions_list):,} kept, "
          f"{skipped_amended:,} amended skipped, {skipped_date:,} bad dates")
    return contributions_list, timelines


# ---------------------------------------------------------------------------
# Load expenditures
# ---------------------------------------------------------------------------

def load_expenditures(coid_map: dict) -> dict:
    """Parse the expenditures CSV for tracked candidates only.

    Only candidates whose CO_ID appears in coid_map (populated by
    load_contributions) are included.  Expenditures are sorted by date
    descending so the most recent spending appears first in the UI.

    Amended rows (Amended=Y) are skipped for the same reason as contributions.

    Args:
        coid_map: { co_id: committee_meta } populated by load_contributions().

    Returns:
        {
          co_id: [
            { "date": "2026-01-15", "vendor": "Smith, John",
              "amount": 500.0, "type": "...", "payment": "...",
              "notes": "...", "city": "Denver", "state": "CO" },
            ...
          ],
          ...
        }
        Sorted by date descending within each candidate.
    """
    expenditures = {}
    skipped_amended = 0

    for csv_path in EXPENDITURES_CSVS:
        print(f"  Loading expenditures: {csv_path.name}")
        with open(csv_path, newline="", encoding="latin-1") as f:
            for row in csv.DictReader(f):
                if row["Amended"].strip().upper() == "Y":
                    skipped_amended += 1
                    continue

                co_id = row["CO_ID"].strip()

                # Only include tracked candidates
                if co_id not in coid_map:
                    continue

                dt = parse_date(row["ExpenditureDate"])
                if dt is None:
                    continue

                amount = parse_amount(row["ExpenditureAmount"])
                vendor_last = row["LastName"].strip()
                vendor_first = row["FirstName"].strip()
                vendor = f"{vendor_last}, {vendor_first}".strip(", ") if vendor_first else vendor_last

                if co_id not in expenditures:
                    expenditures[co_id] = []

                expenditures[co_id].append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "vendor": vendor,
                    "amount": amount,
                    "type": row["ExpenditureType"].strip(),
                    "payment": row["PaymentType"].strip(),
                    "notes": row["Explanation"].strip()[:120],  # cap length
                    "city": row["City"].strip(),
                    "state": row["State"].strip(),
                })

    # Sort each candidate's expenditures by date descending
    for co_id in expenditures:
        expenditures[co_id].sort(key=lambda x: x["date"], reverse=True)

    print(f"  Expenditures: {sum(len(v) for v in expenditures.values()):,} rows across "
          f"{len(expenditures)} candidates, {skipped_amended:,} amended skipped")
    return expenditures


# ---------------------------------------------------------------------------
# Load loans
# ---------------------------------------------------------------------------

def load_loans(coid_map: dict) -> dict:
    """Parse the loans CSV for tracked candidates only.

    Colorado campaign finance allows candidates to loan money to their own
    committees.  Each loan has two possible row types distinguished by the
    Type column:
        "O" — Original loan (money flowing in to the committee)
        "P" — Payment on an existing loan (money flowing out)

    Args:
        coid_map: { co_id: committee_meta } populated by load_contributions().

    Returns:
        {
          co_id: {
            "originals": [
              { "date": "...", "source": "...", "source_type": "...",
                "amount": 5000.0, "balance": 5000.0, "interest_rate": 0.0 },
              ...
            ],
            "payments": [
              { "date": "...", "source": "...", "amount": 500.0,
                "loan_date": "...", "original_amount": 5000.0 },
              ...
            ]
          },
          ...
        }
    """
    loans = {}

    for csv_path in LOANS_CSVS:
        print(f"  Loading loans: {csv_path.name}")
        _load_loans_file(csv_path, coid_map, loans)

    print(f"  Loans: {sum(len(v['originals']) for v in loans.values())} originals, "
          f"{sum(len(v['payments']) for v in loans.values())} payments across "
          f"{len(loans)} candidates")
    return loans


def _load_loans_file(csv_path: Path, coid_map: dict, loans: dict) -> None:
    """Read one loan CSV file and accumulate results into *loans* in-place."""
    with open(csv_path, newline="", encoding="latin-1") as f:
        for row in csv.DictReader(f):
            if row["Amended"].strip().upper() == "Y":
                continue

            co_id = row["CO_ID"].strip()
            if co_id not in coid_map:
                continue

            loan_type = row["Type"].strip().upper()
            loan_amount = parse_amount(row["LoanAmount"])
            payment_amount = parse_amount(row["PaymentAmount"])
            loan_date = parse_date(row["LoanDate"])
            pay_date = parse_date(row["PaymentDate"])
            source = row["Name"].strip()
            source_type = row["LoanSourceType"].strip()
            balance = parse_amount(row["LoanBalance"])

            if co_id not in loans:
                loans[co_id] = {"originals": [], "payments": []}

            if loan_type == "O":
                loans[co_id]["originals"].append({
                    "date": loan_date.strftime("%Y-%m-%d") if loan_date else "",
                    "source": source,
                    "source_type": source_type,
                    "amount": loan_amount,
                    "balance": balance,
                    "interest_rate": parse_amount(row["InterestRate"]),
                })
            elif loan_type == "P":
                loans[co_id]["payments"].append({
                    "date": pay_date.strftime("%Y-%m-%d") if pay_date else "",
                    "source": source,
                    "amount": payment_amount,
                    "loan_date": loan_date.strftime("%Y-%m-%d") if loan_date else "",
                    "original_amount": loan_amount,
                })


# ---------------------------------------------------------------------------
# Load quarterly raised + spent (lightweight summary for Flask pages)
# ---------------------------------------------------------------------------

def load_quarterly_data() -> dict:
    """Build per-committee quarterly raised and spent totals from all finance CSVs.

    Unlike load_contributions() / load_expenditures() which build the full
    finance.html explorer dataset, this function does a single focused pass
    and produces a compact summary used by ingest.py to populate the
    quarters_raised_json and quarters_spent_json columns in the candidates
    table.

    Only committees that appear in the legislative or statewide TRACER CSVs
    are included — untracked committees are ignored.

    Returns:
        { UPPER_COMMITTEE_NAME: {
            "raised": { "2024-Q1": 500.0, "2024-Q2": 1200.0, ... },
            "spent":  { "2024-Q1": 300.0, "2024-Q2": 800.0,  ... },
          }, ...
        }
    """
    tracked_by_name = load_tracked_committees()
    quarterly: dict[str, dict] = {}

    def _entry(cn_upper: str) -> dict:
        if cn_upper not in quarterly:
            quarterly[cn_upper] = {"raised": {}, "spent": {}}
        return quarterly[cn_upper]

    print("  Quarterly: scanning contribution files...")
    for csv_path in CONTRIBUTIONS_CSVS:
        with open(csv_path, newline="", encoding="latin-1") as f:
            for row in csv.DictReader(f):
                if row["Amended"].strip().upper() == "Y":
                    continue
                cn_upper = row["CommitteeName"].strip().upper()
                if cn_upper not in tracked_by_name:
                    continue
                dt = parse_date(row["ContributionDate"])
                if dt is None:
                    continue
                qkey = assign_quarter(dt)
                e = _entry(cn_upper)
                e["raised"][qkey] = e["raised"].get(qkey, 0.0) + parse_amount(row["ContributionAmount"])

    print("  Quarterly: scanning expenditure files...")
    for csv_path in EXPENDITURES_CSVS:
        with open(csv_path, newline="", encoding="latin-1") as f:
            for row in csv.DictReader(f):
                if row["Amended"].strip().upper() == "Y":
                    continue
                cn_upper = row["CommitteeName"].strip().upper()
                if cn_upper not in tracked_by_name:
                    continue
                dt = parse_date(row["ExpenditureDate"])
                if dt is None:
                    continue
                qkey = assign_quarter(dt)
                e = _entry(cn_upper)
                e["spent"][qkey] = e["spent"].get(qkey, 0.0) + parse_amount(row["ExpenditureAmount"])

    print(f"  Quarterly data: {len(quarterly)} tracked committees with timeline data")
    return quarterly


# ---------------------------------------------------------------------------
# Sort timeline quarters chronologically
# ---------------------------------------------------------------------------

def sorted_quarters(quarters: dict) -> list[dict]:
    """Convert an unordered quarter dict into a chronological list with cumulative totals.

    The quarters accumulator in timelines stores { "2026-Q2": 8200.0, ... }
    in insertion order, which may not be chronological.  This function
    sorts them and adds a running cumulative sum for Chart.js line charts.

    Args:
        quarters: { "YYYY-Q#": amount_float, ... }

    Returns:
        [
          { "label": "2025-Q4", "amount": 3200.00, "cumulative": 3200.00 },
          { "label": "2026-Q1", "amount": 8200.00, "cumulative": 11400.00 },
          ...
        ]
    """
    def quarter_sort_key(label):
        # Parse "YYYY-Q#" into a (year, quarter_number) tuple for sorting
        parts = label.split("-")
        year = int(parts[0])
        q = int(parts[1][1]) if len(parts) > 1 and parts[1].startswith("Q") else 0
        return (year, q)

    items = sorted(quarters.items(), key=lambda kv: quarter_sort_key(kv[0]))
    result = []
    running = 0.0
    for label, amount in items:
        running += amount
        result.append({"label": label, "amount": round(amount, 2), "cumulative": round(running, 2)})
    return result


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(contributions_list, timelines, expenditures, loans, coid_map) -> str:
    """Render the complete finance.html page as a string.

    Builds the HTML/CSS/JS scaffolding and injects all data as inline JSON
    constants, producing a fully self-contained file that can be opened
    directly in a browser or served as a static file.

    Data injection strategy:
      - JSON blobs are minified (no whitespace) using separators=(",", ":")
        to keep file size down.  A typical season's data produces ~5-10 MB.
      - The JavaScript reads these constants at page load and builds all UI
        state from them — no fetch() calls are needed at runtime.

    Args:
        contributions_list: All contributions as compact arrays (from load_contributions)
        timelines:          Quarterly fundraising per tracked candidate
        expenditures:       Itemized spending per tracked candidate
        loans:              Loan originals + payments per tracked candidate
        coid_map:           { co_id: meta } for populating candidate dropdowns

    Returns:
        Complete HTML string ready to write to map/finance.html.
    """
    # Build the candidate list for the dropdown selectors, sorted by name
    all_candidates = []
    for co_id, meta in coid_map.items():
        name = meta.get("candidate_name") or meta.get("committee_name", "")
        chamber = meta.get("chamber", "")
        label = meta.get("district_label", "")
        party = meta.get("party", "")
        # Display format: "DOE, JANE (House District 44)"
        display = name
        if label:
            display += f" ({label})"
        all_candidates.append({
            "co_id": co_id,
            "name": name,
            "display": display,
            "chamber": chamber,
            "party": party,
            "label": label,
        })
    all_candidates.sort(key=lambda x: x["name"])

    # Convert timelines: replace the unordered quarters dict with a sorted
    # list that includes cumulative totals (for Chart.js rendering)
    timelines_out = {}
    for co_id, tl in timelines.items():
        timelines_out[co_id] = {
            "committee_name": tl["committee_name"],
            "candidate_name": tl["candidate_name"],
            "chamber": tl["chamber"],
            "district_label": tl["district_label"],
            "party": tl["party"],
            "quarters": sorted_quarters(tl["quarters"]),
        }

    # Minified JSON blobs — injected directly into <script> const declarations
    j_contributions = json.dumps(contributions_list, separators=(",", ":"))
    j_timelines     = json.dumps(timelines_out,     separators=(",", ":"))
    j_expenditures  = json.dumps(expenditures,      separators=(",", ":"))
    j_loans         = json.dumps(loans,             separators=(",", ":"))
    j_candidates    = json.dumps(all_candidates,    separators=(",", ":"))

    # Pre-render <option> elements for the candidate dropdowns
    candidate_options = "\n".join(
        f'<option value="{c["co_id"]}">{c["display"]}</option>'
        for c in all_candidates
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TRACERMap — Finance Explorer</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #22263a;
    --border: #2e3352;
    --text: #e0e4f0;
    --text2: #8891b0;
    --accent: #4f8ef7;
    --dem: #4878d0;
    --rep: #d64e4e;
    --unaffiliated: #888;
    --green: #3fb97a;
    --red: #d64e4e;
    --yellow: #e8c84a;
  }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 14px;
    min-height: 100vh;
  }}

  /* Header */
  header {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 14px 24px;
    display: flex;
    align-items: center;
    gap: 16px;
  }}
  header h1 {{ font-size: 18px; font-weight: 600; letter-spacing: 0.5px; }}
  header a {{ color: var(--accent); text-decoration: none; font-size: 13px; }}
  header a:hover {{ text-decoration: underline; }}

  /* Tabs */
  .tabs {{
    display: flex;
    gap: 2px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0 24px;
  }}
  .tab-btn {{
    padding: 11px 20px;
    background: none;
    border: none;
    border-bottom: 2px solid transparent;
    color: var(--text2);
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: color 0.15s, border-color 0.15s;
    margin-bottom: -1px;
  }}
  .tab-btn:hover {{ color: var(--text); }}
  .tab-btn.active {{
    color: var(--accent);
    border-bottom-color: var(--accent);
  }}

  /* Panels */
  .panel {{ display: none; padding: 24px; max-width: 1100px; margin: 0 auto; }}
  .panel.active {{ display: block; }}

  /* Controls row */
  .controls {{
    display: flex;
    gap: 12px;
    align-items: flex-start;
    flex-wrap: wrap;
    margin-bottom: 20px;
  }}

  /* Search input */
  .search-wrap {{ flex: 1; min-width: 240px; max-width: 400px; }}
  .search-wrap input {{
    width: 100%;
    padding: 9px 14px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 14px;
    outline: none;
    transition: border-color 0.15s;
  }}
  .search-wrap input:focus {{ border-color: var(--accent); }}

  /* Contributor type filter */
  .type-filter {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 8px 14px;
    display: flex;
    gap: 14px;
    flex-wrap: wrap;
    align-items: center;
  }}
  .type-filter label {{
    display: flex;
    align-items: center;
    gap: 5px;
    color: var(--text2);
    font-size: 13px;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }}
  .type-filter input[type=checkbox] {{ accent-color: var(--accent); cursor: pointer; }}
  .type-filter .filter-label {{ color: var(--text2); font-size: 12px; margin-right: 2px; }}

  /* Select dropdown */
  select {{
    padding: 9px 14px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 13px;
    outline: none;
    cursor: pointer;
    min-width: 200px;
  }}
  select:focus {{ border-color: var(--accent); }}

  /* Tables */
  .table-wrap {{ overflow-x: auto; border-radius: 8px; border: 1px solid var(--border); }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  thead th {{
    background: var(--surface2);
    padding: 10px 14px;
    text-align: left;
    font-weight: 600;
    color: var(--text2);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 1px solid var(--border);
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }}
  thead th:hover {{ color: var(--text); }}
  thead th.sorted-asc::after  {{ content: " ▲"; font-size: 10px; }}
  thead th.sorted-desc::after {{ content: " ▼"; font-size: 10px; }}
  tbody tr {{
    border-bottom: 1px solid var(--border);
    transition: background 0.1s;
  }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: var(--surface2); }}
  td {{
    padding: 9px 14px;
    color: var(--text);
    vertical-align: top;
  }}
  td.amt {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
  td.dim {{ color: var(--text2); }}
  tfoot td {{
    padding: 10px 14px;
    font-weight: 600;
    border-top: 2px solid var(--border);
    background: var(--surface2);
  }}
  tfoot td.amt {{ text-align: right; color: var(--green); }}

  /* Donor result cards */
  .donor-list {{ display: flex; flex-direction: column; gap: 12px; }}
  .donor-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
  }}
  .donor-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    cursor: pointer;
    user-select: none;
    gap: 12px;
  }}
  .donor-header:hover {{ background: var(--surface2); }}
  .donor-name {{ font-weight: 600; font-size: 15px; }}
  .donor-meta {{ color: var(--text2); font-size: 12px; margin-top: 2px; }}
  .donor-total {{ font-size: 16px; font-weight: 700; color: var(--green); white-space: nowrap; }}
  .donor-chevron {{ color: var(--text2); transition: transform 0.2s; }}
  .donor-card.expanded .donor-chevron {{ transform: rotate(90deg); }}
  .donor-detail {{ display: none; padding: 0 16px 16px; }}
  .donor-card.expanded .donor-detail {{ display: block; }}

  /* Result count */
  .result-count {{ color: var(--text2); font-size: 13px; margin-bottom: 14px; }}

  /* Party dot */
  .party-dot {{
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    margin-right: 5px;
    vertical-align: middle;
  }}
  .party-dem {{ background: var(--dem); }}
  .party-rep {{ background: var(--rep); }}
  .party-other {{ background: var(--unaffiliated); }}

  /* Chart container */
  .chart-wrap {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
  }}

  /* Candidate meta bar */
  .cand-meta {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px 18px;
    margin-bottom: 16px;
    display: flex;
    gap: 24px;
    flex-wrap: wrap;
    align-items: center;
  }}
  .cand-meta .cand-name {{ font-size: 17px; font-weight: 700; }}
  .cand-meta .cand-info {{ color: var(--text2); font-size: 13px; }}

  /* Summary stats row */
  .stats-row {{
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 20px;
  }}
  .stat-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px 18px;
    min-width: 140px;
    flex: 1;
  }}
  .stat-label {{ color: var(--text2); font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .stat-value {{ font-size: 22px; font-weight: 700; margin-top: 4px; }}

  /* Empty state */
  .empty {{ color: var(--text2); padding: 32px; text-align: center; }}

  /* Expenditure type filter */
  .exp-controls {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }}
</style>
</head>
<body>

<header>
  <h1>TRACERMap — Finance Explorer</h1>
  <a href="index.html">← Back to Map</a>
</header>

<nav class="tabs">
  <button class="tab-btn active" data-panel="donor">Donor Lookup</button>
  <button class="tab-btn" data-panel="timeline">Fundraising Timeline</button>
  <button class="tab-btn" data-panel="expenses">Expenditures</button>
</nav>

<!-- ===== TAB 1: DONOR LOOKUP ===== -->
<div id="panel-donor" class="panel active">
  <div class="controls">
    <div class="search-wrap">
      <input id="donor-search" type="text" placeholder="Search donor name…" autocomplete="off" spellcheck="false">
    </div>
    <div class="type-filter">
      <span class="filter-label">Contributor type:</span>
      <label><input type="checkbox" name="tg" value="I" checked> Individual</label>
      <label><input type="checkbox" name="tg" value="L"> Individual (LLC)</label>
      <label><input type="checkbox" name="tg" value="C"> Committee / PAC</label>
      <label><input type="checkbox" name="tg" value="B"> Business / Corp</label>
      <label><input type="checkbox" name="tg" value="O"> Other</label>
    </div>
  </div>
  <div id="donor-result-count" class="result-count"></div>
  <div id="donor-list" class="donor-list"></div>
</div>

<!-- ===== TAB 2: FUNDRAISING TIMELINE ===== -->
<div id="panel-timeline" class="panel">
  <div class="controls">
    <select id="timeline-select">
      <option value="">— Select a candidate —</option>
      {candidate_options}
    </select>
  </div>
  <div id="timeline-content"></div>
</div>

<!-- ===== TAB 3: EXPENDITURES ===== -->
<div id="panel-expenses" class="panel">
  <div class="controls">
    <select id="expenses-select">
      <option value="">— Select a candidate —</option>
      {candidate_options}
    </select>
    <select id="expenses-type-filter">
      <option value="">All categories</option>
    </select>
  </div>
  <div id="expenses-content"></div>
</div>

<script>
// ============================================================
// Embedded data (generated by finance_builder.py)
// ============================================================
// Contributions: array of arrays
// Columns: [date, amount, co_id, committee_name, candidate_name,
//           last, first, city, state, type_group]
const CONTRIBUTIONS = {j_contributions};

// Timelines: {{co_id: {{committee_name, candidate_name, chamber,
//   district_label, party, quarters: [{{label,amount,cumulative}}]}}}}
const TIMELINES = {j_timelines};

// Expenditures: {{co_id: [{{date,vendor,amount,type,payment,notes,city,state}}]}}
const EXPENDITURES = {j_expenditures};

// Loans: {{co_id: {{originals:[...], payments:[...]}}}}
const LOANS = {j_loans};

// Candidates: [{{co_id, name, display, chamber, party, label}}]
const CANDIDATES = {j_candidates};

// ============================================================
// Column indices for CONTRIBUTIONS array
// ============================================================
const C_DATE = 0, C_AMT = 1, C_COID = 2, C_CMTE = 3, C_CAND = 4,
      C_LAST = 5, C_FIRST = 6, C_CITY = 7, C_STATE = 8, C_TG = 9;

// ============================================================
// Utilities
// ============================================================
const fmt = new Intl.NumberFormat("en-US", {{style:"currency", currency:"USD", maximumFractionDigits:0}});
const fmtAmt = amt => fmt.format(amt);
const fmtAmtFull = amt => new Intl.NumberFormat("en-US", {{style:"currency", currency:"USD", minimumFractionDigits:2}}).format(amt);

function partyClass(p) {{
  if (!p) return "party-other";
  const u = p.toUpperCase();
  if (u === "DEMOCRAT") return "party-dem";
  if (u === "REPUBLICAN") return "party-rep";
  return "party-other";
}}

function esc(s) {{
  return String(s ?? "")
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;");
}}

// ============================================================
// Tab switching
// ============================================================
document.querySelectorAll(".tab-btn").forEach(btn => {{
  btn.addEventListener("click", () => {{
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("panel-" + btn.dataset.panel).classList.add("active");
  }});
}});

// ============================================================
// TABLE SORTING UTILITY
// ============================================================
function makeSortable(table) {{
  const headers = table.querySelectorAll("thead th[data-col]");
  let sortCol = null, sortDir = 1;
  headers.forEach(th => {{
    th.addEventListener("click", () => {{
      const col = parseInt(th.dataset.col);
      if (sortCol === col) sortDir = -sortDir;
      else {{ sortCol = col; sortDir = 1; }}
      headers.forEach(h => h.classList.remove("sorted-asc","sorted-desc"));
      th.classList.add(sortDir === 1 ? "sorted-asc" : "sorted-desc");
      const tbody = table.querySelector("tbody");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => {{
        const aVal = a.children[col]?.dataset.val ?? a.children[col]?.textContent ?? "";
        const bVal = b.children[col]?.dataset.val ?? b.children[col]?.textContent ?? "";
        const aNum = parseFloat(aVal), bNum = parseFloat(bVal);
        if (!isNaN(aNum) && !isNaN(bNum)) return (aNum - bNum) * sortDir;
        return aVal.localeCompare(bVal) * sortDir;
      }});
      rows.forEach(r => tbody.appendChild(r));
    }});
  }});
}}

// ============================================================
// TAB 1: DONOR LOOKUP
// ============================================================
(function() {{
  // Build donor index on page load: donor_key -> {{displayName, total, contribs[]}}
  const donorIndex = new Map();

  for (const row of CONTRIBUTIONS) {{
    const last = row[C_LAST].trim().toUpperCase();
    const first = row[C_FIRST].trim().toUpperCase();
    if (!last) continue;
    const key = first ? last + "|" + first : last;
    let entry = donorIndex.get(key);
    if (!entry) {{
      const displayFirst = row[C_FIRST].trim();
      const displayLast  = row[C_LAST].trim();
      const displayName  = displayFirst ? displayFirst + " " + displayLast : displayLast;
      entry = {{ displayName, key, total: 0, contributions: [], cities: new Set() }};
      donorIndex.set(key, entry);
    }}
    entry.total += row[C_AMT];
    entry.cities.add((row[C_CITY] + (row[C_STATE] ? ", " + row[C_STATE] : "")).trim());
    entry.contributions.push(row);
  }}

  // Convert cities set to string
  donorIndex.forEach(e => {{
    e.cityStr = Array.from(e.cities).filter(Boolean).slice(0,2).join(" / ");
    delete e.cities;
  }});

  // Sort donor index as sorted array for display
  const donorArray = Array.from(donorIndex.values())
    .sort((a, b) => a.key.localeCompare(b.key));

  const searchInput = document.getElementById("donor-search");
  const resultCount = document.getElementById("donor-result-count");
  const listEl = document.getElementById("donor-list");

  function activeTypes() {{
    return new Set(
      Array.from(document.querySelectorAll('input[name="tg"]:checked'))
        .map(cb => cb.value)
    );
  }}

  let debounceTimer = null;
  function refresh() {{
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(render, 250);
  }}

  searchInput.addEventListener("input", refresh);
  document.querySelectorAll('input[name="tg"]').forEach(cb => cb.addEventListener("change", refresh));

  function render() {{
    const query = searchInput.value.trim().toUpperCase();
    const types = activeTypes();

    if (query.length < 2) {{
      resultCount.textContent = "Type at least 2 characters to search.";
      listEl.innerHTML = "";
      return;
    }}

    // Find matching donors
    const matches = donorArray.filter(d => {{
      if (!d.key.includes(query) && !d.displayName.toUpperCase().includes(query)) return false;
      return d.contributions.some(row => types.has(row[C_TG]));
    }}).slice(0, 100);

    resultCount.textContent = matches.length === 0
      ? "No donors found."
      : matches.length === 100
        ? "Showing top 100 matches — refine your search for more specific results."
        : `${{matches.length}} donor${{matches.length !== 1 ? "s" : ""}} found.`;

    listEl.innerHTML = matches.map(donor => renderDonorCard(donor, types)).join("");

    // Attach expand/collapse handlers
    listEl.querySelectorAll(".donor-header").forEach(header => {{
      header.addEventListener("click", () => {{
        header.closest(".donor-card").classList.toggle("expanded");
      }});
    }});
  }}

  function renderDonorCard(donor, types) {{
    const filtered = donor.contributions
      .filter(row => types.has(row[C_TG]))
      .sort((a, b) => b[C_DATE].localeCompare(a[C_DATE]));

    const total = filtered.reduce((s, r) => s + r[C_AMT], 0);
    const uniqueCandidates = new Set(filtered.map(r => r[C_CAND] || r[C_CMTE])).size;

    // Group employer/occupation hints
    const empSet = new Set(filtered.map(r => r[10]).filter(Boolean));  // employer if present
    // Occupation would be index 11 but we dropped it from compact; skip for now

    const rows = filtered.map(row => {{
      const isTracked = TIMELINES[row[C_COID]] !== undefined;
      return `<tr>
        <td class="dim">${{row[C_DATE]}}</td>
        <td>${{esc(row[C_CAND] || row[C_CMTE])}}</td>
        <td class="amt" data-val="${{row[C_AMT]}}">${{fmtAmtFull(row[C_AMT])}}</td>
        <td class="dim">${{esc(row[C_CITY])}}, ${{esc(row[C_STATE])}}</td>
      </tr>`;
    }}).join("");

    return `<div class="donor-card">
      <div class="donor-header">
        <div>
          <div class="donor-name">${{esc(donor.displayName)}}</div>
          <div class="donor-meta">${{esc(donor.cityStr)}} &bull; ${{uniqueCandidates}} candidate${{uniqueCandidates !== 1 ? "s" : ""}}</div>
        </div>
        <div style="display:flex;align-items:center;gap:12px">
          <div class="donor-total">${{fmtAmt(total)}}</div>
          <div class="donor-chevron">&#9654;</div>
        </div>
      </div>
      <div class="donor-detail">
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th data-col="0">Date</th>
              <th data-col="1">Candidate / Committee</th>
              <th data-col="2">Amount</th>
              <th data-col="3">Location</th>
            </tr></thead>
            <tbody>${{rows}}</tbody>
            <tfoot><tr>
              <td colspan="2">Total</td>
              <td class="amt">${{fmtAmtFull(total)}}</td>
              <td></td>
            </tr></tfoot>
          </table>
        </div>
      </div>
    </div>`;
  }}

  // Initial empty state
  resultCount.textContent = "Type at least 2 characters to search.";
}})();

// ============================================================
// TAB 2: FUNDRAISING TIMELINE
// ============================================================
(function() {{
  const select = document.getElementById("timeline-select");
  const content = document.getElementById("timeline-content");
  let chart = null;

  // Sync candidate selection with Tab 3
  select.addEventListener("change", () => {{
    const expSel = document.getElementById("expenses-select");
    if (expSel.value !== select.value) expSel.value = select.value;
    render();
  }});

  function render() {{
    const coId = select.value;
    if (!coId) {{
      content.innerHTML = '<div class="empty">Select a candidate to view their fundraising timeline.</div>';
      return;
    }}
    const tl = TIMELINES[coId];
    if (!tl || !tl.quarters.length) {{
      content.innerHTML = '<div class="empty">No contribution data available for this candidate.</div>';
      return;
    }}

    const party = tl.party || "";
    const partyDotClass = partyClass(party);
    const totalRaised = tl.quarters[tl.quarters.length - 1]?.cumulative ?? 0;
    const maxQ = tl.quarters.reduce((a,b) => b.amount > a.amount ? b : a, tl.quarters[0]);
    const numQuarters = tl.quarters.filter(q => q.amount > 0).length;

    const labels  = tl.quarters.map(q => q.label);
    const amounts = tl.quarters.map(q => q.amount);
    const cumulat = tl.quarters.map(q => q.cumulative);

    const barColor  = party.toUpperCase() === "DEMOCRAT" ? "#4878d0"
                    : party.toUpperCase() === "REPUBLICAN" ? "#d64e4e" : "#4f8ef7";

    content.innerHTML = `
      <div class="cand-meta">
        <div class="cand-name">
          <span class="party-dot ${{partyDotClass}}"></span>
          ${{esc(tl.candidate_name)}}
        </div>
        <div class="cand-info">${{esc(tl.committee_name)}}</div>
        ${{tl.district_label ? `<div class="cand-info">${{esc(tl.district_label)}}</div>` : ""}}
      </div>
      <div class="stats-row">
        <div class="stat-card">
          <div class="stat-label">Total Raised</div>
          <div class="stat-value" style="color:var(--green)">${{fmtAmt(totalRaised)}}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Best Quarter</div>
          <div class="stat-value">${{maxQ.label}}</div>
          <div style="color:var(--text2);font-size:12px;margin-top:2px">${{fmtAmt(maxQ.amount)}}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Active Quarters</div>
          <div class="stat-value">${{numQuarters}}</div>
        </div>
      </div>
      <div class="chart-wrap">
        <canvas id="timeline-chart" height="220"></canvas>
      </div>
      <div class="table-wrap">
        <table id="timeline-table">
          <thead><tr>
            <th data-col="0">Quarter</th>
            <th data-col="1">Raised</th>
            <th data-col="2">Running Total</th>
          </tr></thead>
          <tbody>
            ${{tl.quarters.map(q => `<tr>
              <td>${{q.label}}</td>
              <td class="amt" data-val="${{q.amount}}">${{fmtAmt(q.amount)}}</td>
              <td class="amt" data-val="${{q.cumulative}}">${{fmtAmt(q.cumulative)}}</td>
            </tr>`).join("")}}
          </tbody>
        </table>
      </div>`;

    // Destroy previous chart if any
    if (chart) {{ chart.destroy(); chart = null; }}

    const ctx = document.getElementById("timeline-chart").getContext("2d");
    chart = new Chart(ctx, {{
      data: {{
        labels,
        datasets: [
          {{
            type: "bar",
            label: "Raised This Quarter",
            data: amounts,
            backgroundColor: barColor + "cc",
            borderColor: barColor,
            borderWidth: 1,
            yAxisID: "y",
            order: 2,
          }},
          {{
            type: "line",
            label: "Cumulative Total",
            data: cumulat,
            borderColor: "#e8c84a",
            backgroundColor: "transparent",
            borderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 5,
            tension: 0.3,
            yAxisID: "y2",
            order: 1,
          }},
        ],
      }},
      options: {{
        responsive: true,
        interaction: {{ mode: "index", intersect: false }},
        plugins: {{
          legend: {{
            labels: {{ color: "#8891b0", font: {{ size: 12 }} }}
          }},
          tooltip: {{
            callbacks: {{
              label: ctx => " " + ctx.dataset.label + ": " + fmtAmt(ctx.parsed.y),
            }},
          }},
        }},
        scales: {{
          x: {{
            ticks: {{ color: "#8891b0", font: {{ size: 11 }} }},
            grid:  {{ color: "#2e3352" }},
          }},
          y: {{
            position: "left",
            title: {{ display: true, text: "Raised", color: "#8891b0", font: {{ size: 11 }} }},
            ticks: {{ color: "#8891b0", font: {{ size: 11 }}, callback: v => fmtAmt(v) }},
            grid:  {{ color: "#2e3352" }},
          }},
          y2: {{
            position: "right",
            title: {{ display: true, text: "Cumulative", color: "#8891b0", font: {{ size: 11 }} }},
            ticks: {{ color: "#8891b0", font: {{ size: 11 }}, callback: v => fmtAmt(v) }},
            grid:  {{ drawOnChartArea: false }},
          }},
        }},
      }},
    }});

    makeSortable(document.getElementById("timeline-table"));
  }}

  // Handle URL param on load
  const params = new URLSearchParams(location.search);
  const initCoId = params.get("candidate");
  if (initCoId) {{
    select.value = initCoId;
    // Switch to this tab if coming from map
    if (params.get("tab") === "timeline") {{
      document.querySelector('[data-panel="timeline"]').click();
    }}
    render();
  }} else {{
    content.innerHTML = '<div class="empty">Select a candidate to view their fundraising timeline.</div>';
  }}
}})();

// ============================================================
// TAB 3: EXPENDITURES
// ============================================================
(function() {{
  const select = document.getElementById("expenses-select");
  const typeFilter = document.getElementById("expenses-type-filter");
  const content = document.getElementById("expenses-content");

  // Populate expense type filter
  const allTypes = new Set();
  Object.values(EXPENDITURES).forEach(rows => rows.forEach(r => allTypes.add(r.type)));
  Array.from(allTypes).sort().forEach(t => {{
    const opt = document.createElement("option");
    opt.value = t; opt.textContent = t;
    typeFilter.appendChild(opt);
  }});

  select.addEventListener("change", () => {{
    const tlSel = document.getElementById("timeline-select");
    if (tlSel.value !== select.value) tlSel.value = select.value;
    render();
  }});
  typeFilter.addEventListener("change", render);

  function render() {{
    const coId = select.value;
    if (!coId) {{
      content.innerHTML = '<div class="empty">Select a candidate to view their expenditures.</div>';
      return;
    }}

    const rows = EXPENDITURES[coId] || [];
    const tl = TIMELINES[coId];
    const candName = tl?.candidate_name ?? select.options[select.selectedIndex]?.text ?? coId;
    const party = tl?.party ?? "";
    const partyDotClass = partyClass(party);

    const typeVal = typeFilter.value;
    const filtered = typeVal ? rows.filter(r => r.type === typeVal) : rows;
    const total = filtered.reduce((s, r) => s + r.amount, 0);

    // Loan info summary
    const loanData = LOANS[coId];
    let loanHtml = "";
    if (loanData && loanData.originals.length) {{
      const totalLoaned = loanData.originals.reduce((s,l) => s + l.amount, 0);
      const totalRepaid = loanData.payments.reduce((s,p) => s + p.amount, 0);
      const balance = loanData.originals.reduce((s,l) => s + l.balance, 0);
      loanHtml = `<div class="stat-card">
        <div class="stat-label">Loans Received</div>
        <div class="stat-value" style="color:var(--yellow)">${{fmtAmt(totalLoaned)}}</div>
        <div style="color:var(--text2);font-size:12px;margin-top:2px">Balance: ${{fmtAmt(balance)}}</div>
      </div>`;
    }}

    content.innerHTML = `
      <div class="cand-meta">
        <div class="cand-name">
          <span class="party-dot ${{partyDotClass}}"></span>
          ${{esc(candName)}}
        </div>
        ${{tl?.district_label ? `<div class="cand-info">${{esc(tl.district_label)}}</div>` : ""}}
      </div>
      <div class="stats-row">
        <div class="stat-card">
          <div class="stat-label">${{typeVal || "Total"}} Expenditures</div>
          <div class="stat-value" style="color:var(--red)">${{fmtAmt(total)}}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Line Items</div>
          <div class="stat-value">${{filtered.length}}</div>
        </div>
        ${{loanHtml}}
      </div>
      ${{filtered.length === 0
        ? '<div class="empty">No expenditure records found.</div>'
        : `<div class="table-wrap">
          <table id="exp-table">
            <thead><tr>
              <th data-col="0">Date</th>
              <th data-col="1">Vendor</th>
              <th data-col="2">Amount</th>
              <th data-col="3">Category</th>
              <th data-col="4">Payment</th>
              <th data-col="5">Notes</th>
            </tr></thead>
            <tbody>
              ${{filtered.map(r => `<tr>
                <td class="dim">${{r.date}}</td>
                <td>${{esc(r.vendor)}} <span style="color:var(--text2);font-size:11px">${{esc(r.city)}}, ${{esc(r.state)}}</span></td>
                <td class="amt" data-val="${{r.amount}}">${{fmtAmtFull(r.amount)}}</td>
                <td class="dim">${{esc(r.type)}}</td>
                <td class="dim">${{esc(r.payment)}}</td>
                <td class="dim" style="max-width:280px;white-space:pre-wrap;word-break:break-word;font-size:12px">${{esc(r.notes)}}</td>
              </tr>`).join("")}}
            </tbody>
            <tfoot><tr>
              <td colspan="2">Total</td>
              <td class="amt">${{fmtAmtFull(total)}}</td>
              <td colspan="3"></td>
            </tr></tfoot>
          </table>
        </div>`
      }}`;

    if (filtered.length > 0) {{
      makeSortable(document.getElementById("exp-table"));
    }}
  }}

  const params = new URLSearchParams(location.search);
  const initCoId = params.get("candidate");
  if (initCoId) {{
    select.value = initCoId;
    if (params.get("tab") === "expenses") {{
      document.querySelector('[data-panel="expenses"]').click();
    }}
    render();
  }} else {{
    content.innerHTML = '<div class="empty">Select a candidate to view their expenditures.</div>';
  }}
}})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    MAP_DIR.mkdir(exist_ok=True)

    print("Loading tracked committees...")
    tracked_by_name = load_tracked_committees()
    print(f"  {len(tracked_by_name)} committees from tracer CSVs")

    # coid_map is built up during contribution loading
    coid_map = {}  # co_id -> committee_meta

    print("\nLoading contributions...")
    contributions_list, timelines = load_contributions(tracked_by_name, coid_map)
    print(f"  Resolved {len(coid_map)} CO_IDs for tracked committees")
    print(f"  Timeline data for {len(timelines)} candidates")

    print("\nLoading expenditures...")
    expenditures = load_expenditures(coid_map)

    print("\nLoading loans...")
    loans = load_loans(coid_map)

    print(f"\nGenerating {OUTPUT_HTML}...")
    html = generate_html(contributions_list, timelines, expenditures, loans, coid_map)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = OUTPUT_HTML.stat().st_size // 1024
    print(f"  ✓ Finance page → {OUTPUT_HTML} ({size_kb} KB)")
    print("\nOpen in browser: open CHASERMap/map/finance.html")
    print("Or via server:   http://localhost:8731/finance.html")


if __name__ == "__main__":
    main()
