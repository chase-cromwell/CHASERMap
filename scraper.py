#!/usr/bin/env python3
"""
scraper.py — Automated data collection from Colorado's TRACER campaign finance system.

TRACER (tracer.sos.colorado.gov) is the Colorado Secretary of State's public
campaign finance reporting portal.  It does not offer a public API, so this
script uses Playwright to drive a headless (or headed) Chromium browser to:

  1. Navigate to the Political Race Search page
  2. Set the Jurisdiction → Election → Office filters via dropdown menus
  3. For each district, trigger a search and download the CSV export
  4. Merge the financial data with candidate listing files (which provide
     party affiliation, committee name, and listing status)
  5. Write a single master CSV per chamber to data/

The site uses ASP.NET WebForms with __doPostBack for form submissions.
Each dropdown change triggers a full-page postback, so the script uses
Playwright's expect_navigation() to correctly await the round-trip before
proceeding to the next interaction.

Operating modes (controlled via command-line flags):
    python3 scraper.py                  # scrape all Senate + House districts
    python3 scraper.py --statewide      # scrape statewide executive offices
    python3 scraper.py --reprocess      # re-merge listing data without re-scraping
    python3 scraper.py --discover       # print TRACER's current office code map

Output files:
    data/tracer_2026_all_districts.csv  # legislative candidates (all districts)
    data/tracer_2026_statewide.csv      # statewide office candidates

Prerequisites:
    pip install playwright && playwright install chromium
"""

import asyncio
import csv
import io
import re
import sys
from pathlib import Path

from playwright.async_api import Download, async_playwright

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR    = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "tracer_2026_all_districts.csv"
SEARCH_URL  = "https://tracer.sos.colorado.gov/PublicSite/SearchPages/PoliticalRaceSearch.aspx"

# TRACER dropdown option values for the Jurisdiction and Election filters.
# These are static for the 2026 cycle but may change in future years.
# Use --discover mode to find current values if the site is updated.
JURISDICTION_VALUE = "99"    # "STATEWIDE" — covers all CO districts
ELECTION_VALUE     = "463"   # "2026 NOVEMBER ELECTION"

# Paths to the candidate listing CSVs downloaded from the CO SOS candidate search.
# These provide party affiliation, committee name, and listing status — data that
# TRACER's race search does not include in its CSV export.
CANDIDATE_FILES = {
    "Senate":    DATA_DIR / "rpt_CF_CAND_001.csv",
    "House":     DATA_DIR / "rpt_CF_CAND_001-2.csv",
    "Statewide": DATA_DIR / "rpt_CF_CAND_001-statewide.csv",
}

# TRACER's ddlOffice dropdown values for each legislative chamber.
# These are used to set the Office filter before selecting a district.
CHAMBER_OFFICE_VALUES = {
    "Senate": "6",
    "House":  "7",
}

# Statewide executive office dropdown values in TRACER.
# Discovered via: python3 scraper.py --discover
# Update this dict at the start of each election cycle if the values change.
STATEWIDE_OFFICES: dict[str, str] = {
    "Governor":           "1",
    "Lt. Governor":       "2",
    "Secretary of State": "3",
    "State Treasurer":    "4",
    "Attorney General":   "5",
}

STATEWIDE_OUTPUT_FILE = DATA_DIR / "tracer_2026_statewide.csv"

# Delay between district scrapes (milliseconds) to avoid overwhelming TRACER.
# Increase this if you see intermittent failures or rate-limit errors.
REQUEST_DELAY_MS = 800

# ---------------------------------------------------------------------------
# Candidate listing loader
# ---------------------------------------------------------------------------

# Regex to strip generational suffixes before name matching.
# Both TRACER and the candidate listing may include/exclude these, so we
# strip them from both sides before comparing.
_SUFFIX_RE = re.compile(r'\b(JR\.?|SR\.?|II|III|IV|V)\b\.?', re.IGNORECASE)

def normalize_name(name: str) -> str:
    """Reduce a candidate name to "LAST, FIRST" for cross-source matching.

    Strips middle names/initials and generational suffixes (Jr., Sr., II, etc.)
    which TRACER sometimes includes but the candidate listing omits, and
    vice-versa.

    Args:
        name: Raw candidate name in "LAST, FIRST [MIDDLE] [SUFFIX]" format.

    Returns:
        Normalized string "LAST, FIRST" in all-caps with suffix stripped.
    """
    name = name.strip().upper()
    parts = name.split(",", 1)
    if len(parts) == 2:
        last  = _SUFFIX_RE.sub("", parts[0]).strip()
        first = parts[1].strip().split()[0] if parts[1].strip() else ""
        return f"{last}, {first}"
    return name


def normalize_district(label: str) -> str:
    """'HOUSE DISTRICT 44' and 'House District 44' → 'HOUSE DISTRICT 44'"""
    return label.strip().upper()


def load_candidate_listings() -> dict:
    """Read the candidate listing CSVs and build a cross-reference lookup.

    The candidate listing files (rpt_CF_CAND_001*.csv) come from the
    Colorado SOS candidate search and contain party affiliation, committee
    name, and listing status — information not available in TRACER's race
    search export.  This function builds a keyed lookup so merge_with_listing()
    can join the two data sources.

    File format note:
        These files have a 3-row preamble (filter summary, values, blank line)
        before the actual CSV header.  The reader skips lines[0:3] and treats
        lines[3] as the header row.

    Returns:
        (lookup, districts) where:

        lookup — { (normalized_name, normalized_district): {
                    Party, CommitteeName, AcceptedVSL, ListingStatus
                  } }
                  Keys are (upper_last_first, UPPER_DISTRICT_LABEL) pairs.

        districts — { "Senate": ["Senate District 1", "Senate District 2", ...],
                       "House":  ["House District 1", ...] }
                    Districts are sorted numerically by the trailing number.
                    Used by main() to drive the district-by-district scrape loop.
    """
    lookup   = {}
    districts = {}  # chamber → list of DistrictLabel strings (original case)

    for chamber, path in CANDIDATE_FILES.items():
        if not path.exists():
            print(f"⚠  Candidate file not found: {path}")
            continue

        with open(path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        # Files have a 3-row preamble: filter summary row, values row, blank line.
        # The actual CSV header + data starts at lines[3].
        reader = csv.DictReader(lines[3:])
        chamber_districts = []

        for row in reader:
            cand_key     = normalize_name(row["CandidateName"])
            district_key = normalize_district(row["DistrictName"])

            lookup[(cand_key, district_key)] = {
                "Party":         row["Textbox9"],
                "CommitteeName": row["Textbox13"],
                "AcceptedVSL":   row["AcceptedVSL"],
                "ListingStatus": row["OrganizationStatus"],
            }

            # Collect unique district labels per chamber (preserve original casing)
            orig_district = row["DistrictName"].strip()
            if orig_district not in chamber_districts:
                chamber_districts.append(orig_district)

        # Statewide offices have no numeric suffix — only sort legislative chambers
        if chamber != "Statewide":
            districts[chamber] = sorted(
                chamber_districts,
                key=lambda d: int(re.search(r"(\d+)$", d).group(1))
            )
        print(f"  Loaded {len(chamber_districts):3d} districts, "
              f"{sum(1 for k in lookup if k[1].startswith(chamber.upper()[:6]))}"
              f" candidates from {path.name}")

    return lookup, districts


# ---------------------------------------------------------------------------
# Browser helpers
# ---------------------------------------------------------------------------

def extract_district_number(district_text: str) -> str:
    """Extract the numeric district number from a TRACER district label.

    Example: "Senate District 07" → "7"  (leading zeros stripped via int())
    """
    m = re.search(r"(\d+)\s*$", district_text.strip())
    return str(int(m.group(1))) if m else district_text


async def select_and_wait(page, selector: str, value: str) -> None:
    """Select a <select> dropdown value and await the resulting ASP.NET postback.

    TRACER's form uses ASP.NET WebForms.  Each dropdown change fires an
    `__doPostBack()` call via setTimeout(..., 0), which causes a full-page
    navigation.  We must set up expect_navigation() BEFORE triggering the
    change event — otherwise Playwright may miss the navigation that fires
    asynchronously on the next tick.
    """
    async with page.expect_navigation(wait_until="networkidle"):
        await page.locator(selector).select_option(value=value)


async def setup_filters(page, office_value: str) -> None:
    """Apply the three top-level search filters on the TRACER race search page.

    Must be called after navigating to SEARCH_URL and waiting for networkidle.
    Applies filters in order: Jurisdiction → Election → Office.
    Each step triggers an ASP.NET postback that reloads part of the form.
    """
    await select_and_wait(page, 'select[name*="ddlJurisdiction"]', JURISDICTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlElection"]',     ELECTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlOffice"]',       office_value)


async def get_tracer_district_map(page) -> dict:
    """Read all options from the TRACER district dropdown and build a lookup dict.

    After setting the Office filter, the District dropdown is populated with
    the districts TRACER knows about for that office.  This dict maps the
    normalized district label → { value, text } so we can look up the option
    value needed to select a given district.

    Returns:
        { "SENATE DISTRICT 3": {"value": "42", "text": "Senate District 3"}, ... }
    """
    options = await page.locator('select[name*="ddlDistrict"] option').all()
    result = {}
    for opt in options:
        value = await opt.get_attribute("value")
        text  = (await opt.inner_text()).strip()
        if value and "Select" not in text:
            result[normalize_district(text)] = {"value": value, "text": text}
    return result


async def discover_statewide_offices(page) -> dict[str, str]:
    """
    Navigate to TRACER search, set Jurisdiction + Election, then read all
    <option> values from ddlOffice.  Excludes Senate (6) and House (7).
    Returns {office_label: dropdown_value}.
    """
    await page.goto(SEARCH_URL)
    await page.wait_for_load_state("networkidle")
    await select_and_wait(page, 'select[name*="ddlJurisdiction"]', JURISDICTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlElection"]',     ELECTION_VALUE)
    options = await page.locator('select[name*="ddlOffice"] option').all()
    result = {}
    for opt in options:
        value = await opt.get_attribute("value")
        text  = (await opt.inner_text()).strip()
        if value and "Select" not in text and value not in ("6", "7"):
            result[text] = value
    return result


async def scrape_statewide_office(page, office_label: str, office_value: str) -> list[dict]:
    """Search a statewide office (no district selection) and return raw CSV rows."""
    await page.goto(SEARCH_URL)
    await page.wait_for_load_state("networkidle")
    await select_and_wait(page, 'select[name*="ddlJurisdiction"]', JURISDICTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlElection"]',     ELECTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlOffice"]',       office_value)

    await page.locator('input[id*="btnSearch"], input[value="Search"]').click()
    await page.wait_for_load_state("networkidle")

    if await page.locator('text="0 matching record"').count() > 0:
        return []

    async with page.expect_download(timeout=20_000) as dl_info:
        await page.locator("#_ctl0_Content_ucExport_ibtnCSV").click()
    download: Download = await dl_info.value

    csv_path = await download.path()
    if not csv_path:
        raise RuntimeError("Download path was None")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        content = f.read()

    rows = []
    for row in csv.DictReader(io.StringIO(content)):
        row["Chamber"]        = "Statewide"
        row["DistrictNumber"] = "0"
        row["DistrictLabel"]  = office_label
        rows.append(row)
    return rows


async def scrape_district(page, district_value: str, district_text: str, chamber: str) -> list[dict]:
    """Scrape financial data for one legislative district and return raw CSV rows.

    Selects the district via JavaScript (direct value assignment + change event)
    rather than select_and_wait(), because the district dropdown's onchange
    handler does NOT trigger a full postback — it just enables the Search button.
    A short wait (300ms) is used instead of awaiting navigation.

    After clicking Search, if no records are found, navigates back and returns [].
    Otherwise downloads the CSV export and parses it into a list of dicts.
    Each row is augmented with Chamber, DistrictNumber, and DistrictLabel fields
    (not present in TRACER's raw CSV export).

    Args:
        district_value: The <option value> string from the TRACER dropdown.
        district_text:  The display text e.g. "Senate District 3".
        chamber:        "Senate" or "House".

    Returns:
        List of raw CSV row dicts with financial columns from TRACER,
        plus the added Chamber/District fields.
    """
    await page.evaluate(
        """(val) => {
            const sel = document.querySelector('select[name*="ddlDistrict"]');
            sel.value = val;
            // Dispatch a synthetic change event to satisfy any listeners,
            // even though this particular dropdown does not trigger a postback.
            sel.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        district_value,
    )
    # Brief wait to ensure any client-side state updates after the change event
    await page.wait_for_timeout(300)

    await page.locator('input[id*="btnSearch"], input[value="Search"]').click()
    await page.wait_for_load_state("networkidle")

    if await page.locator('text="District is required"').count() > 0:
        raise RuntimeError("District validation failed — filter state lost")

    if await page.locator('text="0 matching record"').count() > 0:
        await _go_back(page)
        return []

    async with page.expect_download(timeout=20_000) as dl_info:
        await page.locator("#_ctl0_Content_ucExport_ibtnCSV").click()
    download: Download = await dl_info.value

    csv_path = await download.path()
    if not csv_path:
        raise RuntimeError("Download path was None")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        content = f.read()

    rows = []
    for row in csv.DictReader(io.StringIO(content)):
        row["Chamber"]        = chamber
        row["DistrictNumber"] = extract_district_number(district_text)
        row["DistrictLabel"]  = district_text
        rows.append(row)

    await _go_back(page)
    return rows


async def _go_back(page) -> None:
    await page.locator('input[value*="Change"]').click()
    await page.wait_for_load_state("networkidle")


# ---------------------------------------------------------------------------
# Merge: join TRACER financial rows with candidate listing data
# ---------------------------------------------------------------------------

def merge_with_listing(tracer_rows: list[dict], listing: dict) -> list[dict]:
    """Join TRACER financial data with the candidate listing on (name, district).

    TRACER's CSV export provides financial figures but not party affiliation,
    committee name, or listing status.  The candidate listing CSVs provide
    those fields but not financials.  This function joins them by matching
    (normalized_name, normalized_district).

    Fallback logic:
        If no exact (name, district) match is found — which can happen when
        TRACER and the listing use slightly different district label formats —
        the function tries a name-only match across all districts.  If still
        no match, the row is appended with Party="Unknown" and empty strings.

    Args:
        tracer_rows: List of raw TRACER CSV row dicts (with Chamber/District added).
        listing:     { (norm_name, norm_district): {Party, CommitteeName, ...} }

    Returns:
        List of merged row dicts with Party, CommitteeName, AcceptedVSL,
        and ListingStatus added to each row.  Unmatched rows are printed as
        warnings but still included with placeholder values.
    """
    merged = []
    unmatched = []

    for row in tracer_rows:
        name_key     = normalize_name(row["CandName"])
        district_key = normalize_district(row["DistrictLabel"])
        info = listing.get((name_key, district_key))

        if info is None:
            # Fallback: name-only match across all entries (handles rare
            # district label format differences between TRACER and the listing)
            fallback = next(
                (v for (n, d), v in listing.items() if n == name_key),
                None
            )
            if fallback:
                info = fallback
            else:
                unmatched.append(f"{row['CandName']} / {row['DistrictLabel']}")
                info = {"Party": "Unknown", "CommitteeName": "", "AcceptedVSL": "", "ListingStatus": ""}

        row.update(info)
        merged.append(row)

    if unmatched:
        print(f"\n  ⚠  Could not match {len(unmatched)} candidate(s) to listing:")
        for u in unmatched:
            print(f"     {u}")

    return merged


# ---------------------------------------------------------------------------
# Reprocess: re-merge existing scraped data without hitting TRACER
# ---------------------------------------------------------------------------

# Columns sourced from the candidate listing files (not from TRACER).
# These are stripped before re-merging so that updated listing data is
# used instead of the stale values from the previous scrape.
LISTING_COLS = {"Party", "CommitteeName", "AcceptedVSL", "ListingStatus"}

def reprocess() -> None:
    """Re-merge the existing output CSV with updated candidate listing files.

    Use this when the candidate listing CSVs have been refreshed (e.g. new
    party registrations or committee name changes) but you don't want to
    re-scrape all of TRACER.  The financial data is preserved; only the
    listing-derived columns (Party, CommitteeName, AcceptedVSL, ListingStatus)
    are replaced.

    Run with:  python3 scraper.py --reprocess
    """
    if not OUTPUT_FILE.exists():
        print(f"⚠  No existing output file found at {OUTPUT_FILE}")
        print("   Run without --reprocess to scrape first.")
        sys.exit(1)

    print("Loading candidate listings...")
    listing, _ = load_candidate_listings()
    print(f"  Total candidates in listing: {len(listing)}\n")

    print(f"Reading existing data from {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  {len(rows)} rows loaded")

    # Strip old listing-derived columns so merge_with_listing re-populates them
    for row in rows:
        for col in LISTING_COLS:
            row.pop(col, None)

    print("\nRe-merging...")
    merged = merge_with_listing(rows, listing)

    front_cols = [
        "Chamber", "DistrictNumber", "DistrictLabel",
        "CandName", "CandidateStatus",
        "Party", "CommitteeName", "AcceptedVSL", "ListingStatus",
    ]
    financial_cols = [k for k in merged[0] if k not in front_cols]
    fieldnames = front_cols + financial_cols

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)

    print(f"\n✓ Rewrote {len(merged)} records → {OUTPUT_FILE}")


# ---------------------------------------------------------------------------
# Statewide scraping
# ---------------------------------------------------------------------------

async def scrape_statewide_main() -> None:
    if not STATEWIDE_OFFICES:
        print("⚠  STATEWIDE_OFFICES is empty.  Run --discover first to find office codes,")
        print("   then fill in the STATEWIDE_OFFICES dict in scraper.py.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading candidate listings (for party merge)...")
    listing, _ = load_candidate_listings()
    print(f"  Total candidates in listing: {len(listing)}\n")

    all_rows: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        for office_label, office_value in STATEWIDE_OFFICES.items():
            print(f"  {office_label} (value={office_value})...", end="", flush=True)
            try:
                raw_rows = await scrape_statewide_office(page, office_label, office_value)
                merged   = merge_with_listing(raw_rows, listing)
                all_rows.extend(merged)
                print(f" → {len(merged)} candidate(s)")
            except Exception as exc:
                print(f" ERROR: {exc}")

        await browser.close()

    if all_rows:
        front_cols = [
            "Chamber", "DistrictNumber", "DistrictLabel",
            "CandName", "CandidateStatus",
            "Party", "CommitteeName", "AcceptedVSL", "ListingStatus",
        ]
        financial_cols = [k for k in all_rows[0] if k not in front_cols]
        fieldnames = front_cols + financial_cols

        with open(STATEWIDE_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)

        print(f"\n✓ Wrote {len(all_rows)} statewide candidate records → {STATEWIDE_OUTPUT_FILE}")
    else:
        print("\n⚠ No statewide rows collected.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading candidate listings...")
    listing, chamber_districts = load_candidate_listings()
    print(f"  Total candidates in listing: {len(listing)}\n")

    all_rows: list[dict] = []
    errors:   list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        for chamber, districts in chamber_districts.items():
            office_value = CHAMBER_OFFICE_VALUES[chamber]

            print(f"\n{'='*50}")
            print(f"  {chamber}  ({len(districts)} districts from listing)")
            print(f"{'='*50}")

            await page.goto(SEARCH_URL)
            await page.wait_for_load_state("networkidle")
            await setup_filters(page, office_value)

            # Build TRACER's internal district-value map for this chamber
            tracer_map = await get_tracer_district_map(page)

            for i, district_label in enumerate(districts):
                district_key = normalize_district(district_label)
                tracer_info  = tracer_map.get(district_key)

                print(f"  [{i+1:3d}/{len(districts)}] {district_label}...", end="", flush=True)

                if not tracer_info:
                    print(f" SKIP (not found in TRACER dropdown)")
                    continue

                try:
                    raw_rows = await scrape_district(
                        page,
                        tracer_info["value"],
                        tracer_info["text"],
                        chamber,
                    )
                    merged = merge_with_listing(raw_rows, listing)
                    all_rows.extend(merged)
                    print(f" → {len(merged)} candidate(s)")

                except Exception as exc:
                    print(f" ERROR: {exc}")
                    errors.append({"chamber": chamber, "district": district_label, "error": str(exc)})

                    try:
                        print(f"    Recovering...", end="", flush=True)
                        await page.goto(SEARCH_URL)
                        await page.wait_for_load_state("networkidle")
                        await setup_filters(page, office_value)
                        tracer_map = await get_tracer_district_map(page)
                        print(" OK")
                    except Exception as re:
                        print(f" RECOVERY FAILED: {re}")

                await page.wait_for_timeout(REQUEST_DELAY_MS)

        await browser.close()

    # -----------------------------------------------------------------------
    # Write master CSV
    # -----------------------------------------------------------------------
    if all_rows:
        # Column order: identity, party/listing, then financials
        front_cols = [
            "Chamber", "DistrictNumber", "DistrictLabel",
            "CandName", "CandidateStatus",
            "Party", "CommitteeName", "AcceptedVSL", "ListingStatus",
        ]
        financial_cols = [
            k for k in all_rows[0]
            if k not in front_cols
        ]
        fieldnames = front_cols + financial_cols

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)

        print(f"\n✓ Wrote {len(all_rows)} candidate records → {OUTPUT_FILE}")
    else:
        print("\n⚠ No rows collected.")

    if errors:
        print(f"\n⚠ {len(errors)} district(s) failed:")
        for e in errors:
            print(f"   {e['chamber']:6s} {e['district']}: {e['error']}")
        sys.exit(1)


if __name__ == "__main__":
    if "--reprocess" in sys.argv:
        reprocess()
    elif "--discover" in sys.argv:
        async def _discover():
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                page    = await (await browser.new_context()).new_page()
                offices = await discover_statewide_offices(page)
                await browser.close()
            print("\nStatewide office codes found in TRACER:")
            for label, value in sorted(offices.items(), key=lambda x: int(x[1])):
                print(f"  {value:>4s}  {label}")
            print("\nCopy the desired entries into STATEWIDE_OFFICES in scraper.py")
        asyncio.run(_discover())
    elif "--statewide" in sys.argv:
        asyncio.run(scrape_statewide_main())
    else:
        asyncio.run(main())
