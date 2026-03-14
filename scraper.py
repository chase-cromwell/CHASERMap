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
    python3 scraper.py --contacts                   # scrape all chambers
    python3 scraper.py --contacts House             # House only
    python3 scraper.py --contacts Senate Statewide  # multiple chambers

Output files:
    data/tracer_2026_all_districts.csv  # legislative candidates (all districts)
    data/tracer_2026_statewide.csv      # statewide office candidates
    data/tracer_2026_contacts.csv       # contact info + TRACER IDs + complaints + filings

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
CONTACTS_OUTPUT_FILE  = DATA_DIR / "tracer_2026_contacts.csv"

# URLs for the contact-detail scraping mode
COMMITTEE_SEARCH_URL = "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CommitteeSearch.aspx"
CONTACT_DETAIL_BASE  = "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx"

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
# Contact-detail scraping helpers  (--contacts mode)
# ---------------------------------------------------------------------------

async def get_org_id_for_committee(page, committee_name: str) -> str | None:
    """Search CommitteeSearch.aspx by committee name and return the OrgID of
    the first matching result.

    The OrgID is the internal TRACER identifier embedded in every
    CandidateDetail.aspx link, e.g. ?OrgID=49405.  It is not available in
    any CSV export and must be obtained by navigating the committee search.

    Strategy:
        1. Fill the committee-name input with the exact name.
        2. Submit the form and wait for networkidle.
        3. Scan all anchor hrefs for the OrgID= query parameter.
           First, try to find an <a> whose link text exactly matches the
           committee name (avoids false positives when the search returns
           multiple committees with similar names).
        4. Fall back to the first OrgID-bearing href on the page.

    Returns the OrgID string (e.g. "49405"), or None if not found.
    """
    await page.goto(COMMITTEE_SEARCH_URL)
    await page.wait_for_load_state("networkidle")

    # Fill committee name.  Use the specific text-input ID to avoid the
    # radio-button inputs whose names also contain "CommitteeName".
    await page.locator('#_ctl0_Content_txtCommitteeName').fill(committee_name)

    # The Search button triggers a full ASP.NET page postback (form POST).
    # Wrap in expect_navigation so we wait for the round-trip to complete
    # before scanning results — plain wait_for_load_state() can return
    # immediately if the page was already at networkidle before the click.
    async with page.expect_navigation(wait_until="networkidle"):
        await page.locator('input[value="Search"]').click()

    # Search the full page HTML for any OrgID pattern.
    # The OrgID appears somewhere in the DOM after a search — in onclick
    # attributes, __doPostBack arguments, hidden inputs, or data attributes —
    # even when anchor hrefs use javascript: postbacks instead of direct URLs.
    # Using outerHTML catches every attribute and text node in one pass.
    org_id = await page.evaluate(r"""() => {
        const html = document.documentElement.outerHTML;
        // Match OrgID= followed immediately by digits (href, onclick, hidden
        // field values, JavaScript literals, etc.)
        const m = html.match(/OrgID[='":\s]+(\d{3,6})/i);
        return m ? m[1] : null;
    }""")
    if org_id:
        return org_id

    # If the search results page genuinely contains no OrgID anywhere, try
    # clicking the first result row and extracting from the detail page HTML.
    result_link = None
    for link in await page.locator('td a').all():
        href = (await link.get_attribute("href") or "").lower()
        if href.startswith("http"):
            continue          # nav / breadcrumb — skip
        result_link = link
        break

    if result_link is None:
        return None

    async with page.expect_navigation(wait_until="networkidle"):
        await result_link.click()

    # Try URL first, then fall back to searching the detail page HTML.
    m = re.search(r'OrgID=(\d+)', page.url, re.IGNORECASE)
    if m:
        return m.group(1)

    return await page.evaluate(r"""() => {
        const html = document.documentElement.outerHTML;
        const m = html.match(/OrgID[='":\s]+(\d{3,6})/i);
        return m ? m[1] : null;
    }""")


_TRACER_BASE = "https://tracer.sos.colorado.gov/PublicSite/SearchPages"

_JS_NEXT_PAGE = r"""(id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    const trs = [...el.querySelectorAll('tr')];
    const last = trs[trs.length - 1];
    if (!last) return null;
    const span = last.querySelector('span');
    if (!span) return null;
    const cur = parseInt(span.innerText.trim(), 10);
    if (isNaN(cur)) return null;
    const links = [...last.querySelectorAll('a')];
    return links.some(a => parseInt(a.innerText.trim(), 10) === cur + 1) ? cur + 1 : null;
}"""


async def _next_grid_page(page, grid_id: str) -> bool:
    """Click the next-page link for a DataGrid if one exists.  Returns True if clicked."""
    next_num = await page.evaluate(_JS_NEXT_PAGE, grid_id)
    if next_num is None:
        return False
    next_link = page.locator(f'#{grid_id} tr:last-child a').filter(
        has_text=re.compile(rf'^{next_num}$')
    )
    async with page.expect_navigation(wait_until="networkidle"):
        await next_link.click()
    return True


async def _collect_filing_rows(page) -> list[dict]:
    """Collect all filing history rows, resolving each to its FilingDetail URL.

    Filing description links use __doPostBack and redirect to
    FilingDetail.aspx?FilingID=XXXXX.  We follow all postbacks in parallel
    via fetch() inside the browser using the existing authenticated session,
    so no Playwright navigation is needed.

    Handles pagination: if the grid still has a next page after setting the
    page size to 50, this function clicks through all remaining pages.
    """
    GRID_ID = "_ctl0_Content_dgdFilingHistory"

    # Async JS: collect rows + link targets, then parallel-fetch all FilingIDs.
    _JS = r"""async (gridId) => {
        const el = document.getElementById(gridId);
        if (!el) return [];
        const form = document.forms[0];

        const rowData = [];
        el.querySelectorAll('tr').forEach((tr, i) => {
            if (i === 0) return;
            const cells = [...tr.querySelectorAll('td')];
            if (cells.length < 2) return;
            const link = tr.querySelector('a[href*="lnkFilingHist"]');
            const m = link?.getAttribute('href')?.match(/'([^']+)'/);
            rowData.push({ cells: cells.map(c => c.innerText.trim()), target: m?.[1] || null });
        });

        const urls = await Promise.all(rowData.map(async ({target}) => {
            if (!target) return '';
            const data = new FormData(form);
            data.set('__EVENTTARGET',  target);
            data.set('__EVENTARGUMENT', '');
            try {
                const resp = await fetch(form.action, {method:'POST', body:data, redirect:'follow'});
                const fid  = new URL(resp.url).searchParams.get('FilingID');
                return fid ? `__BASE__/FilingDetail.aspx?FilingID=${fid}` : '';
            } catch(e) { return ''; }
        }));

        return rowData.map((r, i) => [r.cells, urls[i]]);
    }""".replace("__BASE__", _TRACER_BASE)

    all_rows: list[dict] = []
    while True:
        page_data = await page.evaluate(_JS, GRID_ID)
        for cells, url in page_data:
            if len(cells) >= 8:
                all_rows.append({
                    "committee":    cells[0],
                    "description":  cells[1],
                    "period_begin": cells[2],
                    "period_end":   cells[3],
                    "due_date":     cells[4],
                    "filed_on":     cells[5],
                    "amended":      cells[6],
                    "status":       cells[7],
                    "url":          url or "",
                })
        if not await _next_grid_page(page, GRID_ID):
            break
    return all_rows


async def _collect_complaint_rows(page) -> list[dict]:
    """Collect all complaints rows, extracting each ComplaintDetail URL.

    Complaint case-number links use __doPostBack but the destination URL
    (ComplaintDetail.aspx?ID=XXXX) is embedded in the page's raw HTML as
    an onclick attribute value.  We regex-extract the IDs from page.content()
    and zip them with the table rows — they appear in the same order.

    Handles pagination across multiple pages of the complaints grid.
    """
    GRID_ID = "_ctl0_Content_dgdComplaints"

    _JS_ROWS = r"""(id) => {
        const el = document.getElementById(id);
        if (!el) return [];
        const out = [];
        el.querySelectorAll('tr').forEach((tr, i) => {
            if (i === 0) return;
            const cells = [...tr.querySelectorAll('td')];
            if (cells.length < 2) return;
            out.push(cells.map(c => c.innerText.trim()));
        });
        return out;
    }"""

    all_rows: list[dict] = []
    while True:
        rows = await page.evaluate(_JS_ROWS, GRID_ID)
        html = await page.content()
        ids  = re.findall(r'ComplaintDetail\.aspx\?ID=(\d+)', html)
        base = f"{_TRACER_BASE}/ComplaintDetail.aspx?ID="

        for i, cells in enumerate(rows):
            if len(cells) >= 5:
                all_rows.append({
                    "committee":   cells[0],
                    "case_number": cells[1],
                    "date_filed":  cells[2],
                    "complainant": cells[3],
                    "subject":     cells[4],
                    "status":      cells[5] if len(cells) > 5 else "",
                    "url":         (base + ids[i]) if i < len(ids) else "",
                })
        if not await _next_grid_page(page, GRID_ID):
            break
    return all_rows


async def _collect_all_grid_pages(page, grid_id: str) -> list[list[str]]:
    """Collect all rows from a paginated TRACER DataGrid (plain cell text only).

    Used for grids where links are not needed (campaigns, filings_due).
    For filing history and complaints use the specialised collectors above.
    """
    _JS_ROWS = r"""(id) => {
        const el = document.getElementById(id);
        if (!el) return [];
        const out = [];
        el.querySelectorAll('tr').forEach((tr, i) => {
            if (i === 0) return;
            const cells = [...tr.querySelectorAll('td')];
            if (cells.length < 2) return;
            out.push(cells.map(c => c.innerText.trim()));
        });
        return out;
    }"""

    all_rows: list[list[str]] = []
    while True:
        rows = await page.evaluate(_JS_ROWS, grid_id)
        all_rows.extend(rows)
        if not await _next_grid_page(page, grid_id):
            break
    return all_rows


async def extract_candidate_detail(page) -> dict:
    """Extract every available field from a loaded CandidateDetail.aspx page.

    Steps:
      1. Set page-size dropdowns to 50 (max) for the three paginated grids
         so that most candidates need only one page per grid.
      2. Iterate through any remaining pages of each grid via
         _collect_all_grid_pages().
      3. Extract all scalar fields in a single page.evaluate() call.

    NOTE: TRACER grids use the 'dgd' prefix (DataGrid), not 'gdv' (GridView).
    """
    # ------------------------------------------------------------------
    # Step 1: maximise page sizes so pagination is rarely needed
    # ------------------------------------------------------------------
    PAGE_SIZE_DROPDOWNS = [
        '_ctl0_Content_dgdFilingHistory__ctl8_dgdFilingHistoryPageSizeDropDown',
        '_ctl0_Content_dgdFilingsDue__ctl8_dgdFilingsDuePageSizeDropDown',
        '_ctl0_Content_dgdComplaints__ctl8_dgdComplaintsPageSizeDropDown',
    ]
    for dd_id in PAGE_SIZE_DROPDOWNS:
        el = page.locator(f'#{dd_id}')
        if await el.count() > 0 and await el.input_value() != '50':
            async with page.expect_navigation(wait_until="networkidle"):
                await el.select_option('50')

    # ------------------------------------------------------------------
    # Step 2: collect all rows from each paginated grid
    # Filing history and complaints use specialised collectors that also
    # resolve hyperlinks.  Campaigns and filings-due need plain text only.
    # ------------------------------------------------------------------
    filing_rows = await _collect_filing_rows(page)
    comp_rows   = await _collect_complaint_rows(page)
    due_rows    = await _collect_all_grid_pages(page, '_ctl0_Content_dgdFilingsDue')
    camp_rows   = await _collect_all_grid_pages(page, '_ctl0_Content_dgdCampaigns')

    # ------------------------------------------------------------------
    # Step 3: extract all scalar fields in one JS round-trip
    # ------------------------------------------------------------------
    scalar = await page.evaluate(r"""() => {
        function t(id) {
            const el = document.getElementById(id);
            return el ? el.innerText.trim() : '';
        }
        function addr(...ids) {
            return ids.map(id => t(id)).filter(Boolean).join(', ');
        }
        return {
            org_id:              new URLSearchParams(window.location.search).get('OrgID') || '',
            candidate_id:        t('_ctl0_Content_lblCandidateID'),
            committee_id:        t('_ctl0_Content_lblCommitteeID'),
            cand_name:           t('_ctl0_Content_lblCandName'),
            cand_mail_address:   addr(
                '_ctl0_Content_lblCandMailAddress1',
                '_ctl0_Content_lblCandMailAddress2',
                '_ctl0_Content_lblCandMailCityStateZip',
            ),
            cand_status:         t('_ctl0_Content_lblCandStatus'),
            campaign_status:     t('_ctl0_Content_lblCampaignStatus'),
            cand_phone:          t('_ctl0_Content_lblCandPhone'),
            cand_fax:            t('_ctl0_Content_lblCandFax'),
            date_affidavit_filed: t('_ctl0_Content_lblCandDateDeclared'),
            email:               t('_ctl0_Content_lnkCandEmail'),
            jurisdiction:        t('_ctl0_Content_lblCandJurisdiction'),
            web:                 t('_ctl0_Content_lnkCandWeb') || t('_ctl0_Content_lnkCommWeb'),
            party:               t('_ctl0_Content_lblCandParty'),
            vsl:                 t('_ctl0_Content_lblCandVolSpendLimit'),
            office:              t('_ctl0_Content_lblCandOffice'),
            comm_name:           t('_ctl0_Content_lblCommName'),
            comm_type:           t('_ctl0_Content_lblCommitteeType'),
            comm_phys_address:   addr(
                '_ctl0_Content_lblCommPhysAddress1',
                '_ctl0_Content_lblCommPhysAddress2',
                '_ctl0_Content_lblCommPhysCityStateZip',
            ),
            comm_mail_address:   addr(
                '_ctl0_Content_lblCommMailAddress1',
                '_ctl0_Content_lblCommMailAddress2',
                '_ctl0_Content_lblCommMailCityStateZip',
            ),
            comm_status:         t('_ctl0_Content_lblCommStatus'),
            date_registered:     t('_ctl0_Content_lblCommDateOrganized'),
            date_terminated:     t('_ctl0_Content_lblCommDateTerminated'),
            comm_phone:          t('_ctl0_Content_lblCommPhone'),
            comm_fax:            t('_ctl0_Content_lblCommFax'),
            comm_web:            t('_ctl0_Content_lnkCommWeb'),
            purpose:             t('_ctl0_Content_lblCommPurpose'),
            registered_agent:    t('_ctl0_Content_lblRegisteredAgent'),
            agent_phone:         t('_ctl0_Content_lblAgentPhone'),
            agent_email:         t('_ctl0_Content_lnkAgentEmail'),
            dfa:                 t('_ctl0_Content_lblDFA'),
            dfa_phone:           t('_ctl0_Content_lblDFAPhone'),
            dfa_email:           t('_ctl0_Content_lnkDFAEmail'),
            fin_as_of:           t('_ctl0_Content_lblFilingName'),
            fin_period_end:      t('_ctl0_Content_lblFilingPeriodEndDate'),
            fin_filed_date:      t('_ctl0_Content_lblFilingFiledDate'),
            election_cycle:      t('_ctl0_Content_lblElectionCycleName'),
            cand_expenditures:   t('_ctl0_Content_lblCandidateExpenditures_EC'),
            beginning_balance:   t('_ctl0_Content_lblBeginningBalance_EC'),
            total_contributions: t('_ctl0_Content_lblTotalCont_EC'),
            total_loans_received: t('_ctl0_Content_lblTotalLoansRcvd_EC'),
            total_expenditures:  t('_ctl0_Content_lblTotalExp_EC'),
            total_loans_repaid:  t('_ctl0_Content_lblTotalLoansRepaid_EC'),
            ending_balance:      t('_ctl0_Content_lblEndingBalance_EC'),
            non_mon_contributions: t('_ctl0_Content_lblNonMonContr_EC'),
            non_mon_expenditures:  t('_ctl0_Content_lblNonMonExp_EC'),
        };
    }""")

    # ------------------------------------------------------------------
    # Step 4: shape plain-text table rows into structured dicts.
    # filing_rows and comp_rows are already shaped by their collectors.
    # ------------------------------------------------------------------
    def shape_due(rows):
        return [{"committee": c[0], "description": c[1], "period_begin": c[2],
                 "period_end": c[3], "due_date": c[4]}
                for c in rows if len(c) >= 5]

    def shape_campaigns(rows):
        return [{"committee": c[0], "election_cycle": c[1], "party": c[2],
                 "jurisdiction": c[3], "office": c[4],
                 "district": c[5] if len(c) > 5 else "",
                 "status":   c[6] if len(c) > 6 else ""}
                for c in rows if len(c) >= 5]

    return {
        **scalar,
        "filings":     filing_rows,          # already dicts with "url" field
        "filings_due": shape_due(due_rows),
        "complaints":  comp_rows,            # already dicts with "url" field
        "campaigns":   shape_campaigns(camp_rows),
    }


async def scrape_contacts_main(chambers: list[str] | None = None) -> None:
    """Scrape contact and detail data for every candidate that has a committee.

    Reads both master CSVs (legislative + statewide) produced by the main
    scraper modes.  For each unique committee name it:
        1. Searches CommitteeSearch.aspx to find the internal OrgID.
        2. Navigates to CandidateDetail.aspx?OrgID=<id>&Type=CO.
        3. Extracts all available fields via extract_candidate_detail().
        4. Writes results to data/tracer_2026_contacts.csv.

    De-duplicates by committee name so shared committees are only fetched
    once.  Candidates without a committee name ("None") are skipped — they
    rarely have contact info registered in TRACER.

    Args:
        chambers: Optional list of chamber names to restrict scraping, e.g.
                  ["House"], ["Senate", "Statewide"].  None = all chambers.
                  Valid values: "House", "Senate", "Statewide".

    Run with:
        python3 scraper.py --contacts                   # all chambers
        python3 scraper.py --contacts House             # House only
        python3 scraper.py --contacts Senate Statewide  # multiple chambers
    """
    import json as _json

    # Normalise chamber filter for case-insensitive comparison
    chamber_filter: set[str] | None = (
        {c.strip().title() for c in chambers} if chambers else None
    )
    if chamber_filter:
        print(f"  Chamber filter: {', '.join(sorted(chamber_filter))}")

    # ------------------------------------------------------------------
    # Collect all candidates from both master CSVs
    # ------------------------------------------------------------------
    all_rows: list[dict] = []
    for path in (OUTPUT_FILE, STATEWIDE_OUTPUT_FILE):
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                all_rows.extend(csv.DictReader(f))
        else:
            print(f"  ⚠  Not found, skipping: {path.name}")

    if not all_rows:
        print("⚠  No candidate data found — run the main scraper first.")
        return

    # Apply chamber filter before de-duplication
    if chamber_filter:
        all_rows = [r for r in all_rows if r.get("Chamber", "").strip() in chamber_filter]
        if not all_rows:
            print(f"⚠  No candidates found for chamber(s): {', '.join(sorted(chamber_filter))}")
            return

    # ------------------------------------------------------------------
    # De-duplicate: one scrape per unique committee name
    # ------------------------------------------------------------------
    seen:    set[str]   = set()
    targets: list[dict] = []   # representative row per unique committee
    skipped: int        = 0

    for row in all_rows:
        comm = (row.get("CommitteeName") or "").strip()
        if not comm or comm.lower() == "none":
            skipped += 1
            continue
        if comm not in seen:
            seen.add(comm)
            targets.append(row)

    print(f"  {len(all_rows)} total candidates")
    print(f"  {len(targets)} unique committees to scrape  "
          f"({skipped} skipped — no committee name)\n")

    results: list[dict] = []
    errors:  list[dict] = []

    # ------------------------------------------------------------------
    # Playwright scrape loop
    # ------------------------------------------------------------------
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page    = await (await browser.new_context()).new_page()

        for i, row in enumerate(targets):
            comm = row["CommitteeName"].strip()
            print(f"  [{i+1:3d}/{len(targets)}] {comm}...", end="", flush=True)

            try:
                org_id = await get_org_id_for_committee(page, comm)
                if not org_id:
                    print(" ⚠  OrgID not found")
                    errors.append({"committee": comm, "error": "OrgID not found"})
                    continue

                await page.goto(f"{CONTACT_DETAIL_BASE}?OrgID={org_id}&Type=CO")
                await page.wait_for_load_state("networkidle")
                detail = await extract_candidate_detail(page)

                results.append({
                    # Source identifiers
                    "CommitteeName":       comm,
                    "CandName":            row.get("CandName", ""),
                    "DistrictLabel":       row.get("DistrictLabel", ""),
                    # IDs
                    "OrgID":               detail["org_id"],
                    "CandidateID":         detail["candidate_id"],
                    "CommitteeID":         detail["committee_id"],
                    # Candidate info
                    "CandFullName":        detail["cand_name"],
                    "CandMailAddress":     detail["cand_mail_address"],
                    "CandStatus":          detail["cand_status"],
                    "CampaignStatus":      detail["campaign_status"],
                    "Phone":               detail["cand_phone"] or detail["comm_phone"],
                    "CandFax":             detail["cand_fax"],
                    "DateAffidavitFiled":  detail["date_affidavit_filed"],
                    "Email":               detail["email"],
                    "Jurisdiction":        detail["jurisdiction"],
                    "Web":                 detail["web"],
                    "Party":               detail["party"],
                    "VSL":                 detail["vsl"],
                    "Office":              detail["office"],
                    # Committee info
                    "CommName":            detail["comm_name"],
                    "CommType":            detail["comm_type"],
                    "CommPhysAddress":     detail["comm_phys_address"],
                    "CommMailAddress":     detail["comm_mail_address"],
                    "CommStatus":          detail["comm_status"],
                    "DateRegistered":      detail["date_registered"],
                    "DateTerminated":      detail["date_terminated"],
                    "CommPhone":           detail["comm_phone"],
                    "CommFax":             detail["comm_fax"],
                    "CommWeb":             detail["comm_web"],
                    "Purpose":             detail["purpose"],
                    # Agents
                    "RegisteredAgent":     detail["registered_agent"],
                    "AgentPhone":          detail["agent_phone"],
                    "AgentEmail":          detail["agent_email"],
                    "DFA":                 detail["dfa"],
                    "DFAPhone":            detail["dfa_phone"],
                    "DFAEmail":            detail["dfa_email"],
                    # Financial summary
                    "FinAsOf":             detail["fin_as_of"],
                    "FinPeriodEnd":        detail["fin_period_end"],
                    "FinFiledDate":        detail["fin_filed_date"],
                    "ElectionCycle":       detail["election_cycle"],
                    "CandExpenditures":    detail["cand_expenditures"],
                    "BeginningBalance":    detail["beginning_balance"],
                    "TotalContributions":  detail["total_contributions"],
                    "TotalLoansReceived":  detail["total_loans_received"],
                    "TotalExpenditures":   detail["total_expenditures"],
                    "TotalLoansRepaid":    detail["total_loans_repaid"],
                    "EndingBalance":       detail["ending_balance"],
                    "NonMonContributions": detail["non_mon_contributions"],
                    "NonMonExpenditures":  detail["non_mon_expenditures"],
                    # Tables (JSON)
                    "ComplaintCount":      len(detail["complaints"]),
                    "ComplaintsJSON":      _json.dumps(detail["complaints"]),
                    "FilingsJSON":         _json.dumps(detail["filings"]),
                    "FilingsDueJSON":      _json.dumps(detail["filings_due"]),
                    "CampaignsJSON":       _json.dumps(detail["campaigns"]),
                })

                print(
                    f" ✓  web={detail['web'] or '—'} | "
                    f"complaints={len(detail['complaints'])} | "
                    f"filings={len(detail['filings'])}"
                )

            except Exception as exc:
                print(f" ERROR: {exc}")
                errors.append({"committee": comm, "error": str(exc)})
                # Attempt to recover browser to a known-good page
                try:
                    await page.goto(COMMITTEE_SEARCH_URL)
                    await page.wait_for_load_state("networkidle")
                except Exception:
                    pass

            await page.wait_for_timeout(REQUEST_DELAY_MS)

        await browser.close()

    # ------------------------------------------------------------------
    # Write contacts CSV
    # ------------------------------------------------------------------
    if results:
        fieldnames = [
            # Source identifiers
            "CommitteeName", "CandName", "DistrictLabel",
            # IDs
            "OrgID", "CandidateID", "CommitteeID",
            # Candidate info
            "CandFullName", "CandMailAddress", "CandStatus", "CampaignStatus",
            "Phone", "CandFax", "DateAffidavitFiled", "Email",
            "Jurisdiction", "Web", "Party", "VSL", "Office",
            # Committee info
            "CommName", "CommType", "CommPhysAddress", "CommMailAddress",
            "CommStatus", "DateRegistered", "DateTerminated",
            "CommPhone", "CommFax", "CommWeb", "Purpose",
            # Agents
            "RegisteredAgent", "AgentPhone", "AgentEmail",
            "DFA", "DFAPhone", "DFAEmail",
            # Financial summary
            "FinAsOf", "FinPeriodEnd", "FinFiledDate", "ElectionCycle",
            "CandExpenditures", "BeginningBalance", "TotalContributions",
            "TotalLoansReceived", "TotalExpenditures", "TotalLoansRepaid",
            "EndingBalance", "NonMonContributions", "NonMonExpenditures",
            # Tables (JSON)
            "ComplaintCount", "ComplaintsJSON", "FilingsJSON",
            "FilingsDueJSON", "CampaignsJSON",
        ]
        with open(CONTACTS_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✓  Wrote {len(results)} contact records → {CONTACTS_OUTPUT_FILE}")
    else:
        print("\n⚠  No contact records collected.")

    if errors:
        print(f"\n⚠  {len(errors)} committee(s) failed:")
        for e in errors:
            print(f"   {e['committee']}: {e['error']}")


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
    elif "--contacts" in sys.argv:
        # Collect any positional args after --contacts as chamber names.
        # e.g. "--contacts House Senate" → ["House", "Senate"]
        idx = sys.argv.index("--contacts")
        extra = [a for a in sys.argv[idx + 1:] if not a.startswith("--")]
        asyncio.run(scrape_contacts_main(chambers=extra or None))
    else:
        asyncio.run(main())
