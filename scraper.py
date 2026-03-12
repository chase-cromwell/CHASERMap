#!/usr/bin/env python3
"""
TRACER Political Race Scraper
Scrapes 2026 Colorado legislative candidate fundraising data from
tracer.sos.colorado.gov and merges it with candidate listing files
(party, committee, status) into a single master CSV.

Usage:
    pip install playwright && playwright install chromium
    python scraper.py
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

JURISDICTION_VALUE = "99"   # STATEWIDE
ELECTION_VALUE     = "463"  # 2026 NOVEMBER ELECTION

CANDIDATE_FILES = {
    "Senate": DATA_DIR / "rpt_CF_CAND_001.csv",
    "House":  DATA_DIR / "rpt_CF_CAND_001-2.csv",
}

CHAMBER_OFFICE_VALUES = {
    "Senate": "6",
    "House":  "7",
}

REQUEST_DELAY_MS = 800

# ---------------------------------------------------------------------------
# Candidate listing loader
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(r'\b(JR\.?|SR\.?|II|III|IV|V)\b\.?', re.IGNORECASE)

def normalize_name(name: str) -> str:
    """Reduce to LAST, FIRST for matching — strips middle names/initials and
    generational suffixes (Jr., Sr., II, III, IV) which TRACER sometimes
    includes but the candidate listing omits."""
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
    """
    Read both candidate listing CSVs.
    Returns a lookup dict:
        (normalized_name, normalized_district) -> {
            Party, CommitteeName, AcceptedVSL, ListingStatus, Chamber, DistrictLabel
        }
    Also returns the set of (Chamber, DistrictLabel) pairs to drive scraping.
    """
    lookup   = {}
    districts = {}  # chamber -> list of DistrictLabel strings (original case)

    for chamber, path in CANDIDATE_FILES.items():
        if not path.exists():
            print(f"⚠  Candidate file not found: {path}")
            continue

        with open(path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        # Files have a 3-row preamble: filter summary, values, blank line
        # Row 4 onward is the actual data (0-indexed: lines[3:])
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
    """'Senate District 07' -> '7'"""
    m = re.search(r"(\d+)\s*$", district_text.strip())
    return str(int(m.group(1))) if m else district_text


async def select_and_wait(page, selector: str, value: str) -> None:
    """Select a dropdown value and wait for the resulting ASP.NET postback to complete.
    The onchange handler uses setTimeout('__doPostBack(...)', 0), so the navigation
    is delayed. We use expect_navigation() BEFORE selecting to correctly capture it."""
    async with page.expect_navigation(wait_until="networkidle"):
        await page.locator(selector).select_option(value=value)


async def setup_filters(page, office_value: str) -> None:
    """Set Jurisdiction → Election → Office, waiting for each ASP.NET postback."""
    await select_and_wait(page, 'select[name*="ddlJurisdiction"]', JURISDICTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlElection"]',     ELECTION_VALUE)
    await select_and_wait(page, 'select[name*="ddlOffice"]',       office_value)


async def get_tracer_district_map(page) -> dict:
    """Return {normalized_district_label: dropdown_value} from the District select."""
    options = await page.locator('select[name*="ddlDistrict"] option').all()
    result = {}
    for opt in options:
        value = await opt.get_attribute("value")
        text  = (await opt.inner_text()).strip()
        if value and "Select" not in text:
            result[normalize_district(text)] = {"value": value, "text": text}
    return result


async def scrape_district(page, district_value: str, district_text: str, chamber: str) -> list[dict]:
    """Search one district and return raw rows from the TRACER CSV export."""
    await page.evaluate(
        """(val) => {
            const sel = document.querySelector('select[name*="ddlDistrict"]');
            sel.value = val;
            sel.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        district_value,
    )
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
    """
    Join TRACER financial data with candidate listing on (name, district).
    Adds Party, CommitteeName, AcceptedVSL, ListingStatus to each row.
    """
    merged = []
    unmatched = []

    for row in tracer_rows:
        name_key     = normalize_name(row["CandName"])
        district_key = normalize_district(row["DistrictLabel"])
        info = listing.get((name_key, district_key))

        if info is None:
            # Fallback: try matching without district (handles rare label mismatches)
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

LISTING_COLS = {"Party", "CommitteeName", "AcceptedVSL", "ListingStatus"}

def reprocess() -> None:
    """Re-merge the existing output CSV with the candidate listing files.
    Reads tracer_2026_all_districts.csv, drops the listing-derived columns,
    re-runs the join with updated normalization, and writes back."""
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
    else:
        asyncio.run(main())
