#!/usr/bin/env python3
"""
fetch_notion.py
Fetches the Bindable Performance page from Notion and outputs bindable_data.json.
Runs as part of the GitHub Action nightly rebuild.

Required env vars:
  NOTION_API_KEY  — your Notion integration secret
  NOTION_PAGE_ID  — 347b639a58048181bb1cc0f76892912c
"""

import os, json, re, urllib.request, urllib.error
from datetime import datetime, timezone

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_PAGE_ID = os.environ.get("NOTION_PAGE_ID", "347b639a58048181bb1cc0f76892912c")
NOTION_VERSION = "2022-06-28"

def notion_get(path):
    url = f"https://api.notion.com/v1/{path}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    })
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def get_block_children(block_id):
    results = []
    cursor = None
    while True:
        path = f"blocks/{block_id}/children?page_size=100"
        if cursor:
            path += f"&start_cursor={cursor}"
        data = notion_get(path)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results

def extract_rich_text(rich_text_arr):
    return "".join(t.get("plain_text", "") for t in (rich_text_arr or []))

def extract_table_rows(block_id):
    """Returns list of lists (rows of cells as plain text)."""
    rows = []
    children = get_block_children(block_id)
    for child in children:
        if child["type"] == "table_row":
            cells = child["table_row"]["cells"]
            rows.append([extract_rich_text(cell) for cell in cells])
    return rows

def parse_number(s):
    """Strips $, commas, % and converts to float. Returns None if empty."""
    s = s.strip().replace("$", "").replace(",", "").replace("%", "")
    if not s or s in ["-", "—", ""]:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def parse_weekly_log(rows):
    """Parse the weekly performance log table."""
    if len(rows) < 2:
        return []
    header = rows[0]
    entries = []
    for row in rows[1:]:
        if len(row) < 2 or not row[0].strip():
            continue
        def get(col_name, default=None):
            try:
                idx = header.index(col_name)
                return row[idx] if idx < len(row) else default
            except ValueError:
                return default

        period     = get("Period", "")
        leads      = parse_number(get("Leads", ""))
        ci_leads   = parse_number(get("CI Leads", ""))
        nci_leads  = parse_number(get("NCI Leads", ""))
        ci_q       = parse_number(get("CI Quotes Done", ""))
        nci_q      = parse_number(get("NCI Quotes Done", ""))
        bound      = parse_number(get("Bound Policies", ""))
        avenge     = parse_number(get("Avenge Spend", ""))
        gads       = parse_number(get("Google Ads Spend", ""))
        notes      = get("Notes", "")

        # Calculate derived fields
        quote_rev = None
        if ci_q is not None and nci_q is not None:
            quote_rev = (ci_q * 50) + (nci_q * 25)
        bind_rate = None
        if leads and bound is not None:
            bind_rate = round(bound / leads * 100, 1)
        total_spend = None
        if avenge is not None and gads is not None:
            total_spend = avenge + gads
        elif avenge is not None:
            total_spend = avenge
        cpl = None
        if total_spend and leads:
            cpl = round(total_spend / leads, 2)
        cost_per_bound = None
        if total_spend and bound:
            cost_per_bound = round(total_spend / bound, 2)
        quote_roas = None
        if quote_rev and total_spend and total_spend > 0:
            quote_roas = round(quote_rev / total_spend, 2)

        entries.append({
            "period": period,
            "leads": leads,
            "ci_leads": ci_leads,
            "nci_leads": nci_leads,
            "ci_quotes_done": ci_q,
            "nci_quotes_done": nci_q,
            "quote_revenue": quote_rev,
            "bound_policies": bound,
            "bind_rate": bind_rate,
            "avenge_spend": avenge,
            "google_ads_spend": gads,
            "total_spend": total_spend,
            "cpl_blended": cpl,
            "cost_per_bound": cost_per_bound,
            "quote_roas": quote_roas,
            "notes": notes
        })
    return entries

def parse_acq_cost_log(rows):
    """Parse the acquisition cost log by source."""
    if len(rows) < 2:
        return []
    header = rows[0]
    entries = []
    for row in rows[1:]:
        if len(row) < 2 or not row[0].strip():
            continue
        def get(col_name, default=None):
            try:
                idx = header.index(col_name)
                return row[idx] if idx < len(row) else default
            except ValueError:
                return default
        entries.append({
            "period": get("Period", ""),
            "source": get("Source", ""),
            "spend": parse_number(get("Spend", "")),
            "leads": parse_number(get("Leads from Source", "")),
            "cpl": parse_number(get("CPL", "")),
            "bound": parse_number(get("Bound from Source", "")),
            "cost_per_bound": parse_number(get("Cost / Bound", "")),
            "quote_rev": parse_number(get("Quote Rev from Source", "")),
            "roas": parse_number(get("ROAS", "")),
            "notes": get("Notes", "")
        })
    return entries

def rollup_weekly(entries):
    """Compute MTD / all-time rollups from weekly log."""
    totals = {
        "leads": 0, "ci_leads": 0, "nci_leads": 0,
        "ci_quotes_done": 0, "nci_quotes_done": 0,
        "bound_policies": 0, "avenge_spend": 0.0,
        "google_ads_spend": 0.0, "total_spend": 0.0
    }
    for e in entries:
        for k in totals:
            if e.get(k) is not None:
                totals[k] += e[k]

    totals["quote_revenue"] = (totals["ci_quotes_done"] * 50) + (totals["nci_quotes_done"] * 25)
    totals["bind_rate"] = round(totals["bound_policies"] / totals["leads"] * 100, 1) if totals["leads"] else None
    totals["cpl_blended"] = round(totals["total_spend"] / totals["leads"], 2) if totals["leads"] and totals["total_spend"] else None
    totals["cost_per_bound"] = round(totals["total_spend"] / totals["bound_policies"], 2) if totals["bound_policies"] and totals["total_spend"] else None
    totals["quote_roas"] = round(totals["quote_revenue"] / totals["total_spend"], 2) if totals["total_spend"] and totals["total_spend"] > 0 else None
    return totals

def main():
    print(f"Fetching Notion page {NOTION_PAGE_ID}...")
    blocks = get_block_children(NOTION_PAGE_ID)

    weekly_log_rows = []
    acq_cost_rows = []
    current_section = None

    for block in blocks:
        btype = block["type"]

        # Detect section headings
        if btype == "heading_2":
            text = extract_rich_text(block["heading_2"]["rich_text"]).lower()
            if "weekly performance log" in text:
                current_section = "weekly"
            elif "acquisition cost log" in text:
                current_section = "acq"
            else:
                current_section = None

        # Parse tables in context
        elif btype == "table":
            rows = extract_table_rows(block["id"])
            if current_section == "weekly":
                weekly_log_rows = rows
            elif current_section == "acq":
                acq_cost_rows = rows

    weekly_entries = parse_weekly_log(weekly_log_rows)
    acq_entries = parse_acq_cost_log(acq_cost_rows)
    rollup = rollup_weekly(weekly_entries) if weekly_entries else {}

    # Baseline hardcoded from last full data pull (Apr 17 2026)
    # These are overridden by live weekly log totals once data flows in
    baseline = {
        "total_leads_alltime": 2943,
        "policies_bound_alltime": 897,
        "bind_rate_alltime": 30.5,
        "avg_written_premium": 2078,
        "too_expensive_rate": 26.2,
        "pipeline_in_process": 248,
        "ci_leads_alltime": 1348,
        "nci_leads_alltime": 1367,
        "ci_bind_rate": 33.5,
        "nci_bind_rate": 31.5,
        "quote_rev_post_mar27": 2650,
        "ci_quotes_post_mar27": 43,
        "nci_quotes_post_mar27": 20,
        "jan_2026_commission": 287.91,
        "top_carrier": "MAPFRE",
        "top_carrier_policies": 232,
        "data_through": "Apr 17, 2026"
    }

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "notion_page_id": NOTION_PAGE_ID,
        "baseline": baseline,
        "weekly_log": weekly_entries,
        "acq_cost_log": acq_entries,
        "live_rollup": rollup,
        "has_live_data": len(weekly_entries) > 0 and any(
            e["leads"] is not None for e in weekly_entries
        )
    }

    with open("bindable_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Done. {len(weekly_entries)} weekly entries, {len(acq_entries)} acq cost entries.")
    print(f"Has live data: {output['has_live_data']}")
    if output["has_live_data"]:
        print(f"Live rollup: {json.dumps(rollup, indent=2)}")

if __name__ == "__main__":
    main()
