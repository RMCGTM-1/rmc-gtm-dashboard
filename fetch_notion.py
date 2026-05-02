#!/usr/bin/env python3
"""
fetch_notion.py — merges Notion spend/quote data INTO the existing bindable_data.json.

What it updates from Notion:
  - weekly_log: spend fields (avenge_spend, google_ads_spend, total_spend,
    cpl_blended, cost_per_bound, quote_roas, ci_quotes_done, nci_quotes_done,
    quote_revenue, notes) matched by period label
  - acq_cost_log: fully replaced from Notion Acquisition Cost Log table
  - live_rollup: recomputed from the updated weekly_log

What it does NOT touch:
  - baseline, policy_log, policy_analytics, extended_analytics, quote_to_policy_cr
  - Any field in weekly_log not sourced from Notion (leads, bound_policies, bind_rate etc)
  - These are owned by process_reports.py and updated when new Paperboy files are uploaded

This way the nightly Action keeps spend data fresh without overwriting Paperboy data.
"""

import os, json, urllib.request
from datetime import datetime, timezone

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_PAGE_ID = os.environ.get("NOTION_PAGE_ID", "347b639a58048181bb1cc0f76892912c")
NOTION_VERSION = "2022-06-28"

def notion_get(path):
    req = urllib.request.Request(
        f"https://api.notion.com/v1/{path}",
        headers={"Authorization": f"Bearer {NOTION_API_KEY}",
                 "Notion-Version": NOTION_VERSION,
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def get_children(block_id):
    results, cursor = [], None
    while True:
        path = f"blocks/{block_id}/children?page_size=100"
        if cursor: path += f"&start_cursor={cursor}"
        data = notion_get(path)
        results.extend(data.get("results", []))
        if not data.get("has_more"): break
        cursor = data.get("next_cursor")
    return results

def rt(arr): return "".join(t.get("plain_text", "") for t in (arr or []))

def table_rows(block_id):
    rows = []
    for c in get_children(block_id):
        if c["type"] == "table_row":
            rows.append([rt(cell) for cell in c["table_row"]["cells"]])
    return rows

def num(s):
    s = str(s).strip().replace("$", "").replace(",", "").replace("%", "")
    if not s or s in ["-", "—", ""]: return None
    try: return float(s)
    except: return None

def col(header, row, name, default=None):
    try:
        i = header.index(name)
        return row[i] if i < len(row) else default
    except ValueError:
        return default

def parse_weekly_spend(rows):
    """Extract only spend/quote fields from the Notion weekly log, keyed by period."""
    if len(rows) < 2: return {}
    h, out = rows[0], {}
    for row in rows[1:]:
        if not any(c.strip() for c in row): continue
        period = col(h, row, "Period", "").strip()
        if not period: continue
        ci_q  = num(col(h, row, "CI Quotes Done"))
        nci_q = num(col(h, row, "NCI Quotes Done"))
        av    = num(col(h, row, "Avenge Spend"))
        ga    = num(col(h, row, "Google Ads Spend"))
        qrev  = ((ci_q or 0) * 50) + ((nci_q or 0) * 25) if (ci_q is not None or nci_q is not None) else None
        spend = (av or 0) + (ga or 0) if (av is not None or ga is not None) else None
        bound = num(col(h, row, "Bound Policies"))
        leads = num(col(h, row, "Leads"))
        out[period] = {
            "ci_quotes_done":   ci_q,
            "nci_quotes_done":  nci_q,
            "quote_revenue":    qrev,
            "avenge_spend":     av,
            "google_ads_spend": ga,
            "total_spend":      spend,
            "cpl_blended":      round(spend / leads, 2)  if spend and leads else None,
            "cost_per_bound":   round(spend / bound, 2)  if spend and bound else None,
            "quote_roas":       round(qrev / spend, 2)   if qrev and spend and spend > 0 else None,
            "notes":            col(h, row, "Notes", "").strip(),
        }
    return out

def parse_acq(rows):
    if len(rows) < 2: return []
    h, out = rows[0], []
    for row in rows[1:]:
        if not any(c.strip() for c in row): continue
        out.append({
            "period":         col(h, row, "Period", ""),
            "source":         col(h, row, "Source", ""),
            "spend":          num(col(h, row, "Spend")),
            "leads":          num(col(h, row, "Leads from Source")),
            "cpl":            num(col(h, row, "CPL")),
            "bound":          num(col(h, row, "Bound from Source")),
            "cost_per_bound": num(col(h, row, "Cost / Bound")),
            "quote_rev":      num(col(h, row, "Quote Rev from Source")),
            "roas":           num(col(h, row, "ROAS")),
            "notes":          col(h, row, "Notes", "")
        })
    return out

def recompute_rollup(weekly_log):
    t = {"leads": 0, "ci_leads": 0, "nci_leads": 0,
         "ci_quotes": 0, "nci_quotes": 0,
         "bound": 0, "avenge_spend": 0.0,
         "google_ads_spend": 0.0, "total_spend": 0.0}
    for r in weekly_log:
        t["leads"]           += r.get("leads") or 0
        t["ci_leads"]        += r.get("ci_leads") or 0
        t["nci_leads"]       += r.get("nci_leads") or 0
        t["ci_quotes"]       += r.get("ci_quotes_done") or 0
        t["nci_quotes"]      += r.get("nci_quotes_done") or 0
        t["bound"]           += r.get("bound_policies") or 0
        t["avenge_spend"]    += r.get("avenge_spend") or 0
        t["google_ads_spend"]+= r.get("google_ads_spend") or 0
        t["total_spend"]     += r.get("total_spend") or 0
    t["quote_revenue"]  = t["ci_quotes"] * 50 + t["nci_quotes"] * 25
    t["bind_rate"]      = round(t["bound"] / t["leads"] * 100, 1) if t["leads"] else None
    t["cpl_blended"]    = round(t["total_spend"] / t["leads"], 2)  if t["leads"] and t["total_spend"] else None
    t["cost_per_bound"] = round(t["total_spend"] / t["bound"], 2)  if t["bound"] and t["total_spend"] else None
    t["quote_roas"]     = round(t["quote_revenue"] / t["total_spend"], 2) if t["total_spend"] > 0 else None
    return t

def main():
    # Load the existing bindable_data.json (built by process_reports.py)
    try:
        with open("bindable_data.json") as f:
            data = json.load(f)
        print(f"Loaded existing bindable_data.json (generated {data.get('generated_at','?')})")
    except Exception as e:
        print(f"ERROR: could not load bindable_data.json: {e}")
        raise

    print(f"Fetching Notion page {NOTION_PAGE_ID}...")
    blocks = get_children(NOTION_PAGE_ID)
    weekly_rows, acq_rows, section = [], [], None

    for block in blocks:
        bt = block["type"]
        if bt == "heading_2":
            text = rt(block["heading_2"]["rich_text"]).lower()
            if "weekly performance log" in text:   section = "weekly"
            elif "acquisition cost log" in text:   section = "acq"
            else:                                  section = None
        elif bt == "table":
            rows = table_rows(block["id"])
            if section == "weekly": weekly_rows = rows
            elif section == "acq":  acq_rows    = rows

    # Parse Notion spend data
    notion_spend = parse_weekly_spend(weekly_rows)
    acq_log      = parse_acq(acq_rows)

    # Merge spend fields into existing weekly_log rows (match by period label)
    weekly_log = data.get("weekly_log", [])
    merged_count = 0
    for row in weekly_log:
        period = row.get("period", "")
        if period in notion_spend:
            spend_data = notion_spend[period]
            for field, value in spend_data.items():
                if value is not None:
                    row[field] = value
            merged_count += 1

    # Add any Notion periods not already in weekly_log (e.g. a new week row added in Notion)
    existing_periods = {r["period"] for r in weekly_log}
    for period, spend_data in notion_spend.items():
        if period not in existing_periods:
            weekly_log.append({"period": period, **spend_data})
            print(f"  Added new period from Notion: {period}")

    # Update the data
    data["weekly_log"]  = weekly_log
    data["acq_cost_log"] = acq_log
    data["live_rollup"] = recompute_rollup(weekly_log)
    data["generated_at"] = datetime.now(timezone.utc).isoformat()

    with open("bindable_data.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"Merged spend data for {merged_count} periods from Notion.")
    print(f"Acq cost log: {len(acq_log)} rows.")
    print(f"Weekly log: {len(weekly_log)} periods total.")
    print(f"Policy log: {len(data.get('policy_log', []))} rows (unchanged).")
    print(f"Data through: {data.get('baseline', {}).get('data_through', '?')}")

if __name__ == "__main__":
    main()
