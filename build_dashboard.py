#!/usr/bin/env python3
"""
build_dashboard.py
Reads bindable_data.json and injects live data into index.html.
Replaces the BINDABLE_DATA_PLACEHOLDER script tag with real data.
Runs as part of the GitHub Action after fetch_notion.py.
"""

import json, re, sys
from datetime import datetime, timezone

def fmt_currency(v, decimals=0):
    if v is None: return "—"
    if decimals == 0:
        return f"${v:,.0f}"
    return f"${v:,.{decimals}f}"

def fmt_pct(v):
    if v is None: return "—"
    return f"{v:.1f}%"

def fmt_num(v, decimals=0):
    if v is None: return "—"
    return f"{v:,.{decimals}f}"

def build_kpi_overrides(data):
    """Build a JS object of KPI values to inject into the dashboard."""
    baseline = data.get("baseline", {})
    rollup   = data.get("live_rollup", {})
    has_live = data.get("has_live_data", False)
    weekly   = data.get("weekly_log", [])
    acq      = data.get("acq_cost_log", [])
    gen_at   = data.get("generated_at", "")

    # Prefer live rollup data when available, fall back to baseline
    def live_or_base(live_key, base_key, transform=None):
        v = rollup.get(live_key) if has_live else None
        if v is None:
            v = baseline.get(base_key)
        if transform and v is not None:
            v = transform(v)
        return v

    # Format generated_at
    try:
        dt = datetime.fromisoformat(gen_at.replace("Z", "+00:00"))
        last_updated = dt.strftime("%b %d, %Y at %H:%M UTC")
    except Exception:
        last_updated = gen_at

    # Build weekly log HTML rows
    weekly_rows_html = ""
    for e in weekly:
        def cell(v, fmt_fn=str):
            if v is None: return "<td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:11px;font-family:DM Mono,monospace;color:var(--text3);'>—</td>"
            return f"<td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:11px;font-family:DM Mono,monospace;color:var(--text2);'>{fmt_fn(v)}</td>"

        weekly_rows_html += f"""<tr>
          <td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:12px;font-weight:500;color:var(--text);'>{e.get('period','')}</td>
          {cell(e.get('leads'), lambda v: f"{v:,.0f}")}
          {cell(e.get('ci_leads'), lambda v: f"{v:,.0f}")}
          {cell(e.get('nci_leads'), lambda v: f"{v:,.0f}")}
          {cell(e.get('ci_quotes_done'), lambda v: f"{v:,.0f}")}
          {cell(e.get('nci_quotes_done'), lambda v: f"{v:,.0f}")}
          {cell(e.get('quote_revenue'), lambda v: f"${v:,.0f}")}
          {cell(e.get('bound_policies'), lambda v: f"{v:,.0f}")}
          {cell(e.get('bind_rate'), lambda v: f"{v:.1f}%")}
          {cell(e.get('avenge_spend'), lambda v: f"${v:,.0f}")}
          {cell(e.get('google_ads_spend'), lambda v: f"${v:,.0f}")}
          {cell(e.get('total_spend'), lambda v: f"${v:,.0f}")}
          {cell(e.get('cpl_blended'), lambda v: f"${v:,.2f}")}
          {cell(e.get('cost_per_bound'), lambda v: f"${v:,.2f}")}
          {cell(e.get('quote_roas'), lambda v: f"{v:.2f}x")}
          <td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text3);'>{e.get('notes','')}</td>
        </tr>"""

    if not weekly_rows_html:
        weekly_rows_html = """<tr><td colspan='16' style='padding:20px;text-align:center;color:var(--text3);font-size:12px;'>
          No weekly data yet — add your first row in Notion to see it here.</td></tr>"""

    # Build acq cost rows HTML
    acq_rows_html = ""
    for e in acq:
        def acell(v, fmt_fn=str):
            if v is None: return "<td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:11px;font-family:DM Mono,monospace;color:var(--text3);'>—</td>"
            return f"<td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:11px;font-family:DM Mono,monospace;color:var(--text2);'>{fmt_fn(v)}</td>"

        source = e.get("source", "")
        source_color = "var(--orange)" if source == "Avenge" else "var(--blue)"
        acq_rows_html += f"""<tr>
          <td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:12px;color:var(--text2);'>{e.get('period','')}</td>
          <td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:12px;font-weight:500;color:{source_color};'>{source}</td>
          {acell(e.get('spend'), lambda v: f"${v:,.0f}")}
          {acell(e.get('leads'), lambda v: f"{v:,.0f}")}
          {acell(e.get('cpl'), lambda v: f"${v:,.2f}")}
          {acell(e.get('bound'), lambda v: f"{v:,.0f}")}
          {acell(e.get('cost_per_bound'), lambda v: f"${v:,.2f}")}
          {acell(e.get('quote_rev'), lambda v: f"${v:,.0f}")}
          {acell(e.get('roas'), lambda v: f"{v:.2f}x")}
          <td style='padding:9px 10px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text3);'>{e.get('notes','')}</td>
        </tr>"""

    if not acq_rows_html:
        acq_rows_html = """<tr><td colspan='10' style='padding:20px;text-align:center;color:var(--text3);font-size:12px;'>
          No acquisition cost data yet — add rows in Notion.</td></tr>"""

    # KPI values for display
    total_leads = live_or_base("leads", "total_leads_alltime")
    policies_bound = live_or_base("bound_policies", "policies_bound_alltime")
    bind_rate = live_or_base("bind_rate", "bind_rate_alltime")
    quote_rev = live_or_base("quote_revenue", "quote_rev_post_mar27")
    total_spend = rollup.get("total_spend") if has_live else None
    quote_roas = rollup.get("quote_roas") if has_live else None
    cpl = rollup.get("cpl_blended") if has_live else None

    live_badge = "LIVE" if has_live else "BASELINE"
    live_badge_color = "var(--green)" if has_live else "var(--amber)"

    return {
        "last_updated": last_updated,
        "live_badge": live_badge,
        "live_badge_color": live_badge_color,
        "total_leads": fmt_num(total_leads),
        "policies_bound": fmt_num(policies_bound),
        "bind_rate": fmt_pct(bind_rate),
        "quote_revenue": fmt_currency(quote_rev),
        "total_spend": fmt_currency(total_spend) if total_spend else "—",
        "quote_roas": f"{quote_roas:.2f}x" if quote_roas else "—",
        "cpl_blended": fmt_currency(cpl, 2) if cpl else "—",
        "too_expensive_rate": fmt_pct(baseline.get("too_expensive_rate")),
        "pipeline": fmt_num(baseline.get("pipeline_in_process")),
        "ci_bind_rate": fmt_pct(baseline.get("ci_bind_rate")),
        "nci_bind_rate": fmt_pct(baseline.get("nci_bind_rate")),
        "avg_premium": fmt_currency(baseline.get("avg_written_premium")),
        "data_through": baseline.get("data_through", ""),
        "weekly_rows_html": weekly_rows_html,
        "acq_rows_html": acq_rows_html,
        "has_live_data": str(has_live).lower(),
        "raw_json": json.dumps(data, indent=2)
    }

def inject_into_html(html, overrides):
    """Replace the BINDABLE_DATA placeholder script block with live values."""

    # Build the JS injection block
    js_block = "<script id='bindable-data-injection'>\n"
    js_block += "window.BINDABLE_DATA = " + json.dumps({
        k: v for k, v in overrides.items()
        if k not in ("weekly_rows_html", "acq_rows_html", "raw_json")
    }, indent=2) + ";\n"
    js_block += f"window.BINDABLE_DATA.weekly_rows_html = {json.dumps(overrides['weekly_rows_html'])};\n"
    js_block += f"window.BINDABLE_DATA.acq_rows_html = {json.dumps(overrides['acq_rows_html'])};\n"
    js_block += "</script>\n"

    # Replace placeholder if it exists
    placeholder = "<!-- BINDABLE_DATA_PLACEHOLDER -->"
    if placeholder in html:
        html = html.replace(placeholder, js_block)
    else:
        # Inject before closing </head>
        html = html.replace("</head>", js_block + "</head>", 1)

    return html

def main():
    # Load data
    try:
        with open("bindable_data.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("ERROR: bindable_data.json not found. Run fetch_notion.py first.")
        sys.exit(1)

    # Load dashboard
    try:
        with open("index.html") as f:
            html = f.read()
    except FileNotFoundError:
        print("ERROR: index.html not found.")
        sys.exit(1)

    overrides = build_kpi_overrides(data)
    updated_html = inject_into_html(html, overrides)

    with open("index.html", "w") as f:
        f.write(updated_html)

    print(f"Dashboard updated. Last updated: {overrides['last_updated']}")
    print(f"Live data: {overrides['has_live_data']} | Badge: {overrides['live_badge']}")

if __name__ == "__main__":
    main()
