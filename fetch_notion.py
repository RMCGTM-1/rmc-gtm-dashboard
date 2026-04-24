#!/usr/bin/env python3
"""
fetch_notion.py — pulls Weekly Log, Policy Log, Acq Cost Log from Notion.
Computes quote-to-policy CR, avg days to bind, carrier/state/CI-NCI breakdown.
"""

import os, json, urllib.request
from datetime import datetime, timezone, date as dateclass

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_PAGE_ID = os.environ.get("NOTION_PAGE_ID", "347b639a58048181bb1cc0f76892912c")
NOTION_VERSION = "2022-06-28"

def notion_get(path):
    req = urllib.request.Request(
        f"https://api.notion.com/v1/{path}",
        headers={"Authorization": f"Bearer {NOTION_API_KEY}",
                 "Notion-Version": NOTION_VERSION, "Content-Type": "application/json"})
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

def rt(arr): return "".join(t.get("plain_text","") for t in (arr or []))

def table_rows(block_id):
    rows = []
    for c in get_children(block_id):
        if c["type"] == "table_row":
            rows.append([rt(cell) for cell in c["table_row"]["cells"]])
    return rows

def num(s):
    s = str(s).strip().replace("$","").replace(",","").replace("%","")
    if not s or s in ["-","—",""]: return None
    try: return float(s)
    except: return None

def parse_date(s):
    s = str(s).strip()
    if not s or s in ["-","—",""]: return None
    for fmt in ["%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%b %d, %Y","%B %d, %Y"]:
        try: return datetime.strptime(s, fmt).date().isoformat()
        except: continue
    return None

def days_diff(d1, d2):
    try: return (dateclass.fromisoformat(d2) - dateclass.fromisoformat(d1)).days
    except: return None

def col(header, row, name, default=None):
    try:
        i = header.index(name)
        return row[i] if i < len(row) else default
    except ValueError:
        return default

def parse_weekly(rows):
    if len(rows) < 2: return []
    h, out = rows[0], []
    for row in rows[1:]:
        if not any(c.strip() for c in row): continue
        period = col(h,row,"Period","")
        if not period.strip(): continue
        leads = num(col(h,row,"Leads"))
        ci_q  = num(col(h,row,"CI Quotes Done"))
        nci_q = num(col(h,row,"NCI Quotes Done"))
        bound = num(col(h,row,"Bound Policies"))
        av    = num(col(h,row,"Avenge Spend"))
        ga    = num(col(h,row,"Google Ads Spend"))
        qrev  = ((ci_q or 0)*50)+((nci_q or 0)*25) if ci_q is not None or nci_q is not None else None
        spend = (av or 0)+(ga or 0) if av is not None or ga is not None else None
        out.append({
            "period": period,
            "leads": leads,
            "ci_leads": num(col(h,row,"CI Leads")),
            "nci_leads": num(col(h,row,"NCI Leads")),
            "ci_quotes_done": ci_q,
            "nci_quotes_done": nci_q,
            "quote_revenue": qrev,
            "bound_policies": bound,
            "bind_rate": round(bound/leads*100,1) if leads and bound else None,
            "avenge_spend": av,
            "google_ads_spend": ga,
            "total_spend": spend,
            "cpl_blended": round(spend/leads,2) if spend and leads else None,
            "cost_per_bound": round(spend/bound,2) if spend and bound else None,
            "quote_roas": round(qrev/spend,2) if qrev and spend and spend>0 else None,
            "notes": col(h,row,"Notes","")
        })
    return out

def parse_policy(rows):
    if len(rows) < 2: return []
    h, out = rows[0], []
    for row in rows[1:]:
        if not any(c.strip() for c in row): continue
        dq = parse_date(col(h,row,"Date Quote Completed"))
        db = parse_date(col(h,row,"Date Policy Bound"))
        prem = num(col(h,row,"Written Premium"))
        lead_uuid = col(h,row,"Lead UUID","").strip()
        carrier = col(h,row,"Carrier","").strip()
        if not carrier and not prem and not lead_uuid: continue
        out.append({
            "lead_uuid": lead_uuid,
            "date_quote_completed": dq,
            "date_policy_bound": db,
            "days_to_bind": days_diff(dq, db),
            "ci_nci": col(h,row,"CI / NCI","").strip().upper(),
            "carrier": carrier,
            "written_premium": prem,
            "term_months": int(num(col(h,row,"Term (mo)")) or 0) or None,
            "state": col(h,row,"State","").strip().upper(),
            "lob": col(h,row,"LOB","").strip(),
            "lead_source": col(h,row,"Lead Source","").strip(),
            "notes": col(h,row,"Notes","").strip()
        })
    return out

def parse_acq(rows):
    if len(rows) < 2: return []
    h, out = rows[0], []
    for row in rows[1:]:
        if not any(c.strip() for c in row): continue
        out.append({
            "period": col(h,row,"Period",""),
            "source": col(h,row,"Source",""),
            "spend": num(col(h,row,"Spend")),
            "leads": num(col(h,row,"Leads from Source")),
            "cpl": num(col(h,row,"CPL")),
            "bound": num(col(h,row,"Bound from Source")),
            "cost_per_bound": num(col(h,row,"Cost / Bound")),
            "quote_rev": num(col(h,row,"Quote Rev from Source")),
            "roas": num(col(h,row,"ROAS")),
            "notes": col(h,row,"Notes","")
        })
    return out

def policy_analytics(policies):
    if not policies: return {}
    n = len(policies)
    prems = [e["written_premium"] for e in policies if e["written_premium"]]
    days  = [e["days_to_bind"] for e in policies if e["days_to_bind"] is not None and e["days_to_bind"]>=0]

    # carrier breakdown
    cm = {}
    for e in policies:
        c = e["carrier"] or "Unknown"
        cm.setdefault(c, {"n":0,"p":[],"d":[]})
        cm[c]["n"] += 1
        if e["written_premium"]: cm[c]["p"].append(e["written_premium"])
        if e["days_to_bind"] is not None and e["days_to_bind"]>=0: cm[c]["d"].append(e["days_to_bind"])
    carrier_bd = [{"carrier":c,
                   "policies":v["n"],
                   "avg_premium":round(sum(v["p"])/len(v["p"]),0) if v["p"] else None,
                   "total_written":round(sum(v["p"]),0) if v["p"] else None,
                   "avg_days_to_bind":round(sum(v["d"])/len(v["d"]),1) if v["d"] else None,
                   "pct_of_total":round(v["n"]/n*100,1)}
                  for c,v in sorted(cm.items(), key=lambda x:-x[1]["n"])]

    # CI/NCI breakdown
    def seg(lst):
        p=[e["written_premium"] for e in lst if e["written_premium"]]
        d=[e["days_to_bind"] for e in lst if e["days_to_bind"] is not None and e["days_to_bind"]>=0]
        return {"count":len(lst),
                "avg_premium":round(sum(p)/len(p),0) if p else None,
                "avg_days_to_bind":round(sum(d)/len(d),1) if d else None}

    # state breakdown
    sm = {}
    for e in policies:
        s = e["state"] or "Unknown"
        sm.setdefault(s,{"n":0,"p":[]})
        sm[s]["n"]+=1
        if e["written_premium"]: sm[s]["p"].append(e["written_premium"])
    state_bd = [{"state":s,
                 "policies":v["n"],
                 "avg_premium":round(sum(v["p"])/len(v["p"]),0) if v["p"] else None,
                 "pct_of_total":round(v["n"]/n*100,1)}
                for s,v in sorted(sm.items(),key=lambda x:-x[1]["n"])][:10]

    return {
        "total_policies_logged": n,
        "avg_written_premium": round(sum(prems)/len(prems),2) if prems else None,
        "total_written_premium": round(sum(prems),2) if prems else None,
        "avg_days_to_bind": round(sum(days)/len(days),1) if days else None,
        "min_days_to_bind": min(days) if days else None,
        "max_days_to_bind": max(days) if days else None,
        "carrier_breakdown": carrier_bd,
        "ci_stats": seg([e for e in policies if e["ci_nci"]=="CI"]),
        "nci_stats": seg([e for e in policies if e["ci_nci"]=="NCI"]),
        "state_breakdown": state_bd
    }

def q2p_cr(weekly, policies):
    ci_q  = sum(e["ci_quotes_done"] or 0 for e in weekly if e.get("ci_quotes_done"))
    nci_q = sum(e["nci_quotes_done"] or 0 for e in weekly if e.get("nci_quotes_done"))
    total_q = ci_q + nci_q
    n = len(policies)
    return {
        "cr_pct": round(n/total_q*100,1) if total_q>0 else None,
        "policies_from_quotes": n,
        "total_quotes_completed": total_q,
        "ci_quotes": ci_q,
        "nci_quotes": nci_q
    }

def rollup(entries):
    t={"leads":0,"ci_leads":0,"nci_leads":0,"ci_quotes_done":0,"nci_quotes_done":0,
       "bound_policies":0,"avenge_spend":0.0,"google_ads_spend":0.0,"total_spend":0.0}
    for e in entries:
        for k in t:
            if e.get(k): t[k]+=e[k]
    t["quote_revenue"]=(t["ci_quotes_done"]*50)+(t["nci_quotes_done"]*25)
    t["bind_rate"]=round(t["bound_policies"]/t["leads"]*100,1) if t["leads"] else None
    t["cpl_blended"]=round(t["total_spend"]/t["leads"],2) if t["leads"] and t["total_spend"] else None
    t["cost_per_bound"]=round(t["total_spend"]/t["bound_policies"],2) if t["bound_policies"] and t["total_spend"] else None
    t["quote_roas"]=round(t["quote_revenue"]/t["total_spend"],2) if t["total_spend"]>0 else None
    return t

def main():
    print(f"Fetching Notion page {NOTION_PAGE_ID}...")
    blocks = get_children(NOTION_PAGE_ID)
    weekly_rows, policy_rows, acq_rows, section = [], [], [], None

    for block in blocks:
        bt = block["type"]
        if bt in ("heading_2", "heading_3"):
            rich = block[bt]["rich_text"]
            text = rt(rich).lower()
            # strip non-ascii (emojis) for robust matching
            clean = ''.join(c for c in text if c.isascii()).strip()
            if "weekly performance log" in clean: section="weekly"
            elif "policy log" in clean: section="policy"
            elif "acquisition cost log" in clean: section="acq"
            else: section=None
        elif bt == "table":
            rows = table_rows(block["id"])
            if section=="weekly": weekly_rows=rows
            elif section=="policy": policy_rows=rows
            elif section=="acq": acq_rows=rows

    weekly  = parse_weekly(weekly_rows)
    policies= parse_policy(policy_rows)
    acq     = parse_acq(acq_rows)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "notion_page_id": NOTION_PAGE_ID,
        "baseline": {
            "total_leads_alltime":3036,"policies_bound_alltime":929,
            "bind_rate_alltime":30.6,"avg_written_premium":2101,
            "too_expensive_rate":26.2,"pipeline_in_process":231,
            "ci_leads_alltime":1404,"nci_leads_alltime":1404,
            "ci_bind_rate":33.5,"nci_bind_rate":31.5,
            "quote_rev_post_mar27":3450,"ci_quotes_post_mar27":54,
            "nci_quotes_post_mar27":30,"jan_2026_commission":287.91,
            "data_through":"Apr 22, 2026"
        },
        "weekly_log": weekly,
        "policy_log": policies,
        "acq_cost_log": acq,
        "live_rollup": rollup(weekly) if weekly else {},
        "policy_analytics": policy_analytics(policies),
        "quote_to_policy_cr": q2p_cr(weekly, policies),
        "has_live_data": len(weekly)>0 and any(e["leads"] for e in weekly),
        "has_policy_data": len(policies)>0
    }

    with open("bindable_data.json","w") as f:
        json.dump(output, f, indent=2)

    print(f"Weekly: {len(weekly)} | Policies: {len(policies)} | Acq: {len(acq)}")
    if policies:
        cr = output["quote_to_policy_cr"]
        pa = output["policy_analytics"]
        print(f"Q→P CR: {cr['cr_pct']}% | Avg days to bind: {pa.get('avg_days_to_bind')} | Avg premium: ${pa.get('avg_written_premium')}")

if __name__ == "__main__":
    main()
