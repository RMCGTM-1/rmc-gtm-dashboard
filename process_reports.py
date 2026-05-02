#!/usr/bin/env python3
"""
process_reports.py — RaiseMyCoverage weekly Bindable data pipeline
===================================================================
Usage:
    python process_reports.py --lead LEAD_REPORT.txt --policy POLICY_REPORT.txt

What it does:
    1. Parses the two Paperboy TXT exports (tab-separated)
    2. Computes all monthly rollups, analytics, and commission estimates
    3. Preserves spend data from the existing bindable_data.json (Avenge / Google Ads)
    4. Preserves commission_rates and monthly_actuals (updated manually once a year)
    5. Writes a fully updated bindable_data.json ready to commit to GitHub

What it does NOT touch:
    - index.html
    - fetch_notion.py / build_dashboard.py / rebuild.yml
    - Any spend figures in the weekly log (those come from Notion via the nightly Action)

After running:
    - Commit bindable_data.json to GitHub
    - The nightly Action will merge in fresh Notion spend data automatically
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Try to import pandas; give a clear error if missing ──────────────────────
try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required.  Run:  pip install pandas --break-system-packages")
    sys.exit(1)

# ── Constants ─────────────────────────────────────────────────────────────────
CAMPAIGN_START    = pd.Timestamp("2026-03-27")   # Avenge campaign launch
CI_QUOTE_RATE     = 50.0                          # $/completed CI quote
NCI_QUOTE_RATE    = 25.0                          # $/completed NCI quote
EXISTING_JSON     = Path("bindable_data.json")   # preserved for spend data etc.

# Commission rates — updated manually from commission reports (currently Nov/Dec 2025 + Jan 2026)
COMMISSION_RATES = {
    "ci_agency_rate":         0.1109,
    "nci_agency_rate":        0.1133,
    "ci_rmc_split":           0.5,
    "nci_rmc_split":          0.25,
    "ci_effective_rmc_rate":  0.0554,   # ci_agency_rate × ci_rmc_split
    "nci_effective_rmc_rate": 0.0283,   # nci_agency_rate × nci_rmc_split
    "blended_rmc_rate":       0.0414,
    "source": "Derived from Nov/Dec 2025 + Jan 2026 commission reports",
    "note": (
        "Agency Commission = Bindable earnings. "
        "RMC Commission = RaiseMyCoverage earnings "
        "(50% of agency on CI, 25% on NCI)"
    ),
}

# Known actual commission payments — add a row here when a new statement arrives
MONTHLY_ACTUALS = [
    {"month": "Nov 2025", "ci_policies": 16, "nci_policies": 13,
     "rmc_commission": 2467.07, "note": "Net after cancellations"},
    {"month": "Dec 2025", "ci_policies": 10, "nci_policies":  8,
     "rmc_commission": 1349.53, "note": ""},
    {"month": "Jan 2026", "ci_policies":  5, "nci_policies":  0,
     "rmc_commission":  287.91, "note": "Post-pause restart"},
]

# ── Helpers ────────────────────────────────────────────────────────────────────
def load_tsv(path: Path) -> pd.DataFrame:
    """Read a Paperboy tab-separated export, warn on parse issues."""
    try:
        df = pd.read_csv(path, sep="\t", low_memory=False)
    except Exception as e:
        print(f"ERROR reading {path}: {e}")
        sys.exit(1)
    return df


def safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0


def safe_float(x):
    try:
        v = float(x)
        return None if (v != v) else round(v, 2)   # NaN → None
    except Exception:
        return None


# ── Core computation ───────────────────────────────────────────────────────────
def build_bindable_data(lead_path: Path, policy_path: Path) -> dict:

    # ── Load files ────────────────────────────────────────────────────────────
    lead_df   = load_tsv(lead_path)
    policy_df = load_tsv(policy_path)

    # ── Normalize dtypes ──────────────────────────────────────────────────────
    lead_df["record_creation_time"] = pd.to_datetime(
        lead_df["record_creation_time"], errors="coerce", dayfirst=False)
    policy_df["date_policy_bound"]  = pd.to_datetime(
        policy_df["date_policy_bound"], errors="coerce", dayfirst=False)
    policy_df["effective_date"]     = pd.to_datetime(
        policy_df.get("effective_date"), errors="coerce", dayfirst=False)
    policy_df["written_premium"]    = pd.to_numeric(
        policy_df["written_premium"], errors="coerce")
    lead_df["age"]                  = pd.to_numeric(
        lead_df["age"], errors="coerce")

    lead_df["month"]   = lead_df["record_creation_time"].dt.to_period("M").astype(str)
    policy_df["month"] = policy_df["date_policy_bound"].dt.to_period("M").astype(str)

    # ── Join CI/NCI flag onto policies ────────────────────────────────────────
    ci_map = lead_df.set_index("lead_uuid")["currently_insured"].to_dict()
    policy_df["ci_flag"] = policy_df["lead_uuid"].map(ci_map)

    # ── Load existing JSON (to preserve spend data and nightly Notion fields) ─
    existing_spend   = {}   # period → {avenge_spend, google_ads_spend, ...}
    existing_notion  = {}   # full existing JSON keys not overwritten here
    if EXISTING_JSON.exists():
        try:
            with open(EXISTING_JSON) as f:
                old = json.load(f)
            for row in old.get("weekly_log", []):
                existing_spend[row["period"]] = {
                    "avenge_spend":      row.get("avenge_spend"),
                    "google_ads_spend":  row.get("google_ads_spend"),
                    "total_spend":       row.get("total_spend"),
                    "cpl_blended":       row.get("cpl_blended"),
                    "cost_per_bound":    row.get("cost_per_bound"),
                    "quote_roas":        row.get("quote_roas"),
                    "notes":             row.get("notes", ""),
                }
            # Preserve keys managed entirely by fetch_notion.py / nightly Action
            existing_notion["acq_cost_log"] = old.get("acq_cost_log", [])
            existing_notion["notion_page_id"] = old.get("notion_page_id",
                "347b639a58048181bb1cc0f76892912c")
        except Exception as e:
            print(f"Warning: could not read existing bindable_data.json ({e}). Starting fresh.")

    # ── Monthly weekly_log ────────────────────────────────────────────────────
    all_months = sorted(set(
        list(lead_df["month"].dropna().unique()) +
        list(policy_df["month"].dropna().unique())
    ))

    weekly_log = []
    for m in all_months:
        l = lead_df[lead_df["month"] == m]
        p = policy_df[policy_df["month"] == m]
        leads      = len(l)
        ci_leads   = safe_int((l["currently_insured"] == "CI").sum())
        nci_leads  = safe_int((l["currently_insured"] == "NCI").sum())
        policies   = len(p)
        bind_rate  = round(policies / leads * 100, 1) if leads else 0.0
        sp         = existing_spend.get(m, {})

        weekly_log.append({
            "period":          m,
            "leads":           leads,
            "ci_leads":        ci_leads,
            "nci_leads":       nci_leads,
            # Quote completions default to lead counts (updated by nightly Notion pull)
            "ci_quotes_done":  ci_leads,
            "nci_quotes_done": nci_leads,
            "quote_revenue":   round(ci_leads * CI_QUOTE_RATE + nci_leads * NCI_QUOTE_RATE, 2),
            "bound_policies":  policies,
            "bind_rate":       bind_rate,
            # Spend fields — preserved from existing JSON (nightly Action owns these)
            "avenge_spend":    sp.get("avenge_spend"),
            "google_ads_spend":sp.get("google_ads_spend"),
            "total_spend":     sp.get("total_spend"),
            "cpl_blended":     sp.get("cpl_blended"),
            "cost_per_bound":  sp.get("cost_per_bound"),
            "quote_roas":      sp.get("quote_roas"),
            "notes":           sp.get("notes", ""),
        })

    # ── Totals ────────────────────────────────────────────────────────────────
    total_leads    = len(lead_df)
    total_policies = len(policy_df)
    total_premium  = float(policy_df["written_premium"].sum())
    avg_premium    = round(total_premium / total_policies, 2) if total_policies else 0.0

    # CI / NCI splits
    ci_p  = policy_df[policy_df["ci_flag"] == "CI"]
    nci_p = policy_df[policy_df["ci_flag"] == "NCI"]
    ci_premium  = float(ci_p["written_premium"].sum())
    nci_premium = float(nci_p["written_premium"].sum())

    # ── policy_analytics ─────────────────────────────────────────────────────
    carrier_bd = []
    for carrier, grp in policy_df.groupby("carrier_name"):
        carrier_bd.append({
            "carrier":      carrier,
            "policies":     len(grp),
            "avg_premium":  safe_int(grp["written_premium"].mean()),
            "total_written":round(float(grp["written_premium"].sum()), 2),
            "pct_of_total": round(len(grp) / total_policies * 100, 1),
        })
    carrier_bd.sort(key=lambda x: x["policies"], reverse=True)

    state_bd = []
    for state, grp in policy_df.groupby("state"):
        state_bd.append({
            "state":      state,
            "policies":   len(grp),
            "avg_premium":safe_int(grp["written_premium"].mean()),
            "pct_of_total":round(len(grp) / total_policies * 100, 1),
        })
    state_bd.sort(key=lambda x: x["policies"], reverse=True)

    policy_analytics = {
        "total_policies":       total_policies,
        "total_written_premium":round(total_premium, 2),
        "avg_written_premium":  avg_premium,
        "carrier_breakdown":    carrier_bd,
        "ci_stats": {
            "count":       len(ci_p),
            "avg_premium": safe_int(ci_p["written_premium"].mean()) if len(ci_p) else 0,
        },
        "nci_stats": {
            "count":       len(nci_p),
            "avg_premium": safe_int(nci_p["written_premium"].mean()) if len(nci_p) else 0,
        },
        "state_breakdown": state_bd[:15],
    }

    # ── policy_log (full row-level detail for dashboard PDF export) ───────────
    lead_details = lead_df.set_index("lead_uuid")[
        ["currently_insured", "record_creation_time", "lead_source"]
    ].to_dict("index")

    policy_log = []
    for _, row in policy_df.sort_values("date_policy_bound").iterrows():
        uuid      = str(row["lead_uuid"])
        lead_info = lead_details.get(uuid, {})
        dq_raw    = lead_info.get("record_creation_time")
        db_raw    = row["date_policy_bound"]
        dq_str    = dq_raw.date().isoformat() if pd.notna(dq_raw) else None
        db_str    = db_raw.date().isoformat() if pd.notna(db_raw) else None
        try:
            dtb = (db_raw.date() - dq_raw.date()).days if dq_str and db_str else None
        except Exception:
            dtb = None
        ci_flag   = row.get("ci_flag", "")
        policy_log.append({
            "lead_uuid":            uuid,
            "date_quote_completed": dq_str,
            "date_policy_bound":    db_str,
            "days_to_bind":         dtb,
            "ci_nci":               ci_flag if pd.notna(ci_flag) else "",
            "carrier":              str(row.get("carrier_name", "") or ""),
            "written_premium":      safe_float(row["written_premium"]),
            "term_months":          safe_int(row.get("term", 0) or row.get("term_months", 0)),
            "state":                str(row.get("state", "") or ""),
            "lob":                  str(row.get("lob", "") or row.get("LOB", "") or ""),
            "lead_source":          str(lead_info.get("lead_source", "") or ""),
            "notes":                "",
        })

    # ── baseline (top KPI row in dashboard) ───────────────────────────────────
    nosale       = lead_df[lead_df["current_status"] == "No Sale"]
    too_exp_rate = round(
        len(nosale[nosale["current_disposition"] == "Too Expensive"]) /
        len(nosale) * 100, 1
    ) if len(nosale) else 0.0

    # Bind rates by CI/NCI
    ci_leads_total  = len(lead_df[lead_df["currently_insured"] == "CI"])
    nci_leads_total = len(lead_df[lead_df["currently_insured"] == "NCI"])
    ci_bound        = len(policy_df[policy_df["ci_flag"] == "CI"])
    nci_bound       = len(policy_df[policy_df["ci_flag"] == "NCI"])
    ci_bind_rate    = round(ci_bound  / ci_leads_total  * 100, 1) if ci_leads_total  else 0.0
    nci_bind_rate   = round(nci_bound / nci_leads_total * 100, 1) if nci_leads_total else 0.0
    bind_rate_all   = round(total_policies / total_leads * 100, 1) if total_leads else 0.0

    # Post-campaign quote revenue (from weekly_log rows on/after campaign start)
    post_rows   = [r for r in weekly_log
                   if pd.Period(r["period"], freq="M") >= pd.Period(CAMPAIGN_START, freq="M")]
    ci_q_post   = sum(r["ci_quotes_done"]  for r in post_rows)
    nci_q_post  = sum(r["nci_quotes_done"] for r in post_rows)
    quote_rev_post = ci_q_post * CI_QUOTE_RATE + nci_q_post * NCI_QUOTE_RATE

    # Data through date
    max_policy_date = policy_df["date_policy_bound"].max()
    data_through    = max_policy_date.strftime("%b %-d, %Y") if pd.notna(max_policy_date) else "Unknown"

    baseline = {
        "total_leads_alltime":    total_leads,
        "policies_bound_alltime": total_policies,
        "bind_rate_alltime":      bind_rate_all,
        "avg_written_premium":    safe_int(avg_premium),
        "too_expensive_rate":     too_exp_rate,
        "pipeline_in_process":    total_leads - total_policies,
        "ci_leads_alltime":       ci_leads_total,
        "nci_leads_alltime":      nci_leads_total,
        "ci_bind_rate":           ci_bind_rate,
        "nci_bind_rate":          nci_bind_rate,
        "quote_rev_post_mar27":   round(quote_rev_post, 2),
        "ci_quotes_post_mar27":   ci_q_post,
        "nci_quotes_post_mar27":  nci_q_post,
        "jan_2026_commission":    287.91,   # actual statement figure — update with each new statement
        "data_through":           data_through,
    }

    # ── quote_to_policy_cr ────────────────────────────────────────────────────
    total_q = sum(r["ci_quotes_done"] + r["nci_quotes_done"] for r in weekly_log)
    quote_to_policy_cr = {
        "cr_pct":                  round(total_policies / total_q * 100, 1) if total_q else None,
        "policies_from_quotes":    total_policies,
        "total_quotes_completed":  total_q,
        "ci_quotes":               sum(r["ci_quotes_done"]  for r in weekly_log),
        "nci_quotes":              sum(r["nci_quotes_done"] for r in weekly_log),
    }

    # ── live_rollup ───────────────────────────────────────────────────────────
    def rollup(rows):
        t = {"leads": 0, "ci_leads": 0, "nci_leads": 0,
             "ci_quotes": 0, "nci_quotes": 0,
             "bound": 0, "avenge_spend": 0.0,
             "google_ads_spend": 0.0, "total_spend": 0.0}
        for r in rows:
            t["leads"]           += r["leads"]
            t["ci_leads"]        += r["ci_leads"]
            t["nci_leads"]       += r["nci_leads"]
            t["ci_quotes"]       += r["ci_quotes_done"]
            t["nci_quotes"]      += r["nci_quotes_done"]
            t["bound"]           += r["bound_policies"]
            t["avenge_spend"]    += r["avenge_spend"]    or 0
            t["google_ads_spend"]+= r["google_ads_spend"]or 0
            t["total_spend"]     += r["total_spend"]     or 0
        t["quote_revenue"]  = t["ci_quotes"] * CI_QUOTE_RATE + t["nci_quotes"] * NCI_QUOTE_RATE
        t["bind_rate"]      = round(t["bound"] / t["leads"] * 100, 1) if t["leads"] else None
        t["cpl_blended"]    = round(t["total_spend"] / t["leads"], 2)  if t["leads"] and t["total_spend"] else None
        t["cost_per_bound"] = round(t["total_spend"] / t["bound"], 2)  if t["bound"] and t["total_spend"] else None
        t["quote_roas"]     = round(t["quote_revenue"] / t["total_spend"], 2) if t["total_spend"] > 0 else None
        return t

    live_rollup = rollup(weekly_log)

    # ── extended_analytics ───────────────────────────────────────────────────
    # Post-campaign data
    post_p   = policy_df[policy_df["date_policy_bound"] >= CAMPAIGN_START]
    post_ci  = post_p[post_p["ci_flag"] == "CI"]
    post_nci = post_p[post_p["ci_flag"] == "NCI"]
    post_premium = float(post_p["written_premium"].sum())

    cr = COMMISSION_RATES
    est_agency_alltime = (
        ci_premium  * cr["ci_agency_rate"] +
        nci_premium * cr["nci_agency_rate"]
    )
    est_rmc_alltime = (
        ci_premium  * cr["ci_effective_rmc_rate"] +
        nci_premium * cr["nci_effective_rmc_rate"]
    )
    est_rmc_post = (
        float(post_ci["written_premium"].sum())  * cr["ci_effective_rmc_rate"] +
        float(post_nci["written_premium"].sum()) * cr["nci_effective_rmc_rate"]
    )

    # Age bands
    age_band_defs = [(18,25),(26,35),(36,45),(46,55),(56,65),(66,120)]
    age_band_labels = ["18-25","26-35","36-45","46-55","56-65","65+"]
    policy_ages = policy_df["lead_uuid"].map(lead_df.set_index("lead_uuid")["age"])
    age_bands = []
    for (lo, hi), label in zip(age_band_defs, age_band_labels):
        l_band = lead_df[(lead_df["age"] >= lo) & (lead_df["age"] <= hi)]
        p_band = policy_df[(policy_ages >= lo) & (policy_ages <= hi)]
        ln, pn = len(l_band), len(p_band)
        age_bands.append({
            "band":         label,
            "leads":        ln,
            "bound":        pn,
            "bind_rate":    round(pn / ln * 100, 1) if ln else 0,
            "avg_premium":  safe_int(p_band["written_premium"].mean()) if pn else 0,
            "pct_of_leads": round(ln / total_leads * 100, 1) if total_leads else 0,
        })

    # Pipeline baseline (uses current bind rate and avg premium)
    pipeline_leads   = total_leads - total_policies
    est_policies     = round(pipeline_leads * (bind_rate_all / 100), 1)
    est_wp           = round(est_policies * avg_premium, 2)
    est_quote_rev    = round(pipeline_leads * 0.5 * CI_QUOTE_RATE +
                             pipeline_leads * 0.5 * NCI_QUOTE_RATE, 2)   # rough 50/50 split
    est_agency_pipe  = round(est_wp * (cr["ci_agency_rate"] + cr["nci_agency_rate"]) / 2, 2)
    est_rmc_pipe     = round(est_wp * cr["blended_rmc_rate"], 2)

    # Disposition breakdown
    disposition_breakdown = {
        k: int(v)
        for k, v in nosale["current_disposition"].value_counts().to_dict().items()
    }

    extended_analytics = {
        "commission_rates": COMMISSION_RATES,
        "monthly_actuals":  MONTHLY_ACTUALS,
        "written_premium": {
            "total_alltime":      round(total_premium, 2),
            "avg_alltime":        avg_premium,
            "policies_count":     total_policies,
            "ci_total":           round(ci_premium,  2),
            "nci_total":          round(nci_premium, 2),
            "post_mar27_total":   round(post_premium, 2),
            "post_mar27_policies":len(post_p),
        },
        "commission_model": {
            "alltime": {
                "written_premium":      round(total_premium, 2),
                "est_agency_commission":round(est_agency_alltime, 2),
                "est_rmc_commission":   round(est_rmc_alltime, 2),
                "est_gross_commission": round(est_agency_alltime, 2),  # alias used by dash
                "ci_policies":          len(ci_p),
                "nci_policies":         len(nci_p),
                "policies":             total_policies,
            },
            "post_mar27": {
                "written_premium":      round(post_premium, 2),
                "est_rmc_commission":   round(est_rmc_post, 2),
                "est_agency_commission":round(post_premium * (cr["ci_agency_rate"] + cr["nci_agency_rate"]) / 2, 2),
                "policies":             len(post_p),
                "ci_policies":          len(post_ci),
                "nci_policies":         len(post_nci),
                "quote_model_revenue":  round(quote_rev_post, 2),
                "ci_quotes":            ci_q_post,
                "nci_quotes":           nci_q_post,
                "quote_model_advantage":round(quote_rev_post - est_rmc_post, 2),
            },
        },
        "pipeline": {
            "leads_in_process": pipeline_leads,
            "baseline": {
                "cr_pct":               bind_rate_all,
                "avg_premium":          safe_int(avg_premium),
                "blended_rmc_rate":     round(cr["blended_rmc_rate"] * 100, 2),
                "est_policies":         est_policies,
                "est_written_premium":  est_wp,
                "est_agency_commission":est_agency_pipe,
                "est_gross_commission": est_agency_pipe,   # alias
                "est_rmc_commission":   est_rmc_pipe,
                "est_quote_revenue":    est_quote_rev,
            },
        },
        "age_bands":             age_bands,
        "disposition_breakdown": disposition_breakdown,
    }

    # ── Assemble final output ─────────────────────────────────────────────────
    output = {
        "generated_at":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000+00:00"),
        "notion_page_id":    existing_notion.get("notion_page_id",
                                "347b639a58048181bb1cc0f76892912c"),
        "baseline":          baseline,
        "weekly_log":        weekly_log,
        "policy_log":        policy_log,
        "acq_cost_log":      existing_notion.get("acq_cost_log", []),
        "live_rollup":       live_rollup,
        "policy_analytics":  policy_analytics,
        "quote_to_policy_cr":quote_to_policy_cr,
        "has_live_data":     total_leads > 0,
        "has_policy_data":   total_policies > 0,
        "extended_analytics":extended_analytics,
    }

    return output


# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Process Paperboy lead + policy exports into bindable_data.json"
    )
    parser.add_argument("--lead",   required=True, help="Path to lead report TXT file")
    parser.add_argument("--policy", required=True, help="Path to policy report TXT file")
    parser.add_argument("--out",    default="bindable_data.json",
                        help="Output path (default: bindable_data.json)")
    args = parser.parse_args()

    lead_path   = Path(args.lead)
    policy_path = Path(args.policy)
    out_path    = Path(args.out)

    for p in [lead_path, policy_path]:
        if not p.exists():
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    print(f"Processing {lead_path.name} + {policy_path.name} ...")
    data = build_bindable_data(lead_path, policy_path)

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    # ── Summary ───────────────────────────────────────────────────────────────
    b   = data["baseline"]
    ea  = data["extended_analytics"]
    cm  = ea["commission_model"]

    print(f"\n✅  bindable_data.json written to {out_path}")
    print(f"\n{'─'*48}")
    print(f"  Total leads:        {b['total_leads_alltime']:,}")
    print(f"  Total policies:     {b['policies_bound_alltime']:,}")
    print(f"  Overall bind rate:  {b['bind_rate_alltime']}%")
    print(f"  Avg written premium:{b['avg_written_premium']:,}")
    print(f"  Data through:       {b['data_through']}")
    print(f"  Est RMC commission: ${cm['alltime']['est_rmc_commission']:,.2f} (all-time)")
    print(f"  Post-Mar-27 RMC:    ${cm['post_mar27']['est_rmc_commission']:,.2f}")
    print(f"  Pipeline leads:     {ea['pipeline']['leads_in_process']:,}")
    print(f"{'─'*48}")
    print(f"\nNext step: commit {out_path} to GitHub → nightly Action adds spend data.")


if __name__ == "__main__":
    main()
