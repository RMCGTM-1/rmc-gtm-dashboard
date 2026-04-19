#!/usr/bin/env python3
"""
build_dashboard.py
Reads bindable_data.json and writes it alongside index.html.
The dashboard fetches bindable_data.json at runtime via fetch() —
no inline injection, no script-tag corruption issues.
"""

import json, sys
from datetime import datetime, timezone

def main():
    # Load data
    try:
        with open("bindable_data.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("ERROR: bindable_data.json not found. Run fetch_notion.py first.")
        sys.exit(1)

    # Validate index.html exists
    try:
        with open("index.html") as f:
            html = f.read()
    except FileNotFoundError:
        print("ERROR: index.html not found.")
        sys.exit(1)

    # Write the data file — dashboard fetches this at runtime
    with open("bindable_data.json", "w") as f:
        json.dump(data, f, indent=2)

    gen_at = data.get("generated_at", "")
    try:
        dt = datetime.fromisoformat(gen_at.replace("Z", "+00:00"))
        last_updated = dt.strftime("%b %d, %Y at %H:%M UTC")
    except Exception:
        last_updated = gen_at

    print(f"bindable_data.json written. Last updated: {last_updated}")
    print(f"Has live data: {data.get('has_live_data', False)}")
    print("Dashboard will fetch data client-side at runtime.")

if __name__ == "__main__":
    main()
