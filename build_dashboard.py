#!/usr/bin/env python3
"""
build_dashboard.py
Reads bindable_data.json, writes it alongside index.html (for fetch() fallback),
AND injects it as window.__BINDABLE_DATA directly into index.html so the
dashboard works when opened as a local file (file://) or from GitHub Pages
without needing a separate network request.
"""

import json, sys, re
from datetime import datetime, timezone

INJECT_MARKER = "<!-- __BINDABLE_DATA_INJECT__ -->"

def main():
    # Load data
    try:
        with open("bindable_data.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("ERROR: bindable_data.json not found. Run fetch_notion.py first.")
        sys.exit(1)

    # Load index.html
    try:
        with open("index.html") as f:
            html = f.read()
    except FileNotFoundError:
        print("ERROR: index.html not found.")
        sys.exit(1)

    # Build the inline script block
    data_json = json.dumps(data, separators=(',', ':'))
    inject_script = (
        f"{INJECT_MARKER}\n"
        f"<script>window.__BINDABLE_DATA = {data_json};</script>"
    )

    # Replace existing inject block if present, otherwise insert before </head>
    if INJECT_MARKER in html:
        html = re.sub(
            r'<!-- __BINDABLE_DATA_INJECT__ -->.*?</script>',
            inject_script,
            html,
            flags=re.DOTALL
        )
    else:
        html = html.replace("</head>", inject_script + "\n</head>", 1)

    # Write updated index.html
    with open("index.html", "w") as f:
        f.write(html)

    # Also keep the .json file for fetch() fallback
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
    print("Data injected inline into index.html — works as file:// and on GitHub Pages.")

if __name__ == "__main__":
    main()
