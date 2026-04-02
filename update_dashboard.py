#!/usr/bin/env python3
"""
update_dashboard.py — обновляет reach-dashboard/index.html свежими данными.

Источники:
  1. LiveDune API (через n8n webhook) — IG охваты постов и Reels
  2. LiveDune API (через n8n webhook) — TG Synchronews просмотры
  3. Google Sheets (через n8n webhook) — охваты блогеров по платформам

Запуск: python3 update_dashboard.py
"""

import json
import subprocess
import sys
import re
from datetime import datetime, date
from collections import defaultdict

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
N8N_BASE = "https://n8n.synchronize.ru"
N8N_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlOTM1YWVmZi01NDdmLTRjMTUtOWNjZS03OWVjMjIzZTE5MmEiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwMDIzNjA5fQ.7hMQTooyz_guX4ZqAxgGNcJU9p8WiWYxz_1Gf_WdHnA"

LIVEDUNE_TOKEN = "aa65e9a00de49734.59556736"
LD_IG_ACCOUNT = 2300066
LD_TG_ACCOUNT = 2315706

# Blogger sheet (gid=1182030426, contains TG/IG/YT breakdown)
BLOGGER_SHEET_ID = "1C0BcmKhZLQ8_93dkEo5M5vTqUWTVG36qF0fkrj2t6is"
BLOGGER_GID = "1182030426"

# Razmescheniya sheet (Sep 2025+)
RAZMESCHENIYA_SHEET_ID = "1Vi_R0PVSvdwTn9sFdL25oh7ZFJTFZ4MvXOlgmakKx_4"

# Path to HTML template
DASHBOARD_HTML = "/Users/phrmv/synchronize/reach-dashboard/index.html"

# Start month for dashboard
START_MONTH = "2023-07"

# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def curl_get(url, headers=None):
    """HTTP GET via subprocess curl (LibreSSL fix)."""
    cmd = ["curl", "-s", "--max-time", "30", url]
    if headers:
        for k, v in headers.items():
            cmd += ["-H", f"{k}: {v}"]
    result = subprocess.run(cmd, capture_output=True)
    raw = result.stdout
    # Strip control characters except tab/newline/cr
    clean = bytearray(b if b >= 0x20 or b in (0x09, 0x0a, 0x0d) else 0x20 for b in raw)
    return clean.decode("utf-8", errors="replace")


def curl_post(url, payload, headers=None):
    """HTTP POST JSON via subprocess curl."""
    body = json.dumps(payload)
    cmd = ["curl", "-s", "--max-time", "60", "-X", "POST",
           "-H", "Content-Type: application/json",
           "-d", body, url]
    if headers:
        for k, v in headers.items():
            cmd += ["-H", f"{k}: {v}"]
    result = subprocess.run(cmd, capture_output=True)
    raw = result.stdout
    clean = bytearray(b if b >= 0x20 or b in (0x09, 0x0a, 0x0d) else 0x20 for b in raw)
    return clean.decode("utf-8", errors="replace")


def parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}", file=sys.stderr)
        print(f"  First 300 chars: {text[:300]}", file=sys.stderr)
        return None


def months_range(start="2023-07", end=None):
    """Generate list of YYYY-MM strings from start to end (inclusive)."""
    if end is None:
        today = date.today()
        end = f"{today.year}-{today.month:02d}"
    result = []
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    while (y, m) <= (ey, em):
        result.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def month_bounds(ym):
    """Return (first_day, last_day) strings for YYYY-MM."""
    y, m = int(ym[:4]), int(ym[5:7])
    first = f"{y}-{m:02d}-01"
    if m == 12:
        last = f"{y}-12-31"
    else:
        import calendar
        last_day = calendar.monthrange(y, m)[1]
        last = f"{y}-{m:02d}-{last_day:02d}"
    return first, last


# ──────────────────────────────────────────────
# LIVEDUNE: fetch monthly reach via direct API
# ──────────────────────────────────────────────

def fetch_livedune_month(account_id, date_from, date_to, field="reach"):
    """
    Fetch all posts for account in [date_from, date_to], sum their reach/impressions.
    field='reach'       → IG posts/reels (reach.total)
    field='impressions' → TG posts (impressions.total)
    """
    base = f"https://api.livedune.com/accounts/{account_id}/posts"
    params = (
        f"access_token={LIVEDUNE_TOKEN}"
        f"&date_from={date_from}&date_to={date_to}"
        f"&limit=100"
    )
    total = 0
    after = None
    seen = set()
    page = 0

    while True:
        url = f"{base}?{params}"
        if after:
            url += f"&after={after}"
        page += 1

        text = curl_get(url)
        data = parse_json(text)
        if not data or "posts" not in data:
            break

        posts = data["posts"]
        if not posts:
            break

        for p in posts:
            pid = p.get("id") or p.get("post_id")
            if pid in seen:
                continue
            seen.add(pid)

            if field == "reach":
                r = p.get("reach", {})
                if isinstance(r, dict):
                    total += r.get("total", 0) or 0
                else:
                    total += r or 0
            else:  # impressions (TG)
                imp = p.get("impressions", {})
                if isinstance(imp, dict):
                    total += imp.get("total", 0) or 0
                else:
                    total += imp or 0

        after_new = data.get("after") or data.get("pagination", {}).get("after")
        if not after_new or after_new == after:
            break
        after = after_new

    print(f"    [{date_from}→{date_to}] account={account_id} pages={page} posts={len(seen)} {field}={total}")
    return total


def fetch_ig_monthly(months):
    """
    Returns two dicts: posts_reach[ym], reels_reach[ym]
    IG account: LD_IG_ACCOUNT, posts have type='post' or 'reel' / 'carousel'
    """
    posts = {}
    reels = {}

    base = f"https://api.livedune.com/accounts/{LD_IG_ACCOUNT}/posts"

    for ym in months:
        date_from, date_to = month_bounds(ym)
        params = (
            f"access_token={LIVEDUNE_TOKEN}"
            f"&date_from={date_from}&date_to={date_to}"
            f"&limit=100"
        )
        p_total = r_total = 0
        after = None
        seen = set()
        page = 0

        while True:
            url = f"{base}?{params}"
            if after:
                url += f"&after={after}"
            page += 1

            text = curl_get(url)
            data = parse_json(text)
            if not data or "posts" not in data:
                break

            pg_posts = data["posts"]
            if not pg_posts:
                break

            for p in pg_posts:
                pid = p.get("id") or p.get("post_id")
                if pid in seen:
                    continue
                seen.add(pid)

                reach_val = 0
                r = p.get("reach", {})
                if isinstance(r, dict):
                    reach_val = r.get("total", 0) or 0
                else:
                    reach_val = r or 0

                ptype = (p.get("type") or "").lower()
                if "reel" in ptype:
                    r_total += reach_val
                else:
                    p_total += reach_val

            after_new = data.get("after") or data.get("pagination", {}).get("after")
            if not after_new or after_new == after:
                break
            after = after_new

        print(f"  IG {ym}: posts={p_total:,}  reels={r_total:,}  (pages={page}, n={len(seen)})")
        posts[ym] = p_total
        reels[ym] = r_total

    return posts, reels


def fetch_tg_monthly(months):
    """Returns dict tg[ym] = total impressions."""
    tg = {}
    base = f"https://api.livedune.com/accounts/{LD_TG_ACCOUNT}/posts"

    for ym in months:
        date_from, date_to = month_bounds(ym)
        total = 0
        params = (
            f"access_token={LIVEDUNE_TOKEN}"
            f"&date_from={date_from}&date_to={date_to}"
            f"&limit=100"
        )
        after = None
        seen = set()
        page = 0

        while True:
            url = f"{base}?{params}"
            if after:
                url += f"&after={after}"
            page += 1

            text = curl_get(url)
            data = parse_json(text)
            if not data or "posts" not in data:
                break

            posts = data["posts"]
            if not posts:
                break

            for p in posts:
                pid = p.get("id") or p.get("post_id")
                if pid in seen:
                    continue
                seen.add(pid)
                imp = p.get("impressions", {})
                if isinstance(imp, dict):
                    total += imp.get("total", 0) or 0
                else:
                    total += imp or 0

            after_new = data.get("after") or data.get("pagination", {}).get("after")
            if not after_new or after_new == after:
                break
            after = after_new

        print(f"  TG {ym}: views={total:,}  (pages={page}, n={len(seen)})")
        tg[ym] = total

    return tg


# ──────────────────────────────────────────────
# BLOGGERS: read from Google Sheets via n8n
# ──────────────────────────────────────────────

def fetch_blogger_sheet_via_n8n():
    """
    Read blogger reach sheet via n8n (Google Sheets OAuth credential).
    Sheet: 1C0BcmKhZLQ8_93dkEo5M5vTqUWTVG36qF0fkrj2t6is, gid=1182030426
    Returns list of row dicts.
    """
    # We use the n8n API to execute a one-shot read via HTTP Request node approach
    # Actually easier: use the n8n execute workflow endpoint for a helper workflow
    # For simplicity, use gviz trick first, fall back to n8n execution
    url = (
        f"https://docs.google.com/spreadsheets/d/{BLOGGER_SHEET_ID}/gviz/tq"
        f"?tqx=out:json&gid={BLOGGER_GID}"
    )
    text = curl_get(url)

    # Check if we got a login page (not public)
    if "accounts.google.com" in text or "<!DOCTYPE" in text.lower():
        print("  Blogger sheet not public, fetching via n8n...")
        return fetch_blogger_sheet_n8n_api()

    # Parse gviz response
    match = re.search(r"google\.visualization\.Query\.setResponse\((.*)\)", text, re.DOTALL)
    if not match:
        print("  gviz parse failed, trying n8n...")
        return fetch_blogger_sheet_n8n_api()

    data = parse_json(match.group(1))
    if not data:
        return fetch_blogger_sheet_n8n_api()

    return parse_gviz_table(data)


def fetch_blogger_sheet_n8n_api():
    """
    Fetch blogger sheet rows via n8n Sheets API (credential BDOb1HHgzz9BfYJF).
    Creates a temporary execution of a read-sheet workflow.
    """
    # Use n8n API to read sheets — we'll do it via a simple HTTP call
    # through n8n's Google Sheets credential proxy approach.
    # Best approach: use the existing workflow that reads this sheet.
    # For now, build a direct n8n HTTP proxy call.

    # n8n webhook endpoint — we need to use the Google Sheets API endpoint
    # via n8n credential. The simplest: call Sheets API v4 directly via n8n
    # as a passthrough, using OAuth token from credential BDOb1HHgzz9BfYJF.

    # Alternative: call the Sheets API v4 directly with service account
    # (we have BigQuery SA but not Sheets SA for this sheet)

    # Simplest working approach: create temp workflow via n8n API, activate, call, delete
    return fetch_via_temp_n8n_workflow()


def fetch_via_temp_n8n_workflow():
    """Create a temp n8n workflow to read the sheet, execute it, clean up."""
    SHEETS_CRED_ID = "BDOb1HHgzz9BfYJF"
    WEBHOOK_PATH = "blogger-reach-tmp-read"

    workflow_def = {
        "name": "__tmp_blogger_reach_read",
        "nodes": [
            {
                "id": "wh1",
                "name": "Webhook",
                "type": "n8n-nodes-base.webhook",
                "typeVersion": 2,
                "position": [0, 0],
                "webhookId": "blogger-reach-tmp-001",
                "parameters": {
                    "httpMethod": "GET",
                    "path": WEBHOOK_PATH,
                    "responseMode": "responseNode"
                }
            },
            {
                "id": "sh1",
                "name": "Read Sheet",
                "type": "n8n-nodes-base.googleSheets",
                "typeVersion": 4.4,
                "position": [200, 0],
                "credentials": {"googleSheetsOAuth2Api": {"id": SHEETS_CRED_ID, "name": "Google Sheets account"}},
                "parameters": {
                    "operation": "read",
                    "documentId": {"__rl": True, "value": BLOGGER_SHEET_ID, "mode": "id"},
                    "sheetName": {"__rl": True, "value": BLOGGER_GID, "mode": "id"},
                    "options": {}
                }
            },
            {
                "id": "resp1",
                "name": "Respond",
                "type": "n8n-nodes-base.respondToWebhook",
                "typeVersion": 1,
                "position": [400, 0],
                "parameters": {
                    "respondWith": "allIncomingItems",
                    "options": {}
                }
            }
        ],
        "connections": {
            "Webhook": {"main": [[{"node": "Read Sheet", "type": "main", "index": 0}]]},
            "Read Sheet": {"main": [[{"node": "Respond", "type": "main", "index": 0}]]}
        },
        "settings": {"executionOrder": "v1"}
    }

    headers = {"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"}

    # Create workflow
    print("  Creating temp n8n workflow for blogger sheet read...")
    resp = curl_post(f"{N8N_BASE}/api/v1/workflows", workflow_def,
                     {"X-N8N-API-KEY": N8N_API_KEY})
    wf_data = parse_json(resp)
    if not wf_data or "id" not in wf_data:
        print(f"  Failed to create workflow: {resp[:200]}", file=sys.stderr)
        return []
    wf_id = wf_data["id"]
    print(f"  Workflow created: {wf_id}")

    # Activate
    activate_resp = curl_post(f"{N8N_BASE}/api/v1/workflows/{wf_id}/activate", {},
                              {"X-N8N-API-KEY": N8N_API_KEY})
    print(f"  Activated: {activate_resp[:80]}")

    # Call webhook
    import time
    time.sleep(2)
    webhook_url = f"{N8N_BASE}/webhook/{WEBHOOK_PATH}"
    print(f"  Calling: {webhook_url}")
    data_text = curl_get(webhook_url)
    rows = parse_json(data_text)

    # Deactivate + delete
    deact_cmd = ["curl", "-s", "-X", "POST",
                 f"{N8N_BASE}/api/v1/workflows/{wf_id}/deactivate",
                 "-H", f"X-N8N-API-KEY: {N8N_API_KEY}"]
    subprocess.run(deact_cmd, capture_output=True)

    del_cmd = ["curl", "-s", "-X", "DELETE",
               f"{N8N_BASE}/api/v1/workflows/{wf_id}",
               "-H", f"X-N8N-API-KEY: {N8N_API_KEY}"]
    subprocess.run(del_cmd, capture_output=True)
    print(f"  Temp workflow {wf_id} deleted.")

    if isinstance(rows, list):
        return rows
    return []


def parse_gviz_table(data):
    """Parse gviz JSON table response into list of row dicts."""
    table = data.get("table", {})
    cols = [c.get("label", "") for c in table.get("cols", [])]
    rows_raw = table.get("rows", [])
    rows = []
    for row in rows_raw:
        cells = row.get("c", [])
        d = {}
        for i, cell in enumerate(cells):
            if i < len(cols):
                val = cell.get("v") if cell else None
                d[cols[i]] = val
        rows.append(d)
    return rows


def aggregate_blogger_reach(rows):
    """
    Aggregate blogger reach by month and platform (TG/Instagram/YouTube).
    Returns: blog_tg[ym], blog_ig[ym], blog_yt[ym]
    """
    blog_tg = defaultdict(int)
    blog_ig = defaultdict(int)
    blog_yt = defaultdict(int)

    if not rows:
        return blog_tg, blog_ig, blog_yt

    print(f"  Processing {len(rows)} blogger rows...")
    # Detect column names
    sample = rows[0] if rows else {}
    all_keys = list(sample.keys())
    print(f"  Columns: {all_keys[:15]}")

    # Find relevant columns — flexible matching
    def find_col(keywords, cols):
        keywords_lower = [k.lower() for k in keywords]
        for col in cols:
            col_l = col.lower().strip()
            if all(k in col_l for k in keywords_lower):
                return col
        return None

    date_col = find_col(["дата"], all_keys) or find_col(["date"], all_keys)
    platform_col = find_col(["площадка"], all_keys) or find_col(["platform"], all_keys)
    reach_col = (find_col(["охват"], all_keys) or find_col(["reach"], all_keys)
                 or find_col(["просмотр"], all_keys))
    status_col = find_col(["статус"], all_keys)

    print(f"  date_col={date_col}, platform_col={platform_col}, reach_col={reach_col}, status_col={status_col}")

    if not (date_col and platform_col and reach_col):
        print("  WARNING: could not identify required columns", file=sys.stderr)
        return blog_tg, blog_ig, blog_yt

    skipped = 0
    processed = 0
    for row in rows:
        # Filter by status if available
        if status_col:
            status = str(row.get(status_col) or "").lower()
            if status and status not in ("прошла", "готово", "done", ""):
                skipped += 1
                continue

        date_val = row.get(date_col)
        platform_val = str(row.get(platform_col) or "").lower().strip()
        reach_val = row.get(reach_col)

        if not date_val or reach_val is None:
            skipped += 1
            continue

        # Parse date → YYYY-MM
        date_str = str(date_val)
        ym = None
        # Various formats: DD.MM.YYYY, YYYY-MM-DD, MM/DD/YYYY
        for pattern, fmt in [
            (r"^(\d{2})\.(\d{2})\.(\d{4})$", "dmy"),
            (r"^(\d{4})-(\d{2})-(\d{2})", "ymd"),
            (r"^(\d{2})/(\d{2})/(\d{4})$", "mdy"),
        ]:
            m = re.match(pattern, date_str)
            if m:
                g = m.groups()
                if fmt == "dmy":
                    ym = f"{g[2]}-{g[1]}"
                elif fmt == "ymd":
                    ym = f"{g[0]}-{g[1]}"
                elif fmt == "mdy":
                    ym = f"{g[2]}-{g[0]}"
                break
        # gviz date format: Date(YYYY,M,D) — month is 0-indexed
        if not ym and "Date(" in date_str:
            m = re.search(r"Date\((\d+),(\d+),(\d+)\)", date_str)
            if m:
                y, mo, d = int(m.group(1)), int(m.group(2)) + 1, int(m.group(3))
                ym = f"{y}-{mo:02d}"

        if not ym:
            skipped += 1
            continue

        try:
            reach_int = int(float(str(reach_val).replace(" ", "").replace(",", ".")))
        except (ValueError, TypeError):
            skipped += 1
            continue

        if "telegram" in platform_val or " tg" in platform_val or platform_val == "tg":
            blog_tg[ym] += reach_int
        elif "instagram" in platform_val or "insta" in platform_val or "ig" in platform_val:
            blog_ig[ym] += reach_int
        elif "youtube" in platform_val or "yt" in platform_val:
            blog_yt[ym] += reach_int

        processed += 1

    print(f"  Processed={processed}, skipped={skipped}")
    return blog_tg, blog_ig, blog_yt


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def build_data(months):
    """Fetch all sources and return DATA dict."""

    print("\n=== Fetching IG reach (posts + reels) ===")
    ig_posts_dict, ig_reels_dict = fetch_ig_monthly(months)

    print("\n=== Fetching TG Synchronews views ===")
    tg_dict = fetch_tg_monthly(months)

    print("\n=== Fetching blogger reach ===")
    rows = fetch_blogger_sheet_via_n8n()
    blog_tg_dict, blog_ig_dict, blog_yt_dict = aggregate_blogger_reach(rows)

    # Build ordered arrays
    data = {
        "months": months,
        "ig_posts": [ig_posts_dict.get(m, 0) for m in months],
        "ig_reels": [ig_reels_dict.get(m, 0) for m in months],
        "tg_sync":  [tg_dict.get(m, 0) for m in months],
        "blog_tg":  [blog_tg_dict.get(m, 0) for m in months],
        "blog_ig":  [blog_ig_dict.get(m, 0) for m in months],
        "blog_yt":  [blog_yt_dict.get(m, 0) for m in months],
    }

    return data


def update_html(data):
    """Replace DATA_JSON and DATA_UPDATED placeholders in index.html."""
    with open(DASHBOARD_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Replace data JSON
    new_json = json.dumps(data, ensure_ascii=False)
    html = re.sub(
        r"/\* DATA_JSON \*/.*?/\* /DATA_JSON \*/",
        f"/* DATA_JSON */{new_json}/* /DATA_JSON */",
        html,
        flags=re.DOTALL
    )

    # Replace updated date
    today_str = date.today().strftime("%d.%m.%Y")
    # In the JS constant
    html = html.replace(
        'const UPDATED = "<!-- DATA_UPDATED -->";',
        f'const UPDATED = "{today_str}";'
    )
    # In the header span (for static render)
    html = re.sub(
        r'<span id="updated-date">.*?</span>',
        f'<span id="updated-date">{today_str}</span>',
        html
    )

    with open(DASHBOARD_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDashboard updated: {DASHBOARD_HTML}")
    print(f"Updated date: {today_str}")


def git_push():
    """Commit and push changes to GitHub."""
    import os
    repo_dir = "/Users/phrmv/synchronize/reach-dashboard"
    today_str = date.today().strftime("%Y-%m-%d")

    cmds = [
        f"cd {repo_dir} && git add index.html",
        f"cd {repo_dir} && git commit -m 'data: update reach dashboard {today_str}'",
        f"cd {repo_dir} && git push origin main",
    ]

    for cmd in cmds:
        print(f"  $ {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout:
            print(f"    {result.stdout.strip()}")
        if result.stderr:
            print(f"    {result.stderr.strip()}")
        if result.returncode != 0:
            print(f"  WARNING: command exited {result.returncode}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Update reach dashboard")
    parser.add_argument("--no-push", action="store_true", help="Skip git push")
    parser.add_argument("--months", type=int, default=0,
                        help="Fetch only last N months (0 = all from START_MONTH)")
    args = parser.parse_args()

    all_months = months_range(START_MONTH)
    if args.months > 0:
        all_months = all_months[-args.months:]

    print(f"Updating dashboard for {len(all_months)} months: {all_months[0]} → {all_months[-1]}")

    data = build_data(all_months)

    print("\n=== Summary ===")
    for key in ["ig_posts", "ig_reels", "tg_sync", "blog_tg", "blog_ig", "blog_yt"]:
        vals = data[key]
        total = sum(vals)
        non_zero = sum(1 for v in vals if v > 0)
        print(f"  {key}: total={total:,}  non-zero months={non_zero}")

    update_html(data)

    if not args.no_push:
        print("\n=== Git push ===")
        git_push()
    else:
        print("\nSkipped git push (--no-push)")
