"""
Agent: New Publisher Ramp Tracker
===================================
Tracks day 1-7 revenue growth for publishers that appeared
in the last 7 days. Tells the team which new publishers are
ramping well and which are stalling so they know where to focus.

For each new publisher shows:
  - Daily revenue for each day since first seen
  - Day-on-day growth trend
  - Top demand partners buying their inventory
  - eCPM and win rate trajectory
  - Status: Ramping / Stalling / Strong Start

Schedule: Daily at 8:30 AM ET
Cron: 0 12 30 * * *  — or use: 30 12 * * *
"""

import os
import json
import requests
from datetime import date, timedelta
from collections import defaultdict
from api import fetch, sf, pct

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")
STATE_FILE    = "/tmp/pub_ramp_state.json"

# ── Config ────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS       = 14    # how far back to look for "new" publishers
NEW_PUB_WINDOW      = 7     # publishers first seen within this many days = "new"
MIN_DAY1_REVENUE    = 20    # $ — ignore new publishers earning less than this on day 1
STRONG_START_REV    = 200   # $ — day 1 revenue that counts as a strong start
STALL_THRESHOLD     = 30    # % — day-on-day drop that signals stalling
MAX_PUBLISHERS      = 8     # max new publishers to show


# ── State helpers ─────────────────────────────────────────────────────────────

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_pub_daily_revenue(date_str: str) -> dict:
    """Returns {publisher_name: {revenue, impressions, ecpm, win_rate}}"""
    try:
        rows = fetch("PUBLISHER",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS", "GROSS_ECPM"],
            date_str, date_str)
        out = {}
        for r in rows:
            pub  = r.get("PUBLISHER_NAME", "").strip()
            if not pub: continue
            rev  = sf(r.get("GROSS_REVENUE", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = sf(r.get("GROSS_ECPM", 0)) or (rev / imps * 1000 if imps > 0 else 0)
            out[pub] = {
                "revenue":   rev,
                "impressions": imps,
                "ecpm":      ecpm,
                "win_rate":  pct(wins, bids),
            }
        return out
    except Exception as e:
        print(f"      [Pub revenue fetch error for {date_str}: {e}]")
        return {}


def get_pub_demand(pub_name: str, from_date: str, to_date: str) -> list:
    """Get top demand partners for a specific publisher."""
    try:
        rows = fetch("PUBLISHER,DEMAND_PARTNER_NAME",
            ["GROSS_REVENUE", "WINS", "BIDS", "GROSS_ECPM"],
            from_date, to_date)
        out = []
        for r in rows:
            if r.get("PUBLISHER_NAME", "").strip() != pub_name: continue
            dem  = r.get("DEMAND_PARTNER_NAME", "").strip()
            rev  = sf(r.get("GROSS_REVENUE", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = sf(r.get("GROSS_ECPM", 0))
            if dem and rev > 0:
                out.append({
                    "name":     dem,
                    "revenue":  rev,
                    "ecpm":     ecpm,
                    "win_rate": pct(wins, bids),
                })
        return sorted(out, key=lambda x: x["revenue"], reverse=True)[:3]
    except Exception:
        return []


# ── Analysis ──────────────────────────────────────────────────────────────────

def classify_ramp(daily_revs: list) -> tuple:
    """
    Returns (status, trend_description) based on revenue trajectory.
    daily_revs: list of revenue values from day 1 to today, oldest first.
    """
    if not daily_revs or len(daily_revs) < 2:
        return "🌱 New", "Only 1 day of data"

    latest = daily_revs[-1]
    first  = daily_revs[0]
    peak   = max(daily_revs)

    # Overall growth from day 1
    overall_growth = (latest - first) / first * 100 if first > 0 else 0

    # Recent trend (last 2 days)
    recent_change = (daily_revs[-1] - daily_revs[-2]) / daily_revs[-2] * 100 if daily_revs[-2] > 0 else 0

    if first >= STRONG_START_REV and overall_growth > 20:
        return "🚀 Strong Ramp", f"Up {overall_growth:.0f}% from day 1, {recent_change:+.0f}% today"
    elif overall_growth > 10:
        return "📈 Ramping", f"Up {overall_growth:.0f}% from day 1"
    elif recent_change < -STALL_THRESHOLD:
        return "⚠️ Stalling", f"Down {abs(recent_change):.0f}% today vs yesterday"
    elif latest < first * 0.5:
        return "📉 Declining", f"Down {abs(overall_growth):.0f}% from day 1"
    elif first >= STRONG_START_REV:
        return "✅ Stable Start", f"Holding steady at {fmt_usd(latest)}/day"
    else:
        return "🌱 Early Stage", f"Day {len(daily_revs)} — monitoring"


def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


# ── Slack builder ─────────────────────────────────────────────────────────────

def build_slack_payload(new_pubs: list, date_str: str) -> dict:
    if not new_pubs:
        return {
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn",
                 "text": f"📊 *New Publisher Ramp Report — {date_str}*\n\nNo new publishers to track today."}}
            ]
        }

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"🌱 New Publisher Ramp Report", "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{date_str}* — {len(new_pubs)} new publisher{'s' if len(new_pubs) > 1 else ''} "
                    f"active in last {NEW_PUB_WINDOW} days. "
                    f"Tracking day-by-day revenue trajectory."
                )
            }
        },
        {"type": "divider"},
    ]

    for pub in new_pubs:
        daily_revs = pub["daily_revenues"]
        status, trend_desc = classify_ramp(daily_revs)

        # Daily sparkline text
        day_labels = [f"D{i+1}: {fmt_usd(r)}" for i, r in enumerate(daily_revs)]
        sparkline  = " → ".join(day_labels)

        # Demand partners
        dem_str = " · ".join(
            f"{d['name'][:20]} ({fmt_usd(d['revenue'])})"
            for d in pub.get("demand", [])
        ) or "No demand data"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{pub['publisher']}*  {status}\n"
                    f"  {sparkline}\n"
                    f"  Today: {fmt_usd(pub['today_rev'])} | "
                    f"eCPM: ${pub['ecpm']:.3f} | "
                    f"Win rate: {pub['win_rate']:.1f}%\n"
                    f"  🎯 Demand: {dem_str}\n"
                    f"  💡 _{trend_desc}_"
                )
            }
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                      "text": (
                          f"New = first seen within {NEW_PUB_WINDOW} days | "
                          f"Min D1 revenue: {fmt_usd(MIN_DAY1_REVENUE)} | "
                          f"PGAM Alerts — daily 8:30 AM ET"
                      )}]
    })

    return {"blocks": blocks}


# ── Main runner ───────────────────────────────────────────────────────────────

def run_publisher_ramp_agent():
    today     = date.today()
    yesterday = today - timedelta(days=1)
    date_str  = yesterday.strftime("%Y-%m-%d")

    print(f"Building publisher ramp report for {date_str}...")

    # Get publisher revenue for each of the last LOOKBACK_DAYS days
    daily_snapshots = {}
    for days_back in range(LOOKBACK_DAYS):
        d     = yesterday - timedelta(days=days_back)
        d_str = d.strftime("%Y-%m-%d")
        daily_snapshots[d_str] = get_pub_daily_revenue(d_str)

    # Find publishers that first appeared within NEW_PUB_WINDOW days
    # "First appeared" = not in any snapshot older than NEW_PUB_WINDOW days
    all_dates    = sorted(daily_snapshots.keys())
    recent_dates = all_dates[-NEW_PUB_WINDOW:]
    older_dates  = all_dates[:-NEW_PUB_WINDOW]

    older_pubs = set()
    for d in older_dates:
        older_pubs.update(daily_snapshots[d].keys())

    new_pub_names = set()
    for d in recent_dates:
        for pub, data in daily_snapshots[d].items():
            if pub not in older_pubs and data["revenue"] >= MIN_DAY1_REVENUE:
                new_pub_names.add(pub)

    print(f"  New publishers found: {len(new_pub_names)}")

    if not new_pub_names:
        print("  No new publishers to report.")
        return

    # Build trajectory for each new publisher
    new_pubs = []
    for pub in new_pub_names:
        daily_revs = []
        first_date = None

        for d in recent_dates:
            rev = daily_snapshots[d].get(pub, {}).get("revenue", 0)
            if rev > 0 and first_date is None:
                first_date = d
            if first_date is not None:
                daily_revs.append(rev)

        if not daily_revs or daily_revs[0] < MIN_DAY1_REVENUE:
            continue

        today_data = daily_snapshots.get(date_str, {}).get(pub, {})
        today_rev  = today_data.get("revenue", 0)
        ecpm       = today_data.get("ecpm", 0)
        win_rate   = today_data.get("win_rate", 0)

        # Get demand context
        window_start = (yesterday - timedelta(days=NEW_PUB_WINDOW)).strftime("%Y-%m-%d")
        demand = get_pub_demand(pub, window_start, date_str)

        new_pubs.append({
            "publisher":       pub,
            "first_date":      first_date,
            "daily_revenues":  daily_revs,
            "today_rev":       today_rev,
            "ecpm":            ecpm,
            "win_rate":        win_rate,
            "demand":          demand,
            "total_rev":       sum(daily_revs),
        })

    # Sort by total revenue descending
    new_pubs = sorted(new_pubs, key=lambda x: x["total_rev"], reverse=True)[:MAX_PUBLISHERS]

    print(f"  Qualified publishers: {len(new_pubs)}")

    payload = build_slack_payload(new_pubs, date_str)

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    print(f"Publisher ramp report sent ✅ ({len(new_pubs)} publishers)")


if __name__ == "__main__":
    run_publisher_ramp_agent()
