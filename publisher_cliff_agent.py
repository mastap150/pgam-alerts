"""
Agent: Publisher Revenue Cliff Monitor
========================================
48-hour rolling check. Fires when any publisher drops >40%
in revenue day-on-day. Enriches with:
  - Which demand partners are most affected
  - Whether it's supply-side (impressions down) or demand-side (eCPM/win rate down)
  - Root cause signal

Fires max once per publisher per day to avoid spam.

Schedule: Every 4 hours
Cron: schedule.every(4).hours in main.py
  OR: 0 */4 * * * in Render cron for pgam-alerts
"""

import os
import json
import requests
from datetime import date, timedelta
from collections import defaultdict
from api import fetch, sf, pct

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")
STATE_FILE    = "/tmp/pub_cliff_state.json"

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_YESTERDAY_REVENUE = 100    # $ — only watch publishers earning >$100/day
DROP_THRESHOLD_PCT    = 40     # % — revenue drop to trigger
MAX_PUBLISHERS        = 5      # max publishers to surface per alert


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


def already_alerted_today(state: dict, pub: str) -> bool:
    today = date.today().strftime("%Y-%m-%d")
    return state.get(f"{today}_{pub}", False)


def mark_alerted(state: dict, pub: str):
    today = date.today().strftime("%Y-%m-%d")
    state[f"{today}_{pub}"] = True
    # Prune old dates
    for key in list(state.keys()):
        if key[:10] < today:
            del state[key]


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_pub_revenue(date_str: str) -> dict:
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
                "revenue":     rev,
                "impressions": imps,
                "wins":        wins,
                "bids":        bids,
                "ecpm":        ecpm,
                "win_rate":    pct(wins, bids),
            }
        return out
    except Exception as e:
        print(f"      [Pub revenue fetch error: {e}]")
        return {}


def get_pub_demand_breakdown(pub_name: str, date_str: str) -> list:
    """Get which demand partners are buying this publisher today."""
    try:
        rows = fetch("PUBLISHER,DEMAND_PARTNER_NAME",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS", "GROSS_ECPM"],
            date_str, date_str)
        out = []
        for r in rows:
            if r.get("PUBLISHER_NAME", "").strip() != pub_name: continue
            dem  = r.get("DEMAND_PARTNER_NAME", "").strip()
            rev  = sf(r.get("GROSS_REVENUE", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = sf(r.get("GROSS_ECPM", 0)) or (rev / imps * 1000 if imps > 0 else 0)
            if dem and rev > 0:
                out.append({
                    "demand_partner": dem,
                    "revenue":        rev,
                    "impressions":    imps,
                    "ecpm":           ecpm,
                    "win_rate":       pct(wins, bids),
                })
        return sorted(out, key=lambda x: x["revenue"], reverse=True)[:5]
    except Exception as e:
        print(f"      [Pub demand breakdown error for {pub_name}: {e}]")
        return []


def diagnose_drop(today: dict, yesterday: dict, demand_today: list) -> str:
    """Classify whether the drop is supply-side or demand-side."""
    imps_today = today.get("impressions", 0)
    imps_prev  = yesterday.get("impressions", 0)
    ecpm_today = today.get("ecpm", 0)
    ecpm_prev  = yesterday.get("ecpm", 0)
    wr_today   = today.get("win_rate", 0)
    wr_prev    = yesterday.get("win_rate", 0)

    if imps_prev > 0:
        imps_change = (imps_today - imps_prev) / imps_prev * 100
        if imps_change < -30:
            return f"Supply-side: impressions down {abs(imps_change):.0f}% — check publisher traffic"

    if ecpm_prev > 0:
        ecpm_change = (ecpm_today - ecpm_prev) / ecpm_prev * 100
        if ecpm_change < -15:
            return f"Demand-side: eCPM dropped {abs(ecpm_change):.0f}% — check floor vs bid alignment"

    if wr_prev > 0:
        wr_change = (wr_today - wr_prev) / wr_prev * 100
        if wr_change < -20:
            return f"Win rate fell {abs(wr_change):.0f}% — floors may be too high for current demand"

    if not demand_today:
        return "Demand partners not responding — check endpoint connectivity"

    return "Mixed signals — review supply volume and demand eCPM together"


# ── Slack builder ─────────────────────────────────────────────────────────────

def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


def build_slack_payload(drops: list, today_str: str, yesterday_str: str) -> dict:
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "📉 Publisher Revenue Cliff Alert", "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{len(drops)} publisher{'s' if len(drops) > 1 else ''} "
                    f"down >{DROP_THRESHOLD_PCT}% vs yesterday*\n"
                    f"Comparing {today_str} vs {yesterday_str}. "
                    f"Supply and demand context shown below."
                )
            }
        },
        {"type": "divider"},
    ]

    for drop in drops:
        change_pct = drop["change_pct"]
        rev_today  = drop["rev_today"]
        rev_prev   = drop["rev_prev"]
        demand     = drop["demand_today"]
        diagnosis  = drop["diagnosis"]

        # Demand partner summary
        if demand:
            dem_parts = [
                f"{d['demand_partner'][:25]} {fmt_usd(d['revenue'])} "
                f"(eCPM ${d['ecpm']:.3f})"
                for d in demand[:3]
            ]
            dem_str = "\n".join(f"    • {p}" for p in dem_parts)
        else:
            dem_str = "    • No demand data available"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"📉 *{drop['publisher']}*\n"
                    f"  Revenue: {fmt_usd(rev_today)} ← was {fmt_usd(rev_prev)} "
                    f"(`{change_pct:.0f}%`)\n"
                    f"  eCPM: ${drop['ecpm_today']:.3f} | "
                    f"Win rate: {drop['wr_today']:.1f}% | "
                    f"Imps: {drop['imps_today']/1000:.0f}K\n"
                    f"  🎯 Active demand:\n{dem_str}\n"
                    f"  💡 _{diagnosis}_"
                )
            }
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                      "text": (
                          f"Threshold: >{DROP_THRESHOLD_PCT}% drop | "
                          f"Min revenue: {fmt_usd(MIN_YESTERDAY_REVENUE)}/day | "
                          f"PGAM Alerts — runs every 4h"
                      )}]
    })

    return {"blocks": blocks}


# ── Main runner ───────────────────────────────────────────────────────────────

def run_publisher_cliff_agent():
    today     = date.today()
    today_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_str  = (today - timedelta(days=2)).strftime("%Y-%m-%d")

    print(f"Fetching publisher revenue: {today_str} vs {prev_str}...")

    today_pubs = get_pub_revenue(today_str)
    prev_pubs  = get_pub_revenue(prev_str)

    state = load_state()
    drops = []

    for pub, today_data in today_pubs.items():
        prev_data = prev_pubs.get(pub, {"revenue": 0, "impressions": 0,
                                        "wins": 0, "bids": 0, "ecpm": 0, "win_rate": 0})
        rev_prev  = prev_data["revenue"]
        rev_today = today_data["revenue"]

        if rev_prev < MIN_YESTERDAY_REVENUE:
            continue

        if rev_prev == 0:
            continue

        change_pct = (rev_today - rev_prev) / rev_prev * 100

        if change_pct > -DROP_THRESHOLD_PCT:
            continue

        if already_alerted_today(state, pub):
            print(f"  Skipping {pub} — already alerted today")
            continue

        print(f"  Cliff detected: {pub} {change_pct:.0f}% — fetching context...")

        demand_today = get_pub_demand_breakdown(pub, today_str)
        diagnosis    = diagnose_drop(today_data, prev_data, demand_today)

        drops.append({
            "publisher":  pub,
            "rev_today":  rev_today,
            "rev_prev":   rev_prev,
            "change_pct": change_pct,
            "ecpm_today": today_data["ecpm"],
            "wr_today":   today_data["win_rate"],
            "imps_today": today_data["impressions"],
            "demand_today": demand_today,
            "diagnosis":    diagnosis,
        })
        mark_alerted(state, pub)

    drops = sorted(drops, key=lambda x: x["change_pct"])[:MAX_PUBLISHERS]

    print(f"  Publishers with cliff: {len(drops)}")

    if not drops:
        print("  No publisher cliffs detected.")
        save_state(state)
        return

    payload = build_slack_payload(drops, today_str, prev_str)

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    save_state(state)
    print(f"Publisher cliff alert sent ✅ ({len(drops)} publishers)")


if __name__ == "__main__":
    run_publisher_cliff_agent()
