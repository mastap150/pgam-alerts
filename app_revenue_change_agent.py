"""
App Revenue Change Intelligence — PGAM Alerts (Slack)
======================================================
When a bundle/app has a significant revenue change day-on-day,
this agent enriches it with:
  - Supply origin: which publisher and SSP is supplying it
  - Demand association: which demand partners are buying it
  - Root cause signal: is the change supply-side or demand-side?

Schedule: Daily — run after the morning email report (e.g. 6 AM ET)
Cron: 0 11 * * *  (6 AM ET = 11 AM UTC)
"""

import os
import requests
from datetime import date, timedelta
from api import fetch, sf, pct

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")

# ── Config ────────────────────────────────────────────────────────────────────
MIN_YESTERDAY_REVENUE  = 50     # only track bundles that earned >$50 yesterday
MIN_CHANGE_PCT         = 30     # only surface changes >30% either direction
TOP_DEMAND_PARTNERS    = 3      # demand partners to show per bundle
TOP_SUPPLY_PARTNERS    = 2      # supply/publisher rows to show per bundle
MAX_BUNDLES_PER_DIR    = 5      # max rising / falling bundles to show


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_bundle_revenue(date_str: str) -> dict:
    """Returns {bundle: {revenue, impressions, wins, bids}} for a given date."""
    try:
        rows = fetch("BUNDLE",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS"],
            date_str, date_str)
        return {
            r.get("BUNDLE", "").strip(): {
                "revenue":     sf(r.get("GROSS_REVENUE", 0)),
                "impressions": sf(r.get("IMPRESSIONS", 0)),
                "wins":        sf(r.get("WINS", 0)),
                "bids":        sf(r.get("BIDS", 0)),
            }
            for r in rows if r.get("BUNDLE", "").strip()
        }
    except Exception as e:
        print(f"      [Bundle revenue fetch error: {e}]")
        return {}


def get_bundle_supply_context(bundle: str, date_str: str) -> list:
    """
    For a given bundle, returns which publishers/SSPs are supplying it.
    Returns list of {publisher, revenue, impressions, ecpm}
    """
    try:
        rows = fetch("BUNDLE,PUBLISHER",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS", "GROSS_ECPM"],
            date_str, date_str)

        results = []
        for r in rows:
            if r.get("BUNDLE", "").strip() != bundle:
                continue
            pub  = r.get("PUBLISHER_NAME", "").strip()
            rev  = sf(r.get("GROSS_REVENUE", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = sf(r.get("GROSS_ECPM", 0)) or (rev / imps * 1000 if imps > 0 else 0)
            wr   = pct(wins, bids)
            if pub and rev > 0:
                results.append({
                    "publisher":   pub,
                    "revenue":     rev,
                    "impressions": imps,
                    "ecpm":        ecpm,
                    "win_rate":    wr,
                })
        return sorted(results, key=lambda x: x["revenue"], reverse=True)[:TOP_SUPPLY_PARTNERS]
    except Exception as e:
        print(f"      [Supply context fetch error for {bundle}: {e}]")
        return []


def get_bundle_demand_context(bundle: str, date_str: str, prev_date_str: str) -> list:
    """
    For a given bundle, returns which demand partners are buying it today vs yesterday.
    Returns list of {demand_partner, revenue_today, revenue_prev, change_pct, ecpm, win_rate}
    """
    try:
        # Today
        today_rows = fetch("BUNDLE,DEMAND_PARTNER_NAME",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS", "GROSS_ECPM"],
            date_str, date_str)

        # Yesterday (for comparison)
        prev_rows = fetch("BUNDLE,DEMAND_PARTNER_NAME",
            ["GROSS_REVENUE"],
            prev_date_str, prev_date_str)

        prev_map = {}
        for r in prev_rows:
            if r.get("BUNDLE", "").strip() == bundle:
                dem = r.get("DEMAND_PARTNER_NAME", "").strip()
                if dem:
                    prev_map[dem] = sf(r.get("GROSS_REVENUE", 0))

        results = []
        for r in today_rows:
            if r.get("BUNDLE", "").strip() != bundle:
                continue
            dem  = r.get("DEMAND_PARTNER_NAME", "").strip()
            rev  = sf(r.get("GROSS_REVENUE", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = sf(r.get("GROSS_ECPM", 0)) or (rev / imps * 1000 if imps > 0 else 0)
            wr   = pct(wins, bids)

            if dem and rev > 0:
                prev_rev   = prev_map.get(dem, 0)
                change_pct = pct(rev - prev_rev, prev_rev) if prev_rev > 0 else None
                results.append({
                    "demand_partner": dem,
                    "revenue_today":  rev,
                    "revenue_prev":   prev_rev,
                    "change_pct":     change_pct,
                    "ecpm":           ecpm,
                    "win_rate":       wr,
                })

        return sorted(results, key=lambda x: x["revenue_today"], reverse=True)[:TOP_DEMAND_PARTNERS]
    except Exception as e:
        print(f"      [Demand context fetch error for {bundle}: {e}]")
        return []


# ── Root cause signal ─────────────────────────────────────────────────────────

def diagnose_change(bundle_today: dict, bundle_prev: dict,
                    demand_context: list) -> str:
    """
    Attempts to classify whether a revenue change is supply-side or demand-side.
    Returns a short diagnostic string.
    """
    imps_today = bundle_today.get("impressions", 0)
    imps_prev  = bundle_prev.get("impressions", 0)
    wins_today = bundle_today.get("wins", 0)
    wins_prev  = bundle_prev.get("wins", 0)

    imps_change = pct(imps_today - imps_prev, imps_prev) if imps_prev > 0 else None
    wins_change = pct(wins_today - wins_prev, wins_prev) if wins_prev > 0 else None

    # Check if demand partners are driving the change
    demand_driven = any(
        r["change_pct"] is not None and abs(r["change_pct"]) > 30
        for r in demand_context
    )

    if imps_change is not None and abs(imps_change) > 30:
        direction = "up" if imps_change > 0 else "down"
        return f"Supply-side: impression volume {direction} {abs(imps_change):.0f}%"
    elif demand_driven:
        movers = [
            f"{r['demand_partner']} {'+' if (r['change_pct'] or 0) > 0 else ''}{r['change_pct']:.0f}%"
            for r in demand_context
            if r["change_pct"] is not None and abs(r["change_pct"]) > 30
        ]
        return f"Demand-side: {', '.join(movers[:2])}"
    elif wins_change is not None and abs(wins_change) > 30:
        direction = "up" if wins_change > 0 else "down"
        return f"Win rate shift: wins {direction} {abs(wins_change):.0f}% — check floor/bid alignment"
    else:
        return "Mixed signals — review supply volume and demand eCPM"


# ── Slack formatter ───────────────────────────────────────────────────────────

def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


def fmt_pct_str(v: float | None) -> str:
    if v is None: return "new"
    return f"{'+' if v >= 0 else ''}{v:.0f}%"


def build_bundle_block(bundle: str, rev_today: float, rev_prev: float,
                       change_pct: float, supply: list, demand: list,
                       diagnosis: str, direction: str) -> dict:
    icon    = "📈" if direction == "up" else "📉"
    rev_str = f"{fmt_usd(rev_today)} ({fmt_pct_str(change_pct)} vs yesterday)"

    lines = [f"{icon} *{bundle}*  {rev_str}"]

    # Supply context
    if supply:
        sup_parts = [
            f"{s['publisher']} ({fmt_usd(s['revenue'])} | eCPM ${s['ecpm']:.2f})"
            for s in supply
        ]
        lines.append(f"  📡 Supply: {' · '.join(sup_parts)}")
    else:
        lines.append("  📡 Supply: data unavailable")

    # Demand context
    if demand:
        dem_parts = [
            f"{d['demand_partner']} {fmt_usd(d['revenue_today'])} ({fmt_pct_str(d['change_pct'])})"
            for d in demand
        ]
        lines.append(f"  🎯 Demand: {' · '.join(dem_parts)}")
    else:
        lines.append("  🎯 Demand: data unavailable")

    # Root cause
    lines.append(f"  💡 _{diagnosis}_")

    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(lines)}
    }


def build_slack_payload(rising: list, falling: list,
                        today_str: str, prev_str: str) -> dict:
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "📊 App Revenue Changes — Supply & Demand Context",
                     "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{today_str} vs {prev_str}*  —  "
                    f"Apps with ≥{MIN_CHANGE_PCT}% revenue change, "
                    f"showing supply origin and demand association."
                )
            }
        },
        {"type": "divider"},
    ]

    if rising:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*📈 Rising Apps ({len(rising)})*"}
        })
        for item in rising:
            blocks.append(build_bundle_block(**item, direction="up"))
            blocks.append({"type": "divider"})

    if falling:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*📉 Falling Apps ({len(falling)})*"}
        })
        for item in falling:
            blocks.append(build_bundle_block(**item, direction="down"))
            blocks.append({"type": "divider"})

    if not rising and not falling:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"✅ No significant app revenue changes today (threshold: ≥{MIN_CHANGE_PCT}% + ≥{fmt_usd(MIN_YESTERDAY_REVENUE)})"}
        })

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"Min revenue: {fmt_usd(MIN_YESTERDAY_REVENUE)}/day  |  "
                f"Min change: {MIN_CHANGE_PCT}%  |  PGAM Alerts"
            )
        }]
    })

    return {"blocks": blocks}


# ── Core runner ───────────────────────────────────────────────────────────────

def run_app_revenue_change_alert():
    today     = date.today()
    yesterday = today - timedelta(days=1)
    prev_day  = today - timedelta(days=2)

    today_str = yesterday.strftime("%Y-%m-%d")   # "yesterday" = most recent full day
    prev_str  = prev_day.strftime("%Y-%m-%d")    # compare against 2 days ago

    print(f"\nFetching bundle revenue: {today_str} vs {prev_str}...")

    today_bundles = get_bundle_revenue(today_str)
    prev_bundles  = get_bundle_revenue(prev_str)

    print(f"  Today: {len(today_bundles)} bundles | Prev: {len(prev_bundles)} bundles")

    rising  = []
    falling = []

    # Find significant movers
    for bundle, today_data in today_bundles.items():
        rev_today = today_data["revenue"]
        prev_data = prev_bundles.get(bundle, {"revenue": 0, "impressions": 0, "wins": 0, "bids": 0})
        rev_prev  = prev_data["revenue"]

        if rev_prev < MIN_YESTERDAY_REVENUE and rev_today < MIN_YESTERDAY_REVENUE:
            continue

        if rev_prev == 0:
            change_pct = 100.0 if rev_today > 0 else 0.0
        else:
            change_pct = (rev_today - rev_prev) / rev_prev * 100

        if abs(change_pct) < MIN_CHANGE_PCT:
            continue

        print(f"  Fetching context for: {bundle} ({change_pct:+.0f}%)...")

        supply  = get_bundle_supply_context(bundle, today_str)
        demand  = get_bundle_demand_context(bundle, today_str, prev_str)
        diagnosis = diagnose_change(today_data, prev_data, demand)

        entry = {
            "bundle":     bundle,
            "rev_today":  rev_today,
            "rev_prev":   rev_prev,
            "change_pct": change_pct,
            "supply":     supply,
            "demand":     demand,
            "diagnosis":  diagnosis,
        }

        if change_pct > 0:
            rising.append(entry)
        else:
            falling.append(entry)

    # Sort and cap
    rising  = sorted(rising,  key=lambda x: x["change_pct"],  reverse=True)[:MAX_BUNDLES_PER_DIR]
    falling = sorted(falling, key=lambda x: x["change_pct"])[:MAX_BUNDLES_PER_DIR]

    print(f"\n  Rising: {len(rising)} | Falling: {len(falling)}")

    payload = build_slack_payload(rising, falling, today_str, prev_str)

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK env var not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    print("App revenue change alert sent ✅")


if __name__ == "__main__":
    run_app_revenue_change_alert()
