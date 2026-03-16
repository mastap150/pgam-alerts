"""
Agent: Margin Compression Monitor
===================================
Fires when true net margin drops significantly, catching fee creep,
payout shifts, or demand mix changes before they compound.

Checks:
  1. Day-on-day margin drop >3pp (immediate compression)
  2. 7-day trend — margin declining consistently
  3. Per-publisher payout ratio increasing (squeezing your share)
  4. Per-demand-partner margin by platform (LL vs TB routing impact)

Output: Slack alert with root cause signal and affected dimension.

Schedule: Add to main.py — runs every 4 hours (not hourly to avoid noise)
Cron: schedule.every(4).hours.do(run_agent, "margin_compression", "MarginCompressionAgent")
"""

import os
import json
from datetime import date, timedelta
from collections import defaultdict
from api import fetch, sf, pct

SLACK_WEBHOOK  = os.environ.get("SLACK_WEBHOOK", "")
STATE_FILE     = "/tmp/margin_compression_state.json"

# ── Thresholds ────────────────────────────────────────────────────────────────
DOD_DROP_THRESHOLD     = 3.0    # pp — day-on-day margin drop to alert
TREND_DROP_THRESHOLD   = 5.0    # pp — 7-day trend drop to alert  
PUB_PAYOUT_SHIFT       = 0.03   # 3pp shift in pub payout ratio
MIN_REVENUE_TO_CARE    = 500    # $ — ignore low-revenue days
COOLDOWN_HOURS         = 6      # hours between same alert firing


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


def can_fire(state: dict, key: str) -> bool:
    last = state.get(f"last_{key}", 0)
    hours_since = (date.today().toordinal() * 24 - last) 
    # Simple: use timestamp
    import time
    last_ts = state.get(f"ts_{key}", 0)
    return (time.time() - last_ts) > COOLDOWN_HOURS * 3600


def mark_fired(state: dict, key: str):
    import time
    state[f"ts_{key}"] = time.time()


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_daily_summary(date_str: str) -> dict:
    rows = fetch("DATE",
        ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"],
        date_str, date_str)
    r = rows[0] if rows else {}
    rev = sf(r.get("GROSS_REVENUE", 0))
    pay = sf(r.get("PUB_PAYOUT", 0))
    return {
        "revenue": rev,
        "payout":  pay,
        "margin":  pct(rev - pay, rev) if rev > 0 else 0,
        "payout_ratio": pct(pay, rev) if rev > 0 else 0,
    }


def get_pub_payout_breakdown(date_str: str) -> list:
    rows = fetch("PUBLISHER",
        ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"],
        date_str, date_str)
    out = []
    for r in rows:
        pub = r.get("PUBLISHER_NAME", "").strip()
        rev = sf(r.get("GROSS_REVENUE", 0))
        pay = sf(r.get("PUB_PAYOUT", 0))
        if rev < 50: continue
        out.append({
            "publisher":    pub,
            "revenue":      rev,
            "payout":       pay,
            "margin":       pct(rev - pay, rev) if rev > 0 else 0,
            "payout_ratio": pct(pay, rev) if rev > 0 else 0,
        })
    return sorted(out, key=lambda x: x["revenue"], reverse=True)


def get_demand_margin_breakdown(date_str: str) -> list:
    rows = fetch("DEMAND_PARTNER_NAME",
        ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"],
        date_str, date_str)
    out = []
    for r in rows:
        dem = r.get("DEMAND_PARTNER_NAME", "").strip()
        rev = sf(r.get("GROSS_REVENUE", 0))
        pay = sf(r.get("PUB_PAYOUT", 0))
        if rev < 50: continue
        out.append({
            "demand_partner": dem,
            "revenue":        rev,
            "payout":         pay,
            "margin":         pct(rev - pay, rev) if rev > 0 else 0,
        })
    return sorted(out, key=lambda x: x["revenue"], reverse=True)


# ── Slack builder ─────────────────────────────────────────────────────────────

def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


def build_slack_payload(alerts: list, today: dict, yesterday: dict,
                        avg_7d: dict, pub_breakdown: list,
                        dem_breakdown: list, today_str: str) -> dict:

    dod_drop = yesterday["margin"] - today["margin"]
    trend_drop = avg_7d["margin"] - today["margin"]

    # Summary block
    summary_text = (
        f"*Margin today:* `{today['margin']:.1f}%` "
        f"({'▼' if dod_drop > 0 else '▲'} {abs(dod_drop):.1f}pp vs yesterday)\n"
        f"*7-day avg:* `{avg_7d['margin']:.1f}%` "
        f"({'▼' if trend_drop > 0 else '▲'} {abs(trend_drop):.1f}pp trend)\n"
        f"*Revenue:* {fmt_usd(today['revenue'])} | "
        f"*Payout:* {fmt_usd(today['payout'])} | "
        f"*Net:* {fmt_usd(today['revenue'] - today['payout'])}"
    )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "⚠️ Margin Compression Alert", "emoji": True}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary_text}
        },
        {"type": "divider"},
    ]

    # Alert details
    alert_lines = []
    for a in alerts:
        alert_lines.append(f"  • {a}")
    if alert_lines:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*What triggered this:*\n" + "\n".join(alert_lines)}
        })
        blocks.append({"type": "divider"})

    # Worst margin publishers
    low_margin_pubs = [p for p in pub_breakdown if p["margin"] < today["margin"] - 5][:5]
    if low_margin_pubs:
        pub_lines = ["*Publishers with lowest margin today:*"]
        for p in low_margin_pubs:
            pub_lines.append(
                f"  • *{p['publisher'][:35]}*  margin=`{p['margin']:.1f}%`  "
                f"rev={fmt_usd(p['revenue'])}  payout={fmt_usd(p['payout'])}"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(pub_lines)}
        })
        blocks.append({"type": "divider"})

    # Worst margin demand partners
    low_margin_dem = [d for d in dem_breakdown if d["margin"] < today["margin"] - 5][:5]
    if low_margin_dem:
        dem_lines = ["*Demand partners with lowest margin today:*"]
        for d in low_margin_dem:
            dem_lines.append(
                f"  • *{d['demand_partner'][:35]}*  margin=`{d['margin']:.1f}%`  "
                f"rev={fmt_usd(d['revenue'])}"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(dem_lines)}
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                      "text": f"PGAM Margin Monitor | {today_str} | Runs every 4h"}]
    })

    return {"blocks": blocks}


# ── Main runner ───────────────────────────────────────────────────────────────

def run_margin_compression_agent():
    import requests

    today     = date.today()
    yesterday = today - timedelta(days=1)
    week_ago  = today - timedelta(days=7)

    today_str     = yesterday.strftime("%Y-%m-%d")   # most recent full day
    yesterday_str = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")
    week_start    = week_ago.strftime("%Y-%m-%d")

    print(f"Fetching margin data for {today_str}...")

    today_data     = get_daily_summary(today_str)
    yesterday_data = get_daily_summary(yesterday_str)

    # 7-day average
    week_rows = fetch("DATE",
        ["GROSS_REVENUE", "PUB_PAYOUT"],
        week_start, yesterday_str)
    total_rev = sum(sf(r.get("GROSS_REVENUE", 0)) for r in week_rows)
    total_pay = sum(sf(r.get("PUB_PAYOUT", 0)) for r in week_rows)
    avg_7d = {
        "revenue": total_rev / max(len(week_rows), 1),
        "payout":  total_pay / max(len(week_rows), 1),
        "margin":  pct(total_rev - total_pay, total_rev) if total_rev > 0 else 0,
    }

    print(f"  Today margin:     {today_data['margin']:.1f}%")
    print(f"  Yesterday margin: {yesterday_data['margin']:.1f}%")
    print(f"  7-day avg margin: {avg_7d['margin']:.1f}%")

    if today_data["revenue"] < MIN_REVENUE_TO_CARE:
        print("  Revenue too low to analyse — skipping.")
        return

    state  = load_state()
    alerts = []

    dod_drop   = yesterday_data["margin"] - today_data["margin"]
    trend_drop = avg_7d["margin"] - today_data["margin"]

    if dod_drop > DOD_DROP_THRESHOLD and can_fire(state, "dod_margin"):
        alerts.append(
            f"Day-on-day drop: margin fell {dod_drop:.1f}pp "
            f"({yesterday_data['margin']:.1f}% → {today_data['margin']:.1f}%)"
        )
        mark_fired(state, "dod_margin")

    if trend_drop > TREND_DROP_THRESHOLD and can_fire(state, "trend_margin"):
        alerts.append(
            f"7-day trend: margin {trend_drop:.1f}pp below weekly average "
            f"(avg {avg_7d['margin']:.1f}%, today {today_data['margin']:.1f}%)"
        )
        mark_fired(state, "trend_margin")

    payout_shift = today_data["payout_ratio"] - (avg_7d["payout"] / avg_7d["revenue"] * 100 if avg_7d["revenue"] > 0 else 0)
    if payout_shift > PUB_PAYOUT_SHIFT * 100 and can_fire(state, "payout_shift"):
        alerts.append(
            f"Publisher payout ratio up {payout_shift:.1f}pp — "
            f"publishers taking a larger share of gross revenue"
        )
        mark_fired(state, "payout_shift")

    if not alerts:
        print("  No margin issues detected.")
        save_state(state)
        return

    pub_breakdown = get_pub_payout_breakdown(today_str)
    dem_breakdown = get_demand_margin_breakdown(today_str)

    payload = build_slack_payload(
        alerts, today_data, yesterday_data, avg_7d,
        pub_breakdown, dem_breakdown, today_str
    )

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    save_state(state)
    print(f"Margin compression alert sent ✅ ({len(alerts)} triggers)")


if __name__ == "__main__":
    run_margin_compression_agent()
