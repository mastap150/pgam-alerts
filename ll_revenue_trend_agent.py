"""
LL Revenue Trend Agent — PGAM Intelligence (Hourly Slack)
==========================================================
Mirrors the Teqblaze revenue trend agent but for the LL platform
(stats.ortb.net via fetch() from api.py).

Posts hourly to Slack showing:
  - Today's gross revenue vs same hour yesterday
  - Pacing % toward daily target
  - Margin (gross rev - pub payout) / gross rev
  - Traffic mix by top publishers
  - Hour-on-hour comparison

After 8pm ET the LL API may reset — shows yesterday's final numbers.

Runs hourly. Wired into run.py as a standalone Slack-only agent
(does not add a section to the email — email already has daily summary).

Schedule: Add to a new Render cron job OR call from run.py with --slack flag.
Simplest: new cron job in pgam-alerts pointing to this file.
"""

import os
import requests
from datetime import datetime, timedelta
from api import fetch, sf, pct

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")

# ── Config ────────────────────────────────────────────────────────────────────
DAILY_REVENUE_TARGET = 3500    # $ gross revenue daily target — adjust as needed
MARGIN_FLOOR         = 20.0    # % — alert if margin drops below this
HOURLY_DROP_THRESHOLD = 30     # % — alert if this hour is >30% below same hour yesterday


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_et_hour() -> int:
    try:
        import pytz
        et = pytz.timezone("America/New_York")
        return datetime.now(et).hour
    except ImportError:
        month  = datetime.utcnow().month
        offset = -4 if 3 <= month <= 11 else -5
        return (datetime.utcnow().hour + offset) % 24


def get_et_now() -> datetime:
    try:
        import pytz
        et = pytz.timezone("America/New_York")
        return datetime.now(et)
    except ImportError:
        month  = datetime.utcnow().month
        offset = -4 if 3 <= month <= 11 else -5
        return datetime.utcnow() + timedelta(hours=offset)


def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.2f}"


def fmt_pct(v: float) -> str:
    return f"{v:.1f}%"


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_daily_summary(date_str: str) -> dict:
    """Total gross revenue, pub payout, impressions for a date."""
    try:
        rows = fetch("DATE",
            ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"],
            date_str, date_str)
        r = rows[0] if rows else {}
        rev  = sf(r.get("GROSS_REVENUE", 0))
        pay  = sf(r.get("PUB_PAYOUT", 0))
        imps = sf(r.get("IMPRESSIONS", 0))
        wins = sf(r.get("WINS", 0))
        bids = sf(r.get("BIDS", 0))
        return {
            "revenue":    rev,
            "payout":     pay,
            "margin":     pct(rev - pay, rev),
            "net":        rev - pay,
            "impressions": imps,
            "win_rate":   pct(wins, bids),
            "ecpm":       rev / imps * 1000 if imps > 0 else 0,
        }
    except Exception as e:
        print(f"      [LL summary fetch error: {e}]")
        return {}


def get_hourly_breakdown(date_str: str) -> list:
    """Revenue by hour for a given date."""
    try:
        rows = fetch("HOUR",
            ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"],
            date_str, date_str)
        return sorted(rows, key=lambda r: int(sf(r.get("HOUR", 0))))
    except Exception as e:
        print(f"      [LL hourly fetch error: {e}]")
        return []


def get_top_publishers(date_str: str, top_n: int = 5) -> list:
    """Top publishers by revenue today."""
    try:
        rows = fetch("PUBLISHER",
            ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"],
            date_str, date_str)
        out = []
        for r in rows:
            pub  = r.get("PUBLISHER_NAME", "").strip()
            rev  = sf(r.get("GROSS_REVENUE", 0))
            pay  = sf(r.get("PUB_PAYOUT", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = rev / imps * 1000 if imps > 0 else 0
            if pub and rev > 0:
                out.append({
                    "publisher": pub,
                    "revenue":   rev,
                    "margin":    pct(rev - pay, rev),
                    "ecpm":      ecpm,
                    "win_rate":  pct(wins, bids),
                })
        return sorted(out, key=lambda x: x["revenue"], reverse=True)[:top_n]
    except Exception as e:
        print(f"      [LL publisher fetch error: {e}]")
        return []


# ── Slack builder ─────────────────────────────────────────────────────────────

def build_slack_payload(today: dict, yesterday: dict, hourly_today: list,
                        hourly_yest: list, top_pubs: list,
                        hour_et: int, date_str: str, yest_str: str) -> dict:

    # After 8pm ET LL data may reset — show yesterday
    api_reset = hour_et >= 20

    if api_reset:
        display = yesterday
        label   = f"Yesterday final — {yest_str}"
        note    = "\n_⚠️ Showing yesterday's completed day (API resets 8pm ET)_"
        expected_pacing = 1.0
    else:
        display = today
        label   = f"Today — {date_str}"
        note    = ""
        expected_pacing = max(hour_et, 1) / 24

    rev    = display.get("revenue", 0)
    pay    = display.get("payout", 0)
    net    = display.get("net", 0)
    margin = display.get("margin", 0)
    ecpm   = display.get("ecpm", 0)
    wr     = display.get("win_rate", 0)

    pacing     = rev / DAILY_REVENUE_TARGET if DAILY_REVENUE_TARGET > 0 else 0
    yest_rev   = yesterday.get("revenue", 0)
    dod_pct    = (rev - yest_rev) / yest_rev * 100 if yest_rev > 0 else 0

    pace_emoji   = "✅" if pacing >= expected_pacing * 0.9 else ("⚠️" if pacing >= expected_pacing * 0.75 else "🚨")
    margin_emoji = "✅" if margin >= MARGIN_FLOOR else "🚨"
    dod_arrow    = "▲" if dod_pct >= 0 else "▼"

    # Hourly comparison
    hourly_line = ""
    if not api_reset and hourly_today:
        this_h_rev  = sf(hourly_today[-1].get("GROSS_REVENUE", 0))
        yest_h_idx  = min(max(hour_et - 1, 0), len(hourly_yest) - 1)
        yest_h_rev  = sf(hourly_yest[yest_h_idx].get("GROSS_REVENUE", 0)) if hourly_yest else 0
        h_arrow     = "🟢 ↑" if this_h_rev >= yest_h_rev else "🔴 ↓"
        h_pct       = (this_h_rev - yest_h_rev) / yest_h_rev * 100 if yest_h_rev > 0 else 0
        hourly_line = (
            f"\n{h_arrow} *This hour:* {fmt_usd(this_h_rev)} "
            f"vs {fmt_usd(yest_h_rev)} same hour yest "
            f"({'+' if h_pct >= 0 else ''}{h_pct:.1f}%)"
        )

    # Top publishers
    pub_lines = ""
    if top_pubs:
        pub_lines = "\n*Top Publishers:*\n" + "\n".join([
            f"  • *{p['publisher'][:30]}*  {fmt_usd(p['revenue'])} | "
            f"margin {fmt_pct(p['margin'])} | eCPM ${p['ecpm']:.3f}"
            for p in top_pubs
        ])

    et_now = get_et_now()
    message = (
        f"*LL Revenue — {et_now.strftime('%b %d, %I:%M %p')} ET* ({label}){note}\n\n"
        f"{pace_emoji} *Gross Revenue:* {fmt_usd(rev)} | "
        f"Pacing {fmt_pct(pacing * 100)} (expected {fmt_pct(expected_pacing * 100)})\n"
        f"💵 *Pub Payout:* {fmt_usd(pay)} | *Net:* {fmt_usd(net)}\n"
        f"{margin_emoji} *Margin:* {fmt_pct(margin)} | "
        f"eCPM: ${ecpm:.3f} | Win Rate: {fmt_pct(wr)}\n"
        f"📈 *vs Yesterday:* {dod_arrow} {abs(dod_pct):.1f}%"
        f"{hourly_line}"
        f"{pub_lines}"
    )

    return {
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            {"type": "context", "elements": [{"type": "mrkdwn",
                "text": "LL Revenue Trend | Runs hourly"}]}
        ]
    }


# ── Main runner ───────────────────────────────────────────────────────────────

def run_ll_revenue_trend():
    et_now   = get_et_now()
    hour_et  = et_now.hour
    today_dt = et_now.date() if hour_et >= 0 else et_now.date() - timedelta(days=1)
    yest_dt  = today_dt - timedelta(days=1)

    date_str = today_dt.strftime("%Y-%m-%d")
    yest_str = yest_dt.strftime("%Y-%m-%d")

    print(f"Fetching LL revenue data for {date_str} (hour ET: {hour_et})...")

    today     = get_daily_summary(date_str)
    yesterday = get_daily_summary(yest_str)
    hourly_today = get_hourly_breakdown(date_str)
    hourly_yest  = get_hourly_breakdown(yest_str)
    top_pubs     = get_top_publishers(date_str)

    print(f"  Revenue today: {fmt_usd(today.get('revenue', 0))}")
    print(f"  Revenue yest:  {fmt_usd(yesterday.get('revenue', 0))}")
    print(f"  Top publishers: {len(top_pubs)}")

    payload = build_slack_payload(
        today, yesterday, hourly_today, hourly_yest,
        top_pubs, hour_et, date_str, yest_str
    )

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    print("LL revenue trend sent ✅")


if __name__ == "__main__":
    run_ll_revenue_trend()
