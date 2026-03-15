"""
Agent: Weekend vs Weekday Baseline Monitor
===========================================
Fixes false anomaly alerts caused by comparing Saturday revenue
against a rolling 7-day average that includes weekdays.

Compares today against the same day-of-week baseline:
  - Monday vs avg of last 4 Mondays
  - Saturday vs avg of last 4 Saturdays
  etc.

Fires when today is >30% below its day-of-week baseline.
Also shows which day-of-week performs best/worst so the team
can set realistic daily expectations.

Schedule: Daily at 9 AM ET
Cron: 0 13 * * *  (9 AM ET = 13 UTC)
"""

import os
import json
import requests
from datetime import date, timedelta
from collections import defaultdict
from api import fetch, sf, pct

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")
STATE_FILE    = "/tmp/dow_baseline_state.json"

# ── Config ────────────────────────────────────────────────────────────────────
LOOKBACK_WEEKS    = 4       # how many prior same-day-of-week to average
DROP_THRESHOLD    = 25.0    # % below DOW baseline to alert
MIN_BASELINE_REV  = 500     # $ — only alert if DOW baseline is meaningful
COOLDOWN_DAYS     = 1       # only fire once per day

DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday",
             "Friday", "Saturday", "Sunday"]


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


def already_alerted_today(state: dict, key: str) -> bool:
    today = date.today().strftime("%Y-%m-%d")
    return state.get(f"{today}_{key}", False)


def mark_alerted(state: dict, key: str):
    today = date.today().strftime("%Y-%m-%d")
    # Prune old keys
    for k in list(state.keys()):
        if k[:10] < today:
            del state[k]
    state[f"{today}_{key}"] = True


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_daily_revenue(date_str: str) -> dict:
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
            "revenue":   rev,
            "payout":    pay,
            "margin":    pct(rev - pay, rev),
            "ecpm":      rev / imps * 1000 if imps > 0 else 0,
            "win_rate":  pct(wins, bids),
        }
    except Exception as e:
        print(f"      [Daily revenue fetch error for {date_str}: {e}]")
        return {}


def build_dow_baselines(today: date) -> dict:
    """
    For each day of week, compute average revenue over last LOOKBACK_WEEKS.
    Returns {0: avg_monday, 1: avg_tuesday, ... 6: avg_sunday}
    """
    baselines = defaultdict(list)

    for week_back in range(1, LOOKBACK_WEEKS + 1):
        # Go back week_back * 7 days to get same day of week
        for dow in range(7):
            # Find the date that is `week_back` weeks ago for this DOW
            days_back = week_back * 7 + (today.weekday() - dow) % 7
            past_date = today - timedelta(days=days_back)
            # Only use if it falls on the right day of week
            if past_date.weekday() == dow:
                data = get_daily_revenue(past_date.strftime("%Y-%m-%d"))
                rev  = data.get("revenue", 0)
                if rev > 0:
                    baselines[dow].append(rev)

    return {
        dow: sum(revs) / len(revs)
        for dow, revs in baselines.items()
        if revs
    }


def build_full_dow_profile(today: date) -> dict:
    """Build a complete DOW profile — avg revenue per day of week."""
    profile = {}
    for dow in range(7):
        revs = []
        for week_back in range(1, LOOKBACK_WEEKS + 1):
            days_back = week_back * 7
            # Get same DOW from week_back weeks ago
            target = today - timedelta(days=days_back)
            # Adjust to hit the right DOW
            diff = (dow - target.weekday()) % 7
            target = target + timedelta(days=diff) - timedelta(days=7)
            data = get_daily_revenue(target.strftime("%Y-%m-%d"))
            rev  = data.get("revenue", 0)
            if rev > 0:
                revs.append(rev)
        if revs:
            profile[dow] = sum(revs) / len(revs)
    return profile


# ── Slack builder ─────────────────────────────────────────────────────────────

def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


def build_slack_payload(today_data: dict, today_dow: int, dow_baseline: float,
                        dow_profile: dict, drop_pct: float,
                        date_str: str, is_alert: bool) -> dict:

    dow_name = DOW_NAMES[today_dow]
    rev_today = today_data.get("revenue", 0)

    # DOW profile table
    profile_lines = []
    best_dow  = max(dow_profile, key=dow_profile.get) if dow_profile else None
    worst_dow = min(dow_profile, key=dow_profile.get) if dow_profile else None
    for dow in range(7):
        if dow not in dow_profile:
            continue
        avg   = dow_profile[dow]
        name  = DOW_NAMES[dow]
        star  = " 🏆" if dow == best_dow else " 📉" if dow == worst_dow else ""
        marker = " ← today" if dow == today_dow else ""
        profile_lines.append(
            f"  `{name[:3]}` {fmt_usd(avg)}{star}{marker}"
        )

    if is_alert:
        header_text = f"⚠️ Revenue Below {dow_name} Baseline"
        summary = (
            f"*{date_str} ({dow_name})*\n"
            f"Today: *{fmt_usd(rev_today)}* | "
            f"{dow_name} avg: *{fmt_usd(dow_baseline)}* | "
            f"Gap: *{drop_pct:.1f}% below*\n"
            f"Margin: {today_data.get('margin', 0):.1f}% | "
            f"eCPM: ${today_data.get('ecpm', 0):.3f} | "
            f"Win rate: {today_data.get('win_rate', 0):.1f}%\n\n"
            f"_This is being compared to {LOOKBACK_WEEKS} prior {dow_name}s, "
            f"not a rolling 7-day average._"
        )
    else:
        header_text = f"📊 Daily Revenue vs {dow_name} Baseline"
        summary = (
            f"*{date_str} ({dow_name})*\n"
            f"Today: *{fmt_usd(rev_today)}* | "
            f"{dow_name} avg: *{fmt_usd(dow_baseline)}* | "
            f"On track ✅\n"
            f"Margin: {today_data.get('margin', 0):.1f}% | "
            f"eCPM: ${today_data.get('ecpm', 0):.3f}"
        )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text, "emoji": True}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary}
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{LOOKBACK_WEEKS}-Week DOW Averages:*\n" +
                        "\n".join(profile_lines)
            }
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                          "text": (
                              f"Threshold: >{DROP_THRESHOLD}% below DOW avg | "
                              f"Baseline: {LOOKBACK_WEEKS} prior same-day weeks | "
                              f"PGAM Alerts — daily 9 AM ET"
                          )}]
        }
    ]

    return {"blocks": blocks}


# ── Main runner ───────────────────────────────────────────────────────────────

def run_weekend_baseline_agent():
    today     = date.today()
    yesterday = today - timedelta(days=1)
    date_str  = yesterday.strftime("%Y-%m-%d")
    dow       = yesterday.weekday()
    dow_name  = DOW_NAMES[dow]

    print(f"Checking DOW baseline for {date_str} ({dow_name})...")

    today_data = get_daily_revenue(date_str)
    rev_today  = today_data.get("revenue", 0)

    print(f"  Revenue: ${rev_today:,.0f}")

    # Build DOW baselines
    print(f"  Building {LOOKBACK_WEEKS}-week DOW profile...")
    dow_profile  = build_full_dow_profile(yesterday)
    dow_baseline = dow_profile.get(dow, 0)

    print(f"  {dow_name} baseline: ${dow_baseline:,.0f}")

    if dow_baseline == 0 or dow_baseline < MIN_BASELINE_REV:
        print(f"  Baseline too low to compare — skipping alert.")
        return

    drop_pct = (dow_baseline - rev_today) / dow_baseline * 100

    print(f"  vs baseline: {drop_pct:+.1f}%")

    state     = load_state()
    is_alert  = drop_pct > DROP_THRESHOLD

    if is_alert and already_alerted_today(state, "dow_drop"):
        print("  Already alerted today — skipping.")
        return

    if is_alert:
        mark_alerted(state, "dow_drop")
        save_state(state)

    payload = build_slack_payload(
        today_data, dow, dow_baseline, dow_profile,
        drop_pct, date_str, is_alert
    )

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    action = "Alert sent" if is_alert else "Daily summary sent"
    print(f"{action} ✅")


if __name__ == "__main__":
    run_weekend_baseline_agent()
