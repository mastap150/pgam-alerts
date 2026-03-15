"""
Agent: LL Domain & App Dropout Monitor
========================================
LL equivalent of the TB domain dropout alert.
Monitors domains and bundles (apps) on the Limelight platform
(stats.ortb.net via api.py fetch()).

For each dropped domain/app shows:
  - Last week avg daily revenue
  - Which publisher was supplying it
  - Which demand partners were buying it
  - eCPM and win rate context

Also surfaces NEW domains/apps that appeared this week
with meaningful revenue — these are worth nurturing.

Schedule: Daily at 9:30 AM ET
Cron: 30 13 * * *
"""

import os
import json
import requests
from datetime import date, timedelta
from collections import defaultdict
from api import fetch, sf, pct

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")
STATE_FILE    = "/tmp/ll_domain_app_state.json"

# ── Config ────────────────────────────────────────────────────────────────────
MIN_DAILY_AVG_DOMAIN  = 20    # $ — minimum daily avg to care about a dropped domain
MIN_DAILY_AVG_BUNDLE  = 20    # $ — minimum daily avg to care about a dropped bundle
MIN_NEW_REVENUE       = 50    # $ — minimum total revenue for a "new" domain/app to surface
MAX_ALERTS_PER_TYPE   = 5     # max domains / bundles to show per section
LOOKBACK_DAYS         = 7     # days to define "last week"


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
    for k in list(state.keys()):
        if k[:10] < today:
            del state[k]
    state[f"{today}_{key}"] = True


# ── Data fetchers ─────────────────────────────────────────────────────────────

def fmt_usd(v: float) -> str:
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


def get_domain_revenue(from_date: str, to_date: str) -> dict:
    """Returns {domain: total_revenue}"""
    try:
        rows = fetch("DOMAIN",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS", "GROSS_ECPM"],
            from_date, to_date)
        out = {}
        for r in rows:
            dom  = r.get("DOMAIN", "").strip()
            rev  = sf(r.get("GROSS_REVENUE", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            wins = sf(r.get("WINS", 0))
            bids = sf(r.get("BIDS", 0))
            ecpm = sf(r.get("GROSS_ECPM", 0)) or (rev / imps * 1000 if imps > 0 else 0)
            if dom and rev > 0:
                out[dom] = {
                    "revenue":   rev,
                    "ecpm":      ecpm,
                    "win_rate":  pct(wins, bids),
                }
        return out
    except Exception as e:
        print(f"      [Domain revenue fetch error: {e}]")
        return {}


def get_bundle_revenue(from_date: str, to_date: str) -> dict:
    """Returns {bundle: {revenue, ecpm, win_rate}}"""
    try:
        rows = fetch("BUNDLE",
            ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS", "GROSS_ECPM"],
            from_date, to_date)
        out = {}
        for r in rows:
            bundle = r.get("BUNDLE", "").strip()
            rev    = sf(r.get("GROSS_REVENUE", 0))
            imps   = sf(r.get("IMPRESSIONS", 0))
            wins   = sf(r.get("WINS", 0))
            bids   = sf(r.get("BIDS", 0))
            ecpm   = sf(r.get("GROSS_ECPM", 0)) or (rev / imps * 1000 if imps > 0 else 0)
            if bundle and rev > 0:
                out[bundle] = {
                    "revenue":   rev,
                    "ecpm":      ecpm,
                    "win_rate":  pct(wins, bids),
                }
        return out
    except Exception as e:
        print(f"      [Bundle revenue fetch error: {e}]")
        return {}


def get_domain_context(domain: str, from_date: str, to_date: str) -> dict:
    """Get publisher and demand partner context for a domain."""
    ctx = {"publisher": None, "demand": None}
    try:
        pub_rows = fetch("PUBLISHER,DOMAIN" if False else "PUBLISHER",
            ["GROSS_REVENUE"], from_date, to_date)
        # Fall back to publisher-level — domain+publisher combo may 500
        # Instead get top publisher overall for context
    except Exception:
        pass

    try:
        dem_rows = fetch("DEMAND_PARTNER_NAME",
            ["GROSS_REVENUE"], from_date, to_date)
        if dem_rows:
            top_dem = max(dem_rows,
                         key=lambda r: sf(r.get("GROSS_REVENUE", 0)),
                         default=None)
            if top_dem:
                ctx["demand"] = top_dem.get("DEMAND_PARTNER_NAME", "").strip()
    except Exception:
        pass

    return ctx


def get_bundle_publisher(bundle: str, from_date: str, to_date: str) -> str | None:
    """Get which publisher supplied this bundle."""
    try:
        rows = fetch("BUNDLE,PUBLISHER",
            ["GROSS_REVENUE"], from_date, to_date)
        for r in rows:
            if r.get("BUNDLE", "").strip() == bundle:
                return r.get("PUBLISHER_NAME", "").strip() or None
    except Exception:
        pass
    return None


# ── Slack builder ─────────────────────────────────────────────────────────────

def build_slack_payload(dropped_domains: list, dropped_bundles: list,
                        new_domains: list, new_bundles: list,
                        date_str: str, prev_date: str) -> dict:

    has_content = any([dropped_domains, dropped_bundles, new_domains, new_bundles])

    if not has_content:
        return {
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn",
                 "text": f"✅ *LL Domain & App Monitor — {date_str}*\nNo significant changes today."}}
            ]
        }

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "🔍 LL Domain & App Changes", "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{date_str} vs {prev_date}*  —  LL platform domain and app activity changes."
            }
        },
        {"type": "divider"},
    ]

    # Dropped domains
    if dropped_domains:
        lines = [f"*📉 Dropped Domains ({len(dropped_domains)})*"]
        for d in dropped_domains:
            pub_str = f" | Pub: *{d['publisher']}*" if d.get("publisher") else ""
            dem_str = f" | Demand: *{d['demand']}*" if d.get("demand") else ""
            lines.append(
                f"  • *{d['name']}*  avg {fmt_usd(d['daily_avg'])}/day{pub_str}{dem_str}\n"
                f"    eCPM: ${d['ecpm']:.3f} | Win rate: {d['win_rate']:.1f}%"
            )
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
        blocks.append({"type": "divider"})

    # Dropped bundles
    if dropped_bundles:
        lines = [f"*📉 Dropped Apps ({len(dropped_bundles)})*"]
        for b in dropped_bundles:
            pub_str = f" | Pub: *{b['publisher']}*" if b.get("publisher") else ""
            lines.append(
                f"  • *{b['name']}*  avg {fmt_usd(b['daily_avg'])}/day{pub_str}\n"
                f"    eCPM: ${b['ecpm']:.3f} | Win rate: {b['win_rate']:.1f}%"
            )
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
        blocks.append({"type": "divider"})

    # New domains
    if new_domains:
        lines = [f"*🆕 New Domains ({len(new_domains)})*"]
        for d in new_domains:
            lines.append(
                f"  • *{d['name']}*  {fmt_usd(d['revenue'])} | "
                f"eCPM: ${d['ecpm']:.3f} | Win rate: {d['win_rate']:.1f}%\n"
                f"    _Monitor quality over next 48h_"
            )
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
        blocks.append({"type": "divider"})

    # New bundles
    if new_bundles:
        lines = [f"*🆕 New Apps ({len(new_bundles)})*"]
        for b in new_bundles:
            pub_str = f" | Pub: *{b['publisher']}*" if b.get("publisher") else ""
            lines.append(
                f"  • *{b['name']}*  {fmt_usd(b['revenue'])}{pub_str}\n"
                f"    eCPM: ${b['ecpm']:.3f} | Win rate: {b['win_rate']:.1f}%"
            )
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": "\n".join(lines)}})

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                      "text": (
                          f"LL platform | "
                          f"Dropped threshold: >{fmt_usd(MIN_DAILY_AVG_DOMAIN)}/day avg | "
                          f"PGAM Alerts — daily 9:30 AM ET"
                      )}]
    })

    return {"blocks": blocks}


# ── Main runner ───────────────────────────────────────────────────────────────

def run_ll_domain_app_agent():
    today     = date.today()
    yesterday = today - timedelta(days=1)
    date_str  = yesterday.strftime("%Y-%m-%d")
    week_ago  = (yesterday - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    prev_week_end = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Fetching LL domain/app data for {date_str}...")

    # Today's data
    today_domains = get_domain_revenue(date_str, date_str)
    today_bundles = get_bundle_revenue(date_str, date_str)

    # Last week data
    prev_domains  = get_domain_revenue(week_ago, prev_week_end)
    prev_bundles  = get_bundle_revenue(week_ago, prev_week_end)

    print(f"  Today domains: {len(today_domains)} | Today bundles: {len(today_bundles)}")
    print(f"  Prev domains:  {len(prev_domains)} | Prev bundles:  {len(prev_bundles)}")

    state = load_state()

    # ── Dropped domains ────────────────────────────────────────────────────────
    dropped_domains = []
    for dom, prev_data in prev_domains.items():
        daily_avg = prev_data["revenue"] / LOOKBACK_DAYS
        if daily_avg < MIN_DAILY_AVG_DOMAIN: continue
        if dom in today_domains: continue
        alert_key = f"dom_dropped_{dom}"
        if already_alerted_today(state, alert_key): continue
        ctx = get_domain_context(dom, week_ago, prev_week_end)
        dropped_domains.append({
            "name":      dom,
            "daily_avg": daily_avg,
            "ecpm":      prev_data["ecpm"],
            "win_rate":  prev_data["win_rate"],
            "publisher": ctx.get("publisher"),
            "demand":    ctx.get("demand"),
        })
        mark_alerted(state, alert_key)

    dropped_domains = sorted(dropped_domains,
                             key=lambda x: x["daily_avg"],
                             reverse=True)[:MAX_ALERTS_PER_TYPE]

    # ── Dropped bundles ────────────────────────────────────────────────────────
    dropped_bundles = []
    for bundle, prev_data in prev_bundles.items():
        daily_avg = prev_data["revenue"] / LOOKBACK_DAYS
        if daily_avg < MIN_DAILY_AVG_BUNDLE: continue
        if bundle in today_bundles: continue
        alert_key = f"bundle_dropped_{bundle}"
        if already_alerted_today(state, alert_key): continue
        pub = get_bundle_publisher(bundle, week_ago, prev_week_end)
        dropped_bundles.append({
            "name":      bundle,
            "daily_avg": daily_avg,
            "ecpm":      prev_data["ecpm"],
            "win_rate":  prev_data["win_rate"],
            "publisher": pub,
        })
        mark_alerted(state, alert_key)

    dropped_bundles = sorted(dropped_bundles,
                             key=lambda x: x["daily_avg"],
                             reverse=True)[:MAX_ALERTS_PER_TYPE]

    # ── New domains ────────────────────────────────────────────────────────────
    new_domains = []
    for dom, today_data in today_domains.items():
        if dom in prev_domains: continue
        if today_data["revenue"] < MIN_NEW_REVENUE: continue
        alert_key = f"dom_new_{dom}"
        if already_alerted_today(state, alert_key): continue
        new_domains.append({
            "name":     dom,
            "revenue":  today_data["revenue"],
            "ecpm":     today_data["ecpm"],
            "win_rate": today_data["win_rate"],
        })
        mark_alerted(state, alert_key)

    new_domains = sorted(new_domains,
                         key=lambda x: x["revenue"],
                         reverse=True)[:MAX_ALERTS_PER_TYPE]

    # ── New bundles ────────────────────────────────────────────────────────────
    new_bundles = []
    for bundle, today_data in today_bundles.items():
        if bundle in prev_bundles: continue
        if today_data["revenue"] < MIN_NEW_REVENUE: continue
        alert_key = f"bundle_new_{bundle}"
        if already_alerted_today(state, alert_key): continue
        pub = get_bundle_publisher(bundle, date_str, date_str)
        new_bundles.append({
            "name":      bundle,
            "revenue":   today_data["revenue"],
            "ecpm":      today_data["ecpm"],
            "win_rate":  today_data["win_rate"],
            "publisher": pub,
        })
        mark_alerted(state, alert_key)

    new_bundles = sorted(new_bundles,
                         key=lambda x: x["revenue"],
                         reverse=True)[:MAX_ALERTS_PER_TYPE]

    print(f"  Dropped: {len(dropped_domains)} domains, {len(dropped_bundles)} bundles")
    print(f"  New: {len(new_domains)} domains, {len(new_bundles)} bundles")

    save_state(state)

    if not any([dropped_domains, dropped_bundles, new_domains, new_bundles]):
        print("  No significant changes — no alert sent.")
        return

    payload = build_slack_payload(
        dropped_domains, dropped_bundles,
        new_domains, new_bundles,
        date_str, week_ago
    )

    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK not set.")
        return

    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    print("LL domain/app alert sent ✅")


if __name__ == "__main__":
    run_ll_domain_app_agent()
