"""
Teqblaze Alerts - FIXED VERSION
Fixes:
1. Persistent deduplication using date-keyed file (survives container restarts)
2. No false $0 revenue alerts — requires 3 consecutive zero checks before alerting
3. Revenue behind pace only fires once per day, after 2 PM ET
4. DSP dropout threshold raised — only fires for DSPs averaging >$500/day
5. Domain dropout threshold raised — only fires for domains averaging >$50/day
6. 400 API errors handled gracefully
"""

import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from api import fetch, sf, pct

# Persistent deduplication — keyed by date so it auto-resets daily
ALERT_TRACKING_FILE = "/tmp/pgam_alerts_tracking.json"

# ── Deduplication ─────────────────────────────────────────────────────────────

def load_tracking() -> dict:
    try:
        with open(ALERT_TRACKING_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def save_tracking(data: dict):
    try:
        with open(ALERT_TRACKING_FILE, 'w') as f:
            json.dump(data, f)
    except Exception:
        pass


def already_alerted_today(alert_key: str) -> bool:
    today = datetime.now().strftime("%Y-%m-%d")
    tracking = load_tracking()
    return tracking.get(today, {}).get(alert_key, False)


def mark_alerted(alert_key: str):
    today = datetime.now().strftime("%Y-%m-%d")
    tracking = load_tracking()
    if today not in tracking:
        # Prune old dates — keep only last 3 days
        old_keys = [k for k in tracking if k < today]
        for k in old_keys:
            del tracking[k]
        tracking[today] = {}
    tracking[today][alert_key] = True
    save_tracking(tracking)


def get_zero_revenue_count() -> int:
    """Track consecutive zero revenue checks to avoid false alerts."""
    tracking = load_tracking()
    return tracking.get("zero_revenue_count", 0)


def set_zero_revenue_count(count: int):
    tracking = load_tracking()
    tracking["zero_revenue_count"] = count
    save_tracking(tracking)


def reset_zero_revenue_count():
    tracking = load_tracking()
    tracking["zero_revenue_count"] = 0
    save_tracking(tracking)


# ── DSP Dropout ───────────────────────────────────────────────────────────────

def check_dsp_dropped_out(date_str: str) -> list:
    alerts = []

    try:
        today_rows = fetch("DSP_NAME", ["DSP_SPEND"], date_str, date_str)
        today_dsps = {r.get("DSP_NAME", ""): sf(r.get("DSP_SPEND", 0)) for r in today_rows}
    except Exception as e:
        print(f"      [DSP today fetch error: {e}]")
        return alerts

    try:
        seven_days_ago = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        yesterday      = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        last_week_rows = fetch("DSP_NAME", ["DSP_SPEND"], seven_days_ago, yesterday)
    except Exception as e:
        print(f"      [DSP last week fetch error: {e}]")
        return alerts

    dsp_totals = defaultdict(float)
    for r in last_week_rows:
        dsp   = r.get("DSP_NAME", "")
        spend = sf(r.get("DSP_SPEND", 0))
        dsp_totals[dsp] += spend

    top_10 = sorted(dsp_totals.items(), key=lambda x: x[1], reverse=True)[:10]

    for dsp, last_week_total in top_10:
        daily_avg   = last_week_total / 7
        today_spend = today_dsps.get(dsp, 0)

        # Only alert if:
        # - DSP averaged >$500/day last week (material)
        # - Today has <$10 (effectively zero)
        # - Not already alerted today
        if daily_avg > 500 and today_spend < 10:
            alert_key = f"dsp_dropped_{dsp}"
            if not already_alerted_today(alert_key):
                alerts.append({
                    "type":            "dsp_dropped",
                    "dsp_name":        dsp,
                    "daily_avg":       daily_avg,
                    "today_spend":     today_spend,
                    "severity":        "high" if daily_avg > 1000 else "medium",
                })
                mark_alerted(alert_key)

    return alerts


# ── Domain Dropout ────────────────────────────────────────────────────────────

def check_domain_dropped(date_str: str) -> list:
    alerts = []

    try:
        today_rows    = fetch("DOMAIN", ["DSP_SPEND"], date_str, date_str)
        today_domains = {r.get("DOMAIN", ""): sf(r.get("DSP_SPEND", 0)) for r in today_rows}
    except Exception as e:
        print(f"      [Domain today fetch error: {e}]")
        return alerts

    try:
        seven_days_ago = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        yesterday      = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        last_week_rows = fetch("DOMAIN", ["DSP_SPEND"], seven_days_ago, yesterday)
    except Exception as e:
        print(f"      [Domain last week fetch error: {e}]")
        return alerts

    domain_totals = defaultdict(float)
    for r in last_week_rows:
        domain = r.get("DOMAIN", "")
        spend  = sf(r.get("DSP_SPEND", 0))
        domain_totals[domain] += spend

    top_20 = sorted(domain_totals.items(), key=lambda x: x[1], reverse=True)[:20]

    for domain, last_week_total in top_20:
        daily_avg    = last_week_total / 7
        today_spend  = today_domains.get(domain, 0)

        # Only alert if:
        # - Domain averaged >$50/day last week (was material)
        # - Today has $0
        # - Not already alerted today
        if daily_avg > 50 and today_spend == 0:
            alert_key = f"domain_dropped_{domain}"
            if not already_alerted_today(alert_key):
                alerts.append({
                    "type":        "domain_dropped",
                    "domain":      domain,
                    "daily_avg":   daily_avg,
                    "severity":    "high" if daily_avg > 100 else "medium",
                })
                mark_alerted(alert_key)

    return alerts


# ── Revenue Issues ────────────────────────────────────────────────────────────

def check_revenue_issues(date_str: str) -> list:
    alerts = []
    current_hour = datetime.now().hour

    try:
        today_rows  = fetch("DATE", ["DSP_SPEND"], date_str, date_str)
        today_spend = sf(today_rows[0].get("DSP_SPEND", 0)) if today_rows else 0
    except Exception as e:
        print(f"      [Revenue today fetch error: {e}]")
        return alerts

    try:
        yesterday      = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_rows = fetch("DATE", ["DSP_SPEND"], yesterday, yesterday)
        yesterday_spend = sf(yesterday_rows[0].get("DSP_SPEND", 0)) if yesterday_rows else 0
    except Exception as e:
        print(f"      [Revenue yesterday fetch error: {e}]")
        return alerts

    # ── Zero revenue check ────────────────────────────────────────────────────
    # Require 3 consecutive zero checks before alerting (avoids API lag false positives)
    if current_hour >= 9 and yesterday_spend > 1000:
        if today_spend == 0:
            count = get_zero_revenue_count() + 1
            set_zero_revenue_count(count)
            print(f"      [Zero revenue count: {count}/3]")
            if count >= 3:
                alert_key = "revenue_zero"
                if not already_alerted_today(alert_key):
                    alerts.append({
                        "type":            "revenue_zero",
                        "yesterday_spend": yesterday_spend,
                        "severity":        "critical",
                    })
                    mark_alerted(alert_key)
        else:
            reset_zero_revenue_count()

    # ── Behind pace check ─────────────────────────────────────────────────────
    # Only after 2 PM ET, only once per day, only if >40% behind
    if current_hour >= 14 and yesterday_spend > 1000:
        expected_pacing = current_hour / 24
        today_pacing    = today_spend / yesterday_spend if yesterday_spend > 0 else 0
        behind          = expected_pacing - today_pacing

        if behind > 0.40:
            alert_key = "revenue_behind_pace"
            if not already_alerted_today(alert_key):
                alerts.append({
                    "type":            "revenue_behind_pace",
                    "pacing":          today_pacing * 100,
                    "expected":        expected_pacing * 100,
                    "today_spend":     today_spend,
                    "yesterday_spend": yesterday_spend,
                    "behind_pct":      behind * 100,
                    "severity":        "high",
                })
                mark_alerted(alert_key)

    return alerts


# ── Formatter ─────────────────────────────────────────────────────────────────

def format_alert_message(alert: dict) -> str:
    if alert["type"] == "dsp_dropped":
        sev = "🔴" if alert["severity"] == "high" else "🟡"
        return (
            f"{sev} *Top DSP Dropped — {alert['dsp_name']}*\n"
            f"Avg ${alert['daily_avg']:,.0f}/day last week | Today: ${alert['today_spend']:.0f}\n"
            f"Check if endpoint is paused or has a technical issue."
        )

    elif alert["type"] == "domain_dropped":
        sev = "🔴" if alert["severity"] == "high" else "🟡"
        return (
            f"{sev} *Top Domain Dropped — {alert['domain']}*\n"
            f"Avg ${alert['daily_avg']:,.0f}/day last week | Today: $0\n"
            f"Check if intentionally removed or supply issue."
        )

    elif alert["type"] == "revenue_zero":
        return (
            f"🚨 *CRITICAL: Zero Revenue Today*\n"
            f"Yesterday: ${alert['yesterday_spend']:,.0f}\n"
            f"Today: $0.00 (confirmed across 3 checks)\n"
            f"Check DSP endpoint health immediately."
        )

    elif alert["type"] == "revenue_behind_pace":
        return (
            f"⚠️ *Revenue Behind Pace*\n"
            f"{alert['behind_pct']:.0f}% behind expected at this hour\n"
            f"Today so far: ${alert['today_spend']:,.0f} | "
            f"Expected: ${alert['yesterday_spend'] * alert['expected'] / 100:,.0f}\n"
            f"Check DSP endpoint health and routing."
        )

    return str(alert)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_hourly_alerts(date_str: str = None) -> str:
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  Teqblaze Alerts — {datetime.now().strftime('%Y-%m-%d %H:%M')} ET")
    print(f"{'='*60}\n")

    all_alerts = []

    print("[1/3] Checking revenue issues...")
    revenue_alerts = check_revenue_issues(date_str)
    all_alerts.extend(revenue_alerts)
    print(f"      {len(revenue_alerts)} revenue alerts")

    print("[2/3] Checking DSP dropouts...")
    dsp_alerts = check_dsp_dropped_out(date_str)
    all_alerts.extend(dsp_alerts)
    print(f"      {len(dsp_alerts)} DSP alerts")

    print("[3/3] Checking domain dropouts...")
    domain_alerts = check_domain_dropped(date_str)
    all_alerts.extend(domain_alerts)
    print(f"      {len(domain_alerts)} domain alerts")

    print(f"\n  Total: {len(all_alerts)} new alerts")

    if not all_alerts:
        return f"✅ Teqblaze Hourly Check — {datetime.now().strftime('%H:%M')} ET — All metrics normal, no alerts."

    severity_order = {"critical": 0, "high": 1, "medium": 2}
    all_alerts.sort(key=lambda x: severity_order.get(x.get("severity", "medium"), 2))

    critical_count = sum(1 for a in all_alerts if a.get("severity") == "critical")
    high_count     = sum(1 for a in all_alerts if a.get("severity") == "high")
    medium_count   = len(all_alerts) - critical_count - high_count

    header = f"*Teqblaze Hourly Alerts — {datetime.now().strftime('%H:%M')} ET*\n"
    if critical_count: header += f"🚨 {critical_count} Critical"
    if high_count:     header += f" | 🔴 {high_count} High"
    if medium_count:   header += f" | 🟡 {medium_count} Medium"

    messages = [header] + [format_alert_message(a) for a in all_alerts]
    return "\n\n".join(messages)


if __name__ == "__main__":
    import sys
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    print(run_hourly_alerts(date_str))
