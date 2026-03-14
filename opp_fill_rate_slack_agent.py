"""
Opp Fill Rate Slack Alert — PGAM Alerts
========================================
Fires every 4 hours via Render cron: 0 */4 * * *

Behaviour:
- ALWAYS posts a daily MTD status update once per day (first run of the day)
- Posts a loud alert EVERY check when below 0.05% threshold
- Silent on subsequent same-day checks if above threshold

Uses SLACK_WEBHOOK env var. Formula: WINS / OPPORTUNITIES.
"""

import os
import json
import requests
from datetime import date
from opp_fill_rate_agent import (
    run_opp_fill_rate_agent, fmt_pct, fmt_num, OPP_FILL_THRESHOLD
)

SLACK_WEBHOOK    = os.environ.get("SLACK_WEBHOOK", "")
LAST_POST_FILE   = "/tmp/opp_fill_last_post.json"  # tracks last daily summary date


def post_to_slack(payload: dict) -> bool:
    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK env var not set.")
        return False
    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    return True


def get_last_post_date() -> str:
    try:
        with open(LAST_POST_FILE, "r") as f:
            return json.load(f).get("date", "")
    except Exception:
        return ""


def set_last_post_date(d: str):
    try:
        with open(LAST_POST_FILE, "w") as f:
            json.dump({"date": d}, f)
    except Exception:
        pass


def _slack_diag_block(title: str, icon: str, rows: list,
                      mtd_fill: float, top_n: int = 5) -> dict | None:
    if not rows:
        return None
    lines = [f"{icon} *{title}*"]
    for r in rows[:top_n]:
        new_rate = mtd_fill + r["drag_delta"]
        flag     = "🚨" if r["below_threshold"] else "✅"
        lines.append(
            f"  {flag} `{r['label'][:50]}`\n"
            f"      Fill: *{fmt_pct(r['fill_rate'])}*  |  "
            f"Opps: {fmt_num(r['opps'])}  |  "
            f"If removed → *{fmt_pct(new_rate)}*"
        )
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(lines)}
    }


def build_daily_summary_payload(result: dict) -> dict:
    """Always-on daily status message — clean and informational."""
    today       = date.today()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    today_str   = today.strftime("%Y-%m-%d")
    mtd_fill    = result["mtd_fill_rate"]
    alert       = result["alert"]

    status_line = (
        f"*{fmt_pct(mtd_fill)}* 🚨 — BELOW threshold, fee risk!"
        if alert else
        f"*{fmt_pct(mtd_fill)}* ✅ — Above threshold"
    )

    # Last 7 days trend
    trend_lines = []
    for r in result["daily_rows"][:7]:
        flag = "🚨" if r["below_threshold"] else "✅"
        trend_lines.append(
            f"  {flag} `{r['date']}`  *{fmt_pct(r['fill_rate'])}*"
            f"  —  {fmt_num(r['wins'])} wins / {fmt_num(r['opps'])} opps"
        )

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "📊 Opp Fill Rate — Daily Update", "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*MTD Fill Rate ({month_start} → {today_str})*\n"
                    f"{status_line}\n\n"
                    f"Opps: {fmt_num(result['mtd_opps'])}  |  "
                    f"Wins: {fmt_num(result['mtd_wins'])}"
                )
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Last 7 Days*\n" + "\n".join(trend_lines)
            }
        },
    ]

    # Add diagnostics if below threshold
    if alert:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⚠️ *Below threshold — see diagnostic breakdown below*"
            }
        })
        diag_sections = [
            ("By Demand Partner",             "📡", result["diag_demand"]),
            ("By Publisher × Demand Partner", "🤝", result["diag_pub_demand"]),
            ("By Bundle × Demand Partner",    "📦", result["diag_bun_demand"]),
        ]
        for title, icon, rows in diag_sections:
            block = _slack_diag_block(title, icon, rows, mtd_fill, top_n=5)
            if block:
                blocks.append({"type": "divider"})
                blocks.append(block)

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"Formula: WINS ÷ OPPORTUNITIES  |  Threshold: {fmt_pct(OPP_FILL_THRESHOLD)}  |  PGAM Alerts"
        }]
    })

    return {"blocks": blocks}


def build_alert_payload(result: dict) -> dict:
    """Loud intraday alert — only fires when below threshold."""
    today       = date.today()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    today_str   = today.strftime("%Y-%m-%d")
    mtd_fill    = result["mtd_fill_rate"]

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "🚨 Opp Fill Rate Alert — Below Threshold", "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*MTD Fill Rate is below 0.05% — fee risk if this persists to month-end.*\n\n"
                    f"*MTD Fill Rate:* `{fmt_pct(mtd_fill)}` 🚨\n"
                    f"*Period:* {month_start} → {today_str}\n"
                    f"Opps: {fmt_num(result['mtd_opps'])}  |  "
                    f"Wins: {fmt_num(result['mtd_wins'])}\n\n"
                    f"_Threshold: ≥ {fmt_pct(OPP_FILL_THRESHOLD)}_"
                )
            }
        },
        {"type": "divider"},
    ]

    diag_sections = [
        ("By Demand Partner",             "📡", result["diag_demand"]),
        ("By Publisher × Demand Partner", "🤝", result["diag_pub_demand"]),
        ("By Bundle × Demand Partner",    "📦", result["diag_bun_demand"]),
    ]
    for title, icon, rows in diag_sections:
        block = _slack_diag_block(title, icon, rows, mtd_fill, top_n=5)
        if block:
            blocks.append(block)
            blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"Formula: WINS ÷ OPPORTUNITIES  |  "
                f"Threshold: {fmt_pct(OPP_FILL_THRESHOLD)}  |  "
                f"PGAM Alerts — fires every 4h when below threshold"
            )
        }]
    })

    return {"blocks": blocks}


def run_slack_opp_fill_alert():
    print("Fetching Opp Fill Rate data...")
    result    = run_opp_fill_rate_agent()
    today_str = date.today().strftime("%Y-%m-%d")

    print(f"MTD Fill Rate : {fmt_pct(result['mtd_fill_rate'])}")
    print(f"Alert         : {'YES 🚨' if result['alert'] else 'No ✅'}")

    last_post = get_last_post_date()

    if result["alert"]:
        # Always fire the loud alert when below threshold
        print("Below threshold — posting alert to Slack...")
        post_to_slack(build_alert_payload(result))
        print("Alert sent ✅")

        # Also send daily summary if not sent today
        if last_post != today_str:
            print("Sending daily summary too...")
            post_to_slack(build_daily_summary_payload(result))
            set_last_post_date(today_str)
            print("Daily summary sent ✅")

    else:
        # Above threshold — only send daily summary once per day
        if last_post != today_str:
            print("Above threshold — sending daily summary...")
            post_to_slack(build_daily_summary_payload(result))
            set_last_post_date(today_str)
            print("Daily summary sent ✅")
        else:
            print("Above threshold + daily summary already sent today — no action. ✅")


if __name__ == "__main__":
    run_slack_opp_fill_alert()
