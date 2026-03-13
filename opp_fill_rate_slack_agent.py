"""
Opp Fill Rate Slack Alert — PGAM Alerts
========================================
Fires every 4 hours via Render cron: 0 */4 * * *
Only posts when MTD fill rate is BELOW 0.05% threshold.
Formula: WINS / OPPORTUNITIES

Uses SLACK_WEBHOOK env var (consistent with other PGAM agents).
"""

import os
import requests
from opp_fill_rate_agent import (
    run_opp_fill_rate_agent, fmt_pct, fmt_num, OPP_FILL_THRESHOLD
)
from datetime import date

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "")


def post_to_slack(payload: dict) -> bool:
    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK env var not set.")
        return False
    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    return True


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


def build_slack_payload(result: dict) -> dict:
    today       = date.today()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    today_str   = today.strftime("%Y-%m-%d")
    mtd_fill    = result["mtd_fill_rate"]

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": "🚨 Opp Fill Rate Below Threshold", "emoji": True}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*MTD Fill Rate is below 0.05% — fee risk if this persists to month-end.*\n\n"
                    f"*MTD Fill Rate:* `{fmt_pct(mtd_fill)}` 🚨\n"
                    f"*Period:* {month_start} → {today_str}\n"
                    f"*Opportunities:* {fmt_num(result['mtd_opps'])}  |  "
                    f"*Wins:* {fmt_num(result['mtd_wins'])}\n\n"
                    f"_Threshold: ≥ {fmt_pct(OPP_FILL_THRESHOLD)} (WINS ÷ OPPORTUNITIES)_"
                )
            }
        },
        {"type": "divider"},
    ]

    # Last 3 days mini-trend
    recent = result["daily_rows"][:3]
    if recent:
        trend_lines = []
        for r in recent:
            flag = "🚨" if r["below_threshold"] else "✅"
            trend_lines.append(
                f"  {flag} `{r['date']}`  "
                f"*{fmt_pct(r['fill_rate'])}*  —  "
                f"{fmt_num(r['wins'])} wins / {fmt_num(r['opps'])} opps"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Recent Daily Trend*\n" + "\n".join(trend_lines)}
        })
        blocks.append({"type": "divider"})

    # Diagnostic sections
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

    # Footer
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"Formula: WINS ÷ OPPORTUNITIES  |  "
                f"Threshold: {fmt_pct(OPP_FILL_THRESHOLD)}  |  "
                f"Min opps for signal: 1,000  |  "
                f"PGAM Alerts — runs every 4h, only fires when below threshold"
            )
        }]
    })

    return {"blocks": blocks}


def run_slack_opp_fill_alert():
    print("Fetching Opp Fill Rate data...")
    result = run_opp_fill_rate_agent()

    print(f"MTD Fill Rate : {fmt_pct(result['mtd_fill_rate'])}")
    print(f"Alert         : {'YES 🚨' if result['alert'] else 'No ✅'}")

    if not result["alert"]:
        print("Fill rate above threshold — no alert sent. ✅")
        return

    print("Below threshold — posting to Slack...")
    payload = build_slack_payload(result)
    post_to_slack(payload)
    print("Slack alert sent ✅")


if __name__ == "__main__":
    run_slack_opp_fill_alert()
