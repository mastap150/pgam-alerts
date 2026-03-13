"""
Opp Fill Rate Slack Alert Agent — PGAM Alerts
==============================================
Posts a daily Slack summary of Opp Fill %.
Fires a loud alert if MTD fill rate is below 0.05% threshold.

Schedule: run once daily (e.g. 9 AM ET via Render cron)
"""

import os
import requests
from datetime import date, timedelta
from opp_fill_rate_agent import run_opp_fill_rate_agent, fmt_pct, OPP_FILL_THRESHOLD

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")


def post_to_slack(payload: dict) -> bool:
    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK_URL not set.")
        return False
    resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    resp.raise_for_status()
    return True


def build_slack_payload(result: dict) -> dict:
    today       = date.today()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    today_str   = today.strftime("%Y-%m-%d")

    mtd_fill    = result["mtd_fill_rate"]
    alert       = result["alert"]
    daily_rows  = result["daily_rows"]

    # ── Header ────────────────────────────────────────────────────────────────
    if alert:
        header_text  = "🚨 *OPP FILL RATE ALERT — Below 0.05% Threshold*"
        header_color = "#d32f2f"
        mtd_status   = f"*{fmt_pct(mtd_fill)}* ← BELOW threshold (fee risk!)"
    else:
        header_text  = "📊 *Opp Fill Rate — Daily Update*"
        header_color = "#2e7d32"
        mtd_status   = f"*{fmt_pct(mtd_fill)}* ✅ Above threshold"

    # ── Daily rows (last 7 days) ───────────────────────────────────────────────
    daily_lines = []
    for r in daily_rows[:7]:
        flag = " 🚨" if r["below_threshold"] else " ✅"
        daily_lines.append(
            f"`{r['date']}`  {fmt_pct(r['fill_rate'])}{flag}  "
            f"_{r['wins']:,} wins / {r['bids']:,} bids_"
        )
    daily_text = "\n".join(daily_lines) if daily_lines else "_No data_"

    # ── Slack Block Kit payload ────────────────────────────────────────────────
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Opp Fill Rate Monitor", "emoji": True}
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{header_text}\n\n"
                    f"*MTD Fill Rate* ({month_start} → {today_str})\n"
                    f"{mtd_status}\n\n"
                    f"_{result['mtd_wins']:,} wins / {result['mtd_bids']:,} bids_"
                )
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Last 7 Days*\n{daily_text}"
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Threshold: ≥ {fmt_pct(OPP_FILL_THRESHOLD)} | Formula: WINS ÷ BIDS | PGAM Alerts"
                }
            ]
        }
    ]

    # Add urgent alert block if below threshold
    if alert:
        blocks.insert(2, {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "⚠️ *Action Required:* MTD Opp Fill Rate is below the 0.05% minimum. "
                    "If this persists through month-end, an additional fee will be charged. "
                    "Review demand routing and bid configuration immediately."
                )
            }
        })

    return {"blocks": blocks}


def run_slack_opp_fill_alert():
    print("Fetching Opp Fill Rate data...")
    result  = run_opp_fill_rate_agent()
    payload = build_slack_payload(result)

    print(f"MTD Fill Rate : {fmt_pct(result['mtd_fill_rate'])}")
    print(f"Alert         : {'YES 🚨' if result['alert'] else 'No ✅'}")

    post_to_slack(payload)
    print("Slack message sent ✅")


if __name__ == "__main__":
    run_slack_opp_fill_alert()
