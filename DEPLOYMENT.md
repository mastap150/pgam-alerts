# PGAM Alert System - Deployment Guide

## Overview
Smart threshold-based alerts sent to Slack only (no email spam).
Runs on 3 separate cron schedules based on alert priority.

## 📋 All 20 Alert Types

### 🔴 Tier 1 - Critical (Check Hourly, 8 AM - 8 PM ET)
1. **Revenue Crash** - Revenue down >30% vs yesterday same hour
2. **Publisher Goes Dark** - Top 3 publisher drops to $0
3. **Timeout Spike** - Timeout rate >10%
4. **Win Rate Crash** - Win rate drops >50%
5. **No Bids** - Top publisher getting 0 bids for 2+ hours

### ⚠️ Tier 2 - Important (Check Every 3 Hours)
6. **Revenue Below Pace** - By noon, <60% of daily projection
7. **Fill Rate Collapse** - Fill rate drops >40% vs 7-day avg
8. **Margin Compression** - Margin drops below 25%
9. **eCPM Collapse** - eCPM drops >40% (placeholder in code)
10. **Anomaly Detection** - Critical anomaly detected (placeholder)
11. **Bid Request Drop** - Bid requests down >50% (placeholder)

### 🚀 Tier 3 - Growth Opportunities (Check Every 2-4 Hours)
12. **Publisher Breakout** - Publisher revenue up >100% AND >$100/hour
13. **Demand Partner Surge** - Demand spending up >150% AND >$200/day
14. **App Explosion** - App goes from <$20 to >$200 in 24 hours
15. **Geographic Goldmine** - Country revenue up >200% (placeholder)
16. **Format Winner** - Ad format revenue up >80% (placeholder)
17. **Consecutive Growth Streak** - Revenue up 3+ hours in a row (placeholder)

### 💎 Tier 4 - Discoveries (Check Twice Daily)
18. **High-Value New App** - New app >$50/hour in first 6 hours (placeholder)
19. **Premium eCPM Opportunity** - App at >$15 eCPM with <10K impressions
20. **Publisher Payout Spike** - Payout >2x normal (placeholder)

*Note: Some alerts marked as "placeholder" are partially implemented and can be expanded.*

---

## 🚀 Deploy to Render

### Step 1: Create GitHub Repo for Alerts

```bash
cd ~/Desktop
mkdir pgam-alerts
cd pgam-alerts

# Copy alert files
cp /path/to/pgam_alerts/* .

# Initialize git
git init
git add .
git commit -m "Initial: PGAM Alert System with 20 alert types"

# Push to GitHub
git remote add origin https://github.com/YOUR_USERNAME/pgam-alerts.git
git branch -M main
git push -u origin main
```

### Step 2: Create 3 Cron Jobs in Render

Go to Render Dashboard → New → Cron Job

#### **Cron Job #1: Critical Alerts (Hourly)**

- **Name:** `pgam-alerts-critical`
- **Repository:** `YOUR_USERNAME/pgam-alerts`
- **Branch:** `main`
- **Schedule:** `0 13-1 * * *` (8 AM - 8 PM ET = 13:00-01:00 UTC next day)
  - Note: This spans midnight UTC, so use: `0 13-23,0-1 * * *`
- **Command:** `python3 alerts.py --tier critical`
- **Region:** Oregon (US West)
- **Plan:** Free

**Environment Variables:**
```
SLACK_WEBHOOK=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

#### **Cron Job #2: Important Alerts (Every 3 Hours)**

- **Name:** `pgam-alerts-important`
- **Schedule:** `0 14,17,20,23 * * *` (9 AM, 12 PM, 3 PM, 6 PM ET)
- **Command:** `python3 alerts.py --tier important`
- **Environment Variables:** Same as above

#### **Cron Job #3: Opportunities (Every 4 Hours)**

- **Name:** `pgam-alerts-opportunities`
- **Schedule:** `0 15,19,23 * * *` (10 AM, 2 PM, 6 PM ET)
- **Command:** `python3 alerts.py --tier opportunities`
- **Environment Variables:** Same as above

#### **Cron Job #4: Discoveries (Twice Daily)**

- **Name:** `pgam-alerts-discoveries`
- **Schedule:** `0 15,21 * * *` (10 AM, 4 PM ET)
- **Command:** `python3 alerts.py --tier discoveries`
- **Environment Variables:** Same as above

---

## ⚙️ Customizing Thresholds

Edit `thresholds.py` to adjust any alert sensitivity:

```python
CRITICAL = {
    "revenue_crash_pct": 30,     # Change to 20 for more sensitive
    "timeout_rate_max": 10,       # Change to 8 for stricter
    # ... etc
}
```

Then push to GitHub:
```bash
git add thresholds.py
git commit -m "Adjust alert thresholds"
git push
```

Render will auto-deploy the changes.

---

## 🔕 Anti-Spam Features

1. **Cooldown Period:** Same alert won't fire twice within 2 hours
2. **Minimum Thresholds:** Must involve meaningful $ amounts
3. **Max Alerts Per Run:** Max 10 alerts at once (rest grouped)
4. **Context Always Included:** Shows "was $X, now $Y"

---

## 📊 Alert Format (Slack)

```
🔴 REVENUE CRASH — 2:35 PM ET

Revenue Crash Detected
• Current hour: $45 (-62% vs yesterday)
• Expected: $120
• Lost revenue: $75/hour

Action: Check top publishers + demand partners immediately
```

---

## 🧪 Testing Alerts

Run manually on your local machine:

```bash
cd pgam-alerts

# Set environment variable
export SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Test critical alerts
python3 alerts.py --tier critical

# Test important alerts
python3 alerts.py --tier important

# Test growth opportunities
python3 alerts.py --tier opportunities

# Test discoveries
python3 alerts.py --tier discoveries
```

---

## 📝 Adding New Alerts

1. Add threshold to `thresholds.py`
2. Create check function in `alerts.py`:
```python
def check_my_new_alert():
    alerts = []
    # Your logic here
    if condition_met:
        if should_fire_alert("my_alert_type", entity_name):
            alerts.append({
                'type': 'My Alert',
                'priority': 'critical',  # or important, growth, discovery
                'priority_icon': '🔴',
                'title': 'Alert Title',
                'details': [
                    'Detail line 1',
                    'Detail line 2'
                ],
                'action': 'What to do about it'
            })
            record_alert("my_alert_type", entity_name)
    return alerts
```
3. Add to appropriate tier in `run_tier()` function
4. Push to GitHub

---

## 🐛 Troubleshooting

### No alerts firing
- Check Render logs for API errors
- Verify SLACK_WEBHOOK environment variable is set
- Check if thresholds are too strict

### Too many alerts
- Increase cooldown period in `thresholds.py`
- Adjust threshold percentages
- Increase minimum $ amounts

### Alerts not reaching Slack
- Test webhook URL manually: `curl -X POST -H 'Content-Type: application/json' -d '{"text":"Test"}' YOUR_WEBHOOK_URL`
- Check Render logs for HTTP errors

---

## 📅 Schedule Summary

| Time (ET) | Tier | Alerts Checked |
|-----------|------|----------------|
| 8 AM - 8 PM (hourly) | Critical | Revenue crash, Publisher dark, Timeout, Win rate, No bids |
| 9 AM, 12 PM, 3 PM, 6 PM | Important | Pacing, Fill rate, Margin |
| 10 AM, 2 PM, 6 PM | Opportunities | Breakouts, Surges, Explosions |
| 10 AM, 4 PM | Discoveries | Premium eCPM, New apps |

---

## 🎯 Next Steps

After deployment:
1. Monitor first day of alerts
2. Adjust thresholds based on noise level
3. Add more alert types as needed (placeholders ready)
4. Expand growth alerts with more metrics

---

Need help? Check Render logs or adjust thresholds!
