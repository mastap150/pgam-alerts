"""
Teqblaze Alerts - OPTIMIZED VERSION
Fixes:
1. Deduplication - only alert once per day per issue
2. Context - shows SSP/DSP breakdown for domains
3. No false $0.00 alerts
4. Smart thresholds
"""

import json
from datetime import datetime, timedelta
from collections import defaultdict
from api import fetch, sf, pct

# Daily alert tracking (reset at midnight)
ALERT_TRACKING_FILE = "/tmp/alerts_sent_today.json"

def load_alerts_sent_today():
    """Load which alerts we've already sent today"""
    try:
        with open(ALERT_TRACKING_FILE, 'r') as f:
            data = json.load(f)
            # Check if it's from today
            if data.get('date') == datetime.now().strftime("%Y-%m-%d"):
                return set(data.get('alerts', []))
    except:
        pass
    return set()

def save_alert_sent(alert_key):
    """Mark an alert as sent today"""
    alerts = load_alerts_sent_today()
    alerts.add(alert_key)
    
    with open(ALERT_TRACKING_FILE, 'w') as f:
        json.dump({
            'date': datetime.now().strftime("%Y-%m-%d"),
            'alerts': list(alerts)
        }, f)

def already_alerted_today(alert_key):
    """Check if we've already sent this alert today"""
    return alert_key in load_alerts_sent_today()


def get_domain_context(domain_id, date_str):
    """
    Get SSP/DSP breakdown for a dropped domain
    Shows which SSPs supplied it and which DSPs bought it
    """
    try:
        # Get last 7 days of data for this domain
        seven_days_ago = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        
        # Try to get SSP + DSP breakdown
        rows = fetch("DOMAIN,SSP_NAME,DSP_NAME",
            ["DSP_SPEND", "PROFIT"],
            seven_days_ago, date_str,
            filters={"DOMAIN": domain_id}
        )
        
        if not rows:
            return None
        
        # Aggregate by SSP and DSP
        ssp_spend = defaultdict(float)
        dsp_spend = defaultdict(float)
        total_spend = 0
        total_profit = 0
        
        for r in rows:
            spend = sf(r.get("DSP_SPEND", 0))
            profit = sf(r.get("PROFIT", 0))
            ssp = r.get("SSP_NAME", "Unknown")
            dsp = r.get("DSP_NAME", "Unknown")
            
            ssp_spend[ssp] += spend
            dsp_spend[dsp] += spend
            total_spend += spend
            total_profit += profit
        
        # Get top SSPs and DSPs
        top_ssps = sorted(ssp_spend.items(), key=lambda x: x[1], reverse=True)[:2]
        top_dsps = sorted(dsp_spend.items(), key=lambda x: x[1], reverse=True)[:2]
        
        margin = (total_profit / total_spend * 100) if total_spend > 0 else 0
        
        return {
            'total_spend': total_spend,
            'margin': margin,
            'top_ssp': top_ssps[0] if top_ssps else None,
            'second_ssp': top_ssps[1] if len(top_ssps) > 1 else None,
            'top_dsp': top_dsps[0] if top_dsps else None,
            'second_dsp': top_dsps[1] if len(top_dsps) > 1 else None,
        }
    except Exception as e:
        print(f"      [Error getting domain context: {e}]")
        return None


def check_dsp_dropped_out(date_str):
    """
    Check for DSPs that were top 10 last week but have $0 today
    ONLY ALERT ONCE PER DAY per DSP
    """
    alerts = []
    
    # Get today's DSP spend
    today_rows = fetch("DSP_NAME", ["DSP_SPEND"], date_str, date_str)
    today_dsps = {r.get("DSP_NAME"): sf(r.get("DSP_SPEND", 0)) for r in today_rows}
    
    # Get last week's top 10 DSPs
    seven_days_ago = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    yesterday = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    
    last_week_rows = fetch("DSP_NAME", ["DSP_SPEND"], seven_days_ago, yesterday)
    
    # Aggregate by DSP
    dsp_totals = defaultdict(float)
    for r in last_week_rows:
        dsp = r.get("DSP_NAME", "")
        spend = sf(r.get("DSP_SPEND", 0))
        dsp_totals[dsp] += spend
    
    # Get top 10
    top_10 = sorted(dsp_totals.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # Check which ones are missing today
    for dsp, last_week_spend in top_10:
        today_spend = today_dsps.get(dsp, 0)
        
        # Only alert if:
        # 1. DSP has <$10 today (basically $0)
        # 2. DSP had >$100 last week (was material)
        # 3. Haven't already alerted today
        if today_spend < 10 and last_week_spend > 100:
            alert_key = f"dsp_dropped_{dsp}"
            
            if not already_alerted_today(alert_key):
                alerts.append({
                    'type': 'dsp_dropped',
                    'dsp_name': dsp,
                    'last_week_spend': last_week_spend / 7,  # Daily average
                    'severity': 'high' if last_week_spend > 1000 else 'medium'
                })
                save_alert_sent(alert_key)
    
    return alerts


def check_domain_dropped(date_str):
    """
    Check for domains that were top 20 last week but have $0 today
    WITH SSP/DSP CONTEXT
    ONLY ALERT ONCE PER DAY per domain
    """
    alerts = []
    
    # Get today's domain spend
    today_rows = fetch("DOMAIN", ["DSP_SPEND"], date_str, date_str)
    today_domains = {r.get("DOMAIN"): sf(r.get("DSP_SPEND", 0)) for r in today_rows}
    
    # Get last week's top 20 domains
    seven_days_ago = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    yesterday = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    
    last_week_rows = fetch("DOMAIN", ["DSP_SPEND"], seven_days_ago, yesterday)
    
    # Aggregate
    domain_totals = defaultdict(float)
    for r in last_week_rows:
        domain = r.get("DOMAIN", "")
        spend = sf(r.get("DSP_SPEND", 0))
        domain_totals[domain] += spend
    
    # Get top 20
    top_20 = sorted(domain_totals.items(), key=lambda x: x[1], reverse=True)[:20]
    
    # Check which are missing
    for domain, last_week_spend in top_20:
        today_spend = today_domains.get(domain, 0)
        
        # Only alert if:
        # 1. Domain has $0 today
        # 2. Domain had >$20 last week (was material)
        # 3. Haven't alerted today
        if today_spend == 0 and last_week_spend > 20:
            alert_key = f"domain_dropped_{domain}"
            
            if not already_alerted_today(alert_key):
                # Get SSP/DSP context
                context = get_domain_context(domain, yesterday)
                
                alerts.append({
                    'type': 'domain_dropped',
                    'domain': domain,
                    'last_week_spend': last_week_spend / 7,  # Daily average
                    'context': context,
                    'severity': 'high' if last_week_spend > 100 else 'medium'
                })
                save_alert_sent(alert_key)
    
    return alerts


def check_revenue_issues(date_str):
    """
    Check for revenue problems
    FILTERS OUT FALSE $0.00 ALERTS
    """
    alerts = []
    
    # Get today's revenue so far
    rows = fetch("DATE", ["DSP_SPEND"], date_str, date_str)
    
    if not rows:
        return alerts
    
    today_spend = sf(rows[0].get("DSP_SPEND", 0))
    
    # Get yesterday's full day
    yesterday = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday_rows = fetch("DATE", ["DSP_SPEND"], yesterday, yesterday)
    yesterday_spend = sf(yesterday_rows[0].get("DSP_SPEND", 0)) if yesterday_rows else 0
    
    # Current hour
    current_hour = datetime.now().hour
    
    # Only alert if:
    # 1. It's past 9 AM (not midnight data lag)
    # 2. Today's spend is ACTUALLY $0 (not just API lag)
    # 3. Yesterday had material spend (>$1000)
    if current_hour >= 9 and today_spend == 0 and yesterday_spend > 1000:
        alert_key = "revenue_zero"
        
        if not already_alerted_today(alert_key):
            alerts.append({
                'type': 'revenue_zero',
                'yesterday_spend': yesterday_spend,
                'severity': 'critical'
            })
            save_alert_sent(alert_key)
    
    # Check if significantly behind pace
    elif current_hour >= 12:  # Only check after noon
        expected_pacing = current_hour / 24
        today_pacing = today_spend / yesterday_spend if yesterday_spend > 0 else 0
        
        # Alert if more than 40% behind
        if today_pacing < (expected_pacing - 0.4):
            alert_key = "revenue_behind_pace"
            
            if not already_alerted_today(alert_key):
                alerts.append({
                    'type': 'revenue_behind_pace',
                    'pacing': today_pacing * 100,
                    'expected': expected_pacing * 100,
                    'today_spend': today_spend,
                    'yesterday_spend': yesterday_spend,
                    'severity': 'high'
                })
                save_alert_sent(alert_key)
    
    return alerts


def format_alert_message(alert):
    """
    Format alert into Slack message with full context
    """
    if alert['type'] == 'dsp_dropped':
        severity = "🔴" if alert['severity'] == 'high' else "🟡"
        return f"""{severity} *Top DSP Dropped Out — {alert['dsp_name']}*
Was averaging ${alert['last_week_spend']:.0f}/day last week, has <$10 today
Check if endpoint is paused or has technical issue"""

    elif alert['type'] == 'domain_dropped':
        severity = "🔴" if alert['severity'] == 'high' else "🟡"
        ctx = alert.get('context')
        
        base_msg = f"""{severity} *Top Domain Dropped — {alert['domain']}*
Was averaging ${alert['last_week_spend']:.0f}/day last week, $0 today"""
        
        if ctx:
            # Add SSP/DSP context
            ssp_info = ""
            if ctx.get('top_ssp'):
                ssp_name, ssp_spend = ctx['top_ssp']
                ssp_info += f"\n• Main SSP: {ssp_name} (${ssp_spend:.0f})"
                if ctx.get('second_ssp'):
                    ssp2_name, ssp2_spend = ctx['second_ssp']
                    ssp_info += f"\n• 2nd SSP: {ssp2_name} (${ssp2_spend:.0f})"
            
            dsp_info = ""
            if ctx.get('top_dsp'):
                dsp_name, dsp_spend = ctx['top_dsp']
                dsp_info += f"\n• Top buyer: {dsp_name} (${dsp_spend:.0f})"
            
            margin_info = f"\nMargin: {ctx['margin']:.1f}%"
            
            return base_msg + ssp_info + dsp_info + margin_info
        else:
            return base_msg + "\nCheck if intentionally removed or supply issue"

    elif alert['type'] == 'revenue_zero':
        return f"""🚨 *CRITICAL: Zero Revenue Today*
Yesterday: ${alert['yesterday_spend']:,.0f}
Today: $0.00 (past 9 AM)

Check DSP endpoint health immediately"""

    elif alert['type'] == 'revenue_behind_pace':
        behind_pct = alert['expected'] - alert['pacing']
        return f"""⚠️ *Revenue Behind Pace*
{behind_pct:.0f}% behind yesterday's pace
Today so far: ${alert['today_spend']:,.0f}
Expected: ${alert['yesterday_spend'] * alert['expected'] / 100:,.0f}

Check DSP endpoint health"""

    return str(alert)


def run_hourly_alerts(date_str=None):
    """
    Main hourly alert function
    OPTIMIZED - Less spam, more context
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{'='*60}")
    print(f"  Teqblaze Alerts - {datetime.now().strftime('%Y-%m-%d %H:%M')} ET")
    print(f"{'='*60}\n")
    
    all_alerts = []
    
    # Check revenue issues first (most critical)
    print("[1/3] Checking revenue issues...")
    revenue_alerts = check_revenue_issues(date_str)
    all_alerts.extend(revenue_alerts)
    print(f"      {len(revenue_alerts)} revenue alerts")
    
    # Check DSP dropouts
    print("[2/3] Checking DSP dropouts...")
    dsp_alerts = check_dsp_dropped_out(date_str)
    all_alerts.extend(dsp_alerts)
    print(f"      {len(dsp_alerts)} DSP alerts")
    
    # Check domain dropouts (with context)
    print("[3/3] Checking domain dropouts...")
    domain_alerts = check_domain_dropped(date_str)
    all_alerts.extend(domain_alerts)
    print(f"      {len(domain_alerts)} domain alerts")
    
    print(f"\n{'='*60}")
    print(f"  Total: {len(all_alerts)} alerts")
    print(f"{'='*60}\n")
    
    # Format and return
    if not all_alerts:
        return "✅ Teqblaze Hourly Check — All metrics normal, no alerts."
    
    # Sort by severity
    severity_order = {'critical': 0, 'high': 1, 'medium': 2}
    all_alerts.sort(key=lambda x: severity_order.get(x.get('severity', 'medium'), 2))
    
    # Format messages
    messages = []
    critical_count = sum(1 for a in all_alerts if a.get('severity') == 'critical')
    high_count = sum(1 for a in all_alerts if a.get('severity') == 'high')
    medium_count = len(all_alerts) - critical_count - high_count
    
    header = f"*Teqblaze Hourly Alerts — {datetime.now().strftime('%H:%M')} ET*\n"
    
    if critical_count > 0:
        header += f"🚨 *{critical_count} Critical*"
    if high_count > 0:
        header += f" | 🔴 {high_count} High"
    if medium_count > 0:
        header += f" | 🟡 {medium_count} Medium"
    
    messages.append(header + "\n")
    
    for alert in all_alerts:
        messages.append(format_alert_message(alert))
    
    return "\n\n".join(messages)


if __name__ == "__main__":
    import sys
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    result = run_hourly_alerts(date_str)
    print(result)
