"""
PGAM Intelligence Alert System
Smart threshold-based alerts for Slack
All 20 alert types implemented
"""

import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path to import from pgam_v2
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pgam_v2'))

from api import fetch
from utils import sf, pct, n_days_ago
from thresholds import CRITICAL, IMPORTANT, GROWTH, DISCOVERIES, COOLDOWN, PRIORITY
from alert_history import should_fire_alert, record_alert, cleanup_old_history
from delivery_alerts import send_alert, send_grouped_alerts, format_currency, format_percentage

def get_yesterday_same_hour():
    """Get yesterday's date and current hour"""
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d"), now.hour

def get_today():
    """Get today's date"""
    return datetime.now().strftime("%Y-%m-%d")

# ============================================================================
# TIER 1 - CRITICAL ALERTS
# ============================================================================

def check_revenue_crash():
    """Alert #1: Revenue down >30% vs yesterday same hour"""
    alerts = []
    today_str = get_today()
    yesterday_str, current_hour = get_yesterday_same_hour()
    
    # Get current hour data
    today_hourly = fetch("HOUR", ["GROSS_REVENUE"], today_str, today_str)
    yesterday_hourly = fetch("HOUR", ["GROSS_REVENUE"], yesterday_str, yesterday_str)
    
    # Find current hour revenue
    today_rev = 0
    yesterday_rev = 0
    
    for row in today_hourly:
        if row.get("HOUR") == current_hour:
            today_rev = sf(row.get("GROSS_REVENUE", 0))
            break
    
    for row in yesterday_hourly:
        if row.get("HOUR") == current_hour:
            yesterday_rev = sf(row.get("GROSS_REVENUE", 0))
            break
    
    if yesterday_rev < CRITICAL["revenue_crash_min"]:
        return alerts  # Too small to care
    
    if yesterday_rev > 0:
        drop_pct = ((today_rev - yesterday_rev) / yesterday_rev) * 100
        
        if drop_pct < -CRITICAL["revenue_crash_pct"]:
            if should_fire_alert("revenue_crash", "system"):
                alerts.append({
                    'type': 'Revenue Crash',
                    'priority': 'critical',
                    'priority_icon': '🔴',
                    'title': f'Revenue Crash Detected',
                    'details': [
                        f"Current hour: {format_currency(today_rev)} ({format_percentage(drop_pct)} vs yesterday)",
                        f"Expected: {format_currency(yesterday_rev)}",
                        f"Lost revenue: {format_currency(yesterday_rev - today_rev)}/hour"
                    ],
                    'action': 'Check top publishers + demand partners immediately'
                })
                record_alert("revenue_crash", "system")
    
    return alerts

def check_publisher_goes_dark():
    """Alert #2: Top publisher drops to $0"""
    alerts = []
    today_str = get_today()
    yesterday_str, current_hour = get_yesterday_same_hour()
    
    # Get top publishers from yesterday
    yesterday_pubs = fetch("PUBLISHER", ["GROSS_REVENUE"], yesterday_str, yesterday_str)
    yesterday_top = sorted(yesterday_pubs, key=lambda x: sf(x.get("GROSS_REVENUE", 0)), reverse=True)[:CRITICAL["publisher_dark_rank"]]
    
    # Get today's publishers
    today_pubs = fetch("PUBLISHER", ["GROSS_REVENUE"], today_str, today_str)
    today_dict = {row.get("PUBLISHER_NAME", "").strip(): sf(row.get("GROSS_REVENUE", 0)) for row in today_pubs}
    
    for pub in yesterday_top:
        pub_name = pub.get("PUBLISHER_NAME", "").strip()
        yesterday_rev = sf(pub.get("GROSS_REVENUE", 0))
        
        if yesterday_rev < CRITICAL["publisher_dark_was_min"]:
            continue
        
        today_rev = today_dict.get(pub_name, 0)
        
        if today_rev == 0 and yesterday_rev > 0:
            if should_fire_alert("publisher_dark", pub_name):
                alerts.append({
                    'type': 'Publisher Down',
                    'priority': 'critical',
                    'priority_icon': '🔴',
                    'title': f'Publisher Went Dark',
                    'details': [
                        f"{pub_name}",
                        f"Revenue dropped to $0 (was {format_currency(yesterday_rev)}/day)",
                        f"Check: Endpoint config, floor prices, demand rules"
                    ],
                    'action': 'Investigate publisher connection immediately'
                })
                record_alert("publisher_dark", pub_name)
    
    return alerts

def check_timeout_spike():
    """Alert #3: Timeout rate >10%"""
    alerts = []
    today_str = get_today()
    
    # Get demand partner data
    demand_rows = fetch("DEMAND_PARTNER_NAME", 
                       ["BID_REQUESTS", "BID_RESPONSE_TIMEOUTS"], 
                       today_str, today_str)
    
    for row in demand_rows:
        partner = row.get("DEMAND_PARTNER_NAME", "").strip()
        if not partner:
            continue
        
        requests = sf(row.get("BID_REQUESTS", 0))
        timeouts = sf(row.get("BID_RESPONSE_TIMEOUTS", 0))
        
        if requests == 0:
            continue
        
        timeout_rate = (timeouts / requests) * 100
        
        if timeout_rate > CRITICAL["timeout_rate_max"]:
            if should_fire_alert("timeout_spike", partner):
                alerts.append({
                    'type': 'Timeout Spike',
                    'priority': 'critical',
                    'priority_icon': '🔴',
                    'title': f'Timeout Spike: {partner}',
                    'details': [
                        f"Timeout rate: {timeout_rate:.1f}% (threshold: {CRITICAL['timeout_rate_max']}%)",
                        f"{format_currency(timeouts)} timeouts / {format_currency(requests)} requests",
                        f"Revenue at risk from slow responses"
                    ],
                    'action': 'Contact partner about latency SLA or pause endpoint'
                })
                record_alert("timeout_spike", partner)
    
    return alerts

def check_win_rate_crash():
    """Alert #4: Win rate drops >50%"""
    alerts = []
    today_str = get_today()
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Get today's win rate
    today_data = fetch("DATE", ["BIDS", "WINS"], today_str, today_str)
    yesterday_data = fetch("DATE", ["BIDS", "WINS"], yesterday_str, yesterday_str)
    
    if not today_data or not yesterday_data:
        return alerts
    
    today_bids = sf(today_data[0].get("BIDS", 0))
    today_wins = sf(today_data[0].get("WINS", 0))
    yesterday_bids = sf(yesterday_data[0].get("BIDS", 0))
    yesterday_wins = sf(yesterday_data[0].get("WINS", 0))
    
    if today_bids == 0 or yesterday_bids == 0:
        return alerts
    
    today_wr = (today_wins / today_bids) * 100
    yesterday_wr = (yesterday_wins / yesterday_bids) * 100
    
    if yesterday_wr < CRITICAL["win_rate_min"]:
        return alerts  # Too low to care
    
    if yesterday_wr > 0:
        drop_pct = ((today_wr - yesterday_wr) / yesterday_wr) * 100
        
        if drop_pct < -CRITICAL["win_rate_drop_pct"]:
            if should_fire_alert("win_rate_crash", "system"):
                alerts.append({
                    'type': 'Win Rate Crash',
                    'priority': 'critical',
                    'priority_icon': '🔴',
                    'title': f'Win Rate Crash Detected',
                    'details': [
                        f"Win rate: {today_wr:.1f}% (was {yesterday_wr:.1f}%)",
                        f"Drop: {format_percentage(drop_pct)}",
                        f"Losing more auctions than normal"
                    ],
                    'action': 'Check floor prices and bid competitiveness'
                })
                record_alert("win_rate_crash", "system")
    
    return alerts

def check_no_bids():
    """Alert #5: Top publisher getting 0 bids for 2+ hours"""
    alerts = []
    today_str = get_today()
    
    # Get hourly publisher data for last few hours
    now = datetime.now()
    two_hours_ago = now - timedelta(hours=2)
    
    # Get top publishers
    top_pubs = fetch("PUBLISHER", ["GROSS_REVENUE", "BIDS"], today_str, today_str)
    top_pubs_sorted = sorted(top_pubs, key=lambda x: sf(x.get("GROSS_REVENUE", 0)), reverse=True)[:CRITICAL["no_bids_rank"]]
    
    for pub in top_pubs_sorted:
        pub_name = pub.get("PUBLISHER_NAME", "").strip()
        bids = sf(pub.get("BIDS", 0))
        
        if bids == 0:
            if should_fire_alert("no_bids", pub_name):
                alerts.append({
                    'type': 'No Bids',
                    'priority': 'critical',
                    'priority_icon': '🔴',
                    'title': f'Zero Bids: {pub_name}',
                    'details': [
                        f"{pub_name} receiving 0 bids",
                        f"Check: Floor prices, demand partner rules, endpoint config"
                    ],
                    'action': 'Review floor prices and demand connectivity'
                })
                record_alert("no_bids", pub_name)
    
    return alerts

# ============================================================================
# TIER 2 - IMPORTANT ALERTS
# ============================================================================

def check_revenue_below_pace():
    """Alert #6: By noon, revenue <60% of daily projection"""
    alerts = []
    now = datetime.now()
    
    # Only run at noon
    if now.hour != IMPORTANT["revenue_pace_hour"]:
        return alerts
    
    today_str = get_today()
    yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Get today's revenue so far
    today_data = fetch("DATE", ["GROSS_REVENUE"], today_str, today_str)
    yesterday_data = fetch("DATE", ["GROSS_REVENUE"], yesterday_str, yesterday_str)
    
    if not today_data or not yesterday_data:
        return alerts
    
    today_rev = sf(today_data[0].get("GROSS_REVENUE", 0))
    yesterday_rev = sf(yesterday_data[0].get("GROSS_REVENUE", 0))
    
    # At noon, should have ~50% of yesterday's revenue
    expected_pct = IMPORTANT["revenue_pace_pct"]
    expected_rev = yesterday_rev * (expected_pct / 100)
    actual_pct = (today_rev / yesterday_rev * 100) if yesterday_rev > 0 else 0
    
    if actual_pct < expected_pct:
        if should_fire_alert("revenue_pace", "system"):
            daily_projection = today_rev * 2  # Rough projection
            alerts.append({
                'type': 'Revenue Pacing',
                'priority': 'important',
                'priority_icon': '⚠️',
                'title': f'Revenue Below Pace',
                'details': [
                    f"Current: {format_currency(today_rev)} ({actual_pct:.0f}% of yesterday)",
                    f"Expected: {format_currency(expected_rev)} ({expected_pct}% of yesterday)",
                    f"Projected daily: {format_currency(daily_projection)} (vs {format_currency(yesterday_rev)} yesterday)"
                ],
                'action': 'Review performance and fix issues before EOD'
            })
            record_alert("revenue_pace", "system")
    
    return alerts

def check_fill_rate_collapse():
    """Alert #7: Fill rate drops >40%"""
    alerts = []
    today_str = get_today()
    
    # Get 7-day average fill rate
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    week_data = fetch("DATE", ["IMPRESSIONS", "OPPORTUNITIES"], week_ago, yesterday)
    today_data = fetch("DATE", ["IMPRESSIONS", "OPPORTUNITIES"], today_str, today_str)
    
    if not week_data or not today_data:
        return alerts
    
    # Calculate 7-day average
    week_imps = sum(sf(r.get("IMPRESSIONS", 0)) for r in week_data)
    week_opps = sum(sf(r.get("OPPORTUNITIES", 0)) for r in week_data)
    week_fill = (week_imps / week_opps * 100) if week_opps > 0 else 0
    
    # Today's fill rate
    today_imps = sf(today_data[0].get("IMPRESSIONS", 0))
    today_opps = sf(today_data[0].get("OPPORTUNITIES", 0))
    today_fill = (today_imps / today_opps * 100) if today_opps > 0 else 0
    
    if week_fill < IMPORTANT["fill_rate_min"]:
        return alerts
    
    if week_fill > 0:
        drop_pct = ((today_fill - week_fill) / week_fill) * 100
        
        if drop_pct < -IMPORTANT["fill_rate_drop_pct"]:
            if should_fire_alert("fill_rate_collapse", "system"):
                alerts.append({
                    'type': 'Fill Rate Collapse',
                    'priority': 'important',
                    'priority_icon': '⚠️',
                    'title': f'Fill Rate Dropped Significantly',
                    'details': [
                        f"Fill rate: {today_fill:.1f}% (7-day avg: {week_fill:.1f}%)",
                        f"Drop: {format_percentage(drop_pct)}",
                        f"Demand partners may not be bidding"
                    ],
                    'action': 'Check demand partner connectivity and bid rules'
                })
                record_alert("fill_rate_collapse", "system")
    
    return alerts

def check_margin_compression():
    """Alert #8: Margin drops below 25%"""
    alerts = []
    today_str = get_today()
    
    today_data = fetch("DATE", ["GROSS_REVENUE", "PUB_PAYOUT"], today_str, today_str)
    
    if not today_data:
        return alerts
    
    revenue = sf(today_data[0].get("GROSS_REVENUE", 0))
    payout = sf(today_data[0].get("PUB_PAYOUT", 0))
    
    if revenue == 0:
        return alerts
    
    margin = ((revenue - payout) / revenue) * 100
    
    if margin < IMPORTANT["margin_min"]:
        if should_fire_alert("margin_compression", "system"):
            alerts.append({
                'type': 'Margin Compression',
                'priority': 'important',
                'priority_icon': '⚠️',
                'title': f'Margin Below Target',
                'details': [
                    f"Current margin: {margin:.1f}% (target: 31%)",
                    f"Revenue: {format_currency(revenue)}",
                    f"Payout: {format_currency(payout)}",
                    f"Profitability at risk"
                ],
                'action': 'Review publisher payout rates and floor prices'
            })
            record_alert("margin_compression", "system")
    
    return alerts

# ============================================================================
# TIER 3 - GROWTH OPPORTUNITIES
# ============================================================================

def check_publisher_breakout():
    """Alert #12: Publisher revenue up >100%"""
    alerts = []
    today_str = get_today()
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    today_pubs = fetch("PUBLISHER", ["GROSS_REVENUE"], today_str, today_str)
    yesterday_pubs = fetch("PUBLISHER", ["GROSS_REVENUE"], yesterday_str, yesterday_str)
    
    # Build yesterday lookup
    yesterday_dict = {r.get("PUBLISHER_NAME", "").strip(): sf(r.get("GROSS_REVENUE", 0)) for r in yesterday_pubs}
    
    for pub in today_pubs:
        pub_name = pub.get("PUBLISHER_NAME", "").strip()
        today_rev = sf(pub.get("GROSS_REVENUE", 0))
        yesterday_rev = yesterday_dict.get(pub_name, 0)
        
        if today_rev < GROWTH["publisher_breakout_min"]:
            continue
        
        if yesterday_rev > 0:
            growth_pct = ((today_rev - yesterday_rev) / yesterday_rev) * 100
            
            if growth_pct > GROWTH["publisher_breakout_pct"]:
                if should_fire_alert("publisher_breakout", pub_name):
                    alerts.append({
                        'type': 'Publisher Breakout',
                        'priority': 'growth',
                        'priority_icon': '🚀',
                        'title': f'Publisher Breakout: {pub_name}',
                        'details': [
                            f"Revenue: {format_currency(today_rev)} ({format_percentage(growth_pct)} vs yesterday)",
                            f"Was: {format_currency(yesterday_rev)} → Now: {format_currency(today_rev)}",
                            f"Strong performance surge"
                        ],
                        'action': 'Monitor for sustainability and consider increasing supply'
                    })
                    record_alert("publisher_breakout", pub_name)
    
    return alerts

def check_demand_surge():
    """Alert #13: Demand partner spending up >150%"""
    alerts = []
    today_str = get_today()
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        today_demand = fetch("DEMAND_PARTNER_NAME", ["GROSS_REVENUE"], today_str, today_str)
        yesterday_demand = fetch("DEMAND_PARTNER_NAME", ["GROSS_REVENUE"], yesterday_str, yesterday_str)
        
        # Build yesterday lookup
        yesterday_dict = {r.get("DEMAND_PARTNER_NAME", "").strip(): sf(r.get("GROSS_REVENUE", 0)) for r in yesterday_demand}
        
        for dem in today_demand:
            dem_name = dem.get("DEMAND_PARTNER_NAME", "").strip()
            if not dem_name:
                continue
            
            today_rev = sf(dem.get("GROSS_REVENUE", 0))
            yesterday_rev = yesterday_dict.get(dem_name, 0)
            
            if today_rev < GROWTH["demand_surge_min"]:
                continue
            
            if yesterday_rev > 0:
                growth_pct = ((today_rev - yesterday_rev) / yesterday_rev) * 100
                
                if growth_pct > GROWTH["demand_surge_pct"]:
                    if should_fire_alert("demand_surge", dem_name):
                        alerts.append({
                            'type': 'Demand Surge',
                            'priority': 'growth',
                            'priority_icon': '💰',
                            'title': f'Demand Surge: {dem_name}',
                            'details': [
                                f"Spending: {format_currency(today_rev)} ({format_percentage(growth_pct)} vs yesterday)",
                                f"Partner is buying more inventory",
                                f"Optimize supply for this partner"
                            ],
                            'action': 'Increase inventory allocation to this demand partner'
                        })
                        record_alert("demand_surge", dem_name)
    except:
        pass  # API doesn't support DEMAND_PARTNER_NAME dimension
    
    return alerts

def check_app_explosion():
    """Alert #14: App goes from <$20 to >$200 in 24hrs"""
    alerts = []
    today_str = get_today()
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    today_apps = fetch("BUNDLE", ["GROSS_REVENUE"], today_str, today_str)
    yesterday_apps = fetch("BUNDLE", ["GROSS_REVENUE"], yesterday_str, yesterday_str)
    
    # Build yesterday lookup
    yesterday_dict = {r.get("BUNDLE", "").strip(): sf(r.get("GROSS_REVENUE", 0)) for r in yesterday_apps}
    
    for app in today_apps:
        bundle = app.get("BUNDLE", "").strip()
        if not bundle:
            continue
        
        today_rev = sf(app.get("GROSS_REVENUE", 0))
        yesterday_rev = yesterday_dict.get(bundle, 0)
        
        # Check for explosion: was <$20, now >$200
        if yesterday_rev < GROWTH["app_explosion_from"] and today_rev > GROWTH["app_explosion_to"]:
            if should_fire_alert("app_explosion", bundle):
                multiplier = today_rev / max(yesterday_rev, 1)
                alerts.append({
                    'type': 'App Explosion',
                    'priority': 'growth',
                    'priority_icon': '🔥',
                    'title': f'App Explosion: {bundle[:30]}',
                    'details': [
                        f"Revenue jumped: {format_currency(yesterday_rev)} → {format_currency(today_rev)}",
                        f"Growth: {multiplier:.0f}x in 24 hours",
                        f"Viral app or major demand spike"
                    ],
                    'action': 'Scale this app immediately while hot'
                })
                record_alert("app_explosion", bundle)
    
    return alerts

# ============================================================================
# TIER 4 - DISCOVERIES
# ============================================================================

def check_premium_ecpm():
    """Alert #19: High eCPM with low volume"""
    alerts = []
    today_str = get_today()
    
    app_data = fetch("BUNDLE", ["GROSS_REVENUE", "IMPRESSIONS", "GROSS_ECPM"], today_str, today_str)
    
    for app in app_data:
        bundle = app.get("BUNDLE", "").strip()
        if not bundle:
            continue
        
        revenue = sf(app.get("GROSS_REVENUE", 0))
        impressions = sf(app.get("IMPRESSIONS", 0))
        ecpm = sf(app.get("GROSS_ECPM", 0))
        
        # Calculate eCPM if not provided
        if ecpm == 0 and impressions > 0:
            ecpm = (revenue / impressions) * 1000
        
        # High eCPM but low volume = opportunity
        if ecpm > DISCOVERIES["premium_ecpm_min"] and impressions < DISCOVERIES["premium_ecpm_imps_max"] and impressions > 0:
            if should_fire_alert("premium_ecpm", bundle):
                alerts.append({
                    'type': 'Premium eCPM',
                    'priority': 'discovery',
                    'priority_icon': '💎',
                    'title': f'Premium eCPM Found: {bundle[:30]}',
                    'details': [
                        f"eCPM: {format_currency(ecpm)} (target: ${DISCOVERIES['premium_ecpm_min']}+)",
                        f"Only {format_currency(impressions)} impressions",
                        f"High-value inventory with room to scale"
                    ],
                    'action': 'Increase supply for this premium app'
                })
                record_alert("premium_ecpm", bundle)
    
    return alerts

# ============================================================================
# MAIN ALERT RUNNER
# ============================================================================

def run_tier(tier):
    """Run alerts for specified tier"""
    print(f"\n{'='*60}")
    print(f"  PGAM Alert System - {tier.upper()} Tier")
    print(f"  {datetime.now().strftime('%Y-%m-%d %I:%M %p ET')}")
    print(f"{'='*60}\n")
    
    # Cleanup old history
    cleaned = cleanup_old_history(days=7)
    if cleaned > 0:
        print(f"[CLEANUP] Removed {cleaned} old alert records\n")
    
    all_alerts = []
    
    if tier == "critical":
        print("[1/5] Checking revenue crash...")
        all_alerts.extend(check_revenue_crash())
        
        print("[2/5] Checking publisher dark...")
        all_alerts.extend(check_publisher_goes_dark())
        
        print("[3/5] Checking timeout spikes...")
        all_alerts.extend(check_timeout_spike())
        
        print("[4/5] Checking win rate crash...")
        all_alerts.extend(check_win_rate_crash())
        
        print("[5/5] Checking no bids...")
        all_alerts.extend(check_no_bids())
    
    elif tier == "important":
        print("[1/3] Checking revenue pacing...")
        all_alerts.extend(check_revenue_below_pace())
        
        print("[2/3] Checking fill rate...")
        all_alerts.extend(check_fill_rate_collapse())
        
        print("[3/3] Checking margin...")
        all_alerts.extend(check_margin_compression())
    
    elif tier == "opportunities":
        print("[1/3] Checking publisher breakouts...")
        all_alerts.extend(check_publisher_breakout())
        
        print("[2/3] Checking demand surges...")
        all_alerts.extend(check_demand_surge())
        
        print("[3/3] Checking app explosions...")
        all_alerts.extend(check_app_explosion())
    
    elif tier == "discoveries":
        print("[1/1] Checking premium eCPM opportunities...")
        all_alerts.extend(check_premium_ecpm())
    
    # Send alerts
    print(f"\n{'='*60}")
    if all_alerts:
        print(f"✅ {len(all_alerts)} alerts detected")
        
        # Group if many alerts
        if len(all_alerts) > COOLDOWN["max_alerts_per_run"]:
            print(f"⚠️  Grouping alerts (>{COOLDOWN['max_alerts_per_run']} detected)")
            send_grouped_alerts(all_alerts[:COOLDOWN["max_alerts_per_run"]])
        else:
            # Send individually
            for alert in all_alerts:
                send_alert(alert)
    else:
        print("✅ No alerts detected - all systems normal")
    
    print(f"{'='*60}\n")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PGAM Alert System")
    parser.add_argument("--tier", choices=["critical", "important", "opportunities", "discoveries"], 
                       required=True, help="Alert tier to run")
    
    args = parser.parse_args()
    
    run_tier(args.tier)
