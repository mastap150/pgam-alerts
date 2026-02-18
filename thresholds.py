"""
PGAM Alert Thresholds Configuration
Easily adjustable thresholds for all alert types
"""

# TIER 1 - CRITICAL ALERTS (Check Hourly)
CRITICAL = {
    "revenue_crash_pct": 30,           # % drop vs yesterday same hour
    "revenue_crash_min": 50,           # $ minimum to care about
    
    "publisher_dark_rank": 3,          # Top N publishers to monitor
    "publisher_dark_was_min": 100,     # Was making at least $X/hour
    
    "timeout_rate_max": 10,            # % max acceptable timeout rate
    "timeout_rate_normal": 5,          # % normal baseline
    
    "win_rate_drop_pct": 50,           # % drop to trigger
    "win_rate_min": 5,                 # Don't alert if win rate was <5%
    
    "no_bids_hours": 2,                # Hours with zero bids
    "no_bids_rank": 5,                 # Top N publishers to monitor
}

# TIER 2 - IMPORTANT ALERTS (Check Every 2-3 Hours)
IMPORTANT = {
    "revenue_pace_hour": 12,           # Check at noon
    "revenue_pace_pct": 60,            # Should be 60% of daily target
    
    "fill_rate_drop_pct": 40,          # % drop vs 7-day avg
    "fill_rate_min": 1,                # Don't alert if fill was <1%
    
    "margin_min": 25,                  # % minimum margin (target: 31%)
    "margin_drop_pct": 15,             # Alert if drops >15% in 3 hours
    
    "ecpm_drop_pct": 40,               # % drop vs yesterday
    "ecpm_min": 0.50,                  # Don't alert if eCPM <$0.50
    
    "anomaly_z_score": 3,              # Sigma threshold for critical
    
    "bid_request_drop_pct": 50,        # % drop vs yesterday
    "bid_request_min": 1000000,        # Minimum requests to care
}

# TIER 3 - GROWTH OPPORTUNITIES (Check Every 2-4 Hours)
GROWTH = {
    "publisher_breakout_pct": 100,     # % growth vs yesterday
    "publisher_breakout_min": 100,     # $ minimum hourly revenue
    
    "demand_surge_pct": 150,           # % growth vs yesterday
    "demand_surge_min": 200,           # $ minimum daily revenue
    
    "app_explosion_from": 20,          # $ before (daily)
    "app_explosion_to": 200,           # $ after (daily)
    "app_explosion_hours": 24,         # Within N hours
    
    "geo_surge_pct": 200,              # % growth vs yesterday
    "geo_surge_min": 100,              # $ minimum daily revenue
    
    "format_winner_pct": 80,           # % revenue growth
    "format_winner_ecpm_up": True,     # eCPM must also be improving
    
    "growth_streak_hours": 3,          # Consecutive hours of growth
    "growth_streak_min_pct": 10,      # Each hour up >10%
}

# TIER 4 - DISCOVERIES (Check Every 3-6 Hours)
DISCOVERIES = {
    "new_app_hourly_min": 50,          # $ per hour in first 6 hours
    "new_app_hours": 6,                # Within first N hours
    
    "premium_ecpm_min": 15,            # $ eCPM threshold
    "premium_ecpm_imps_max": 10000,    # Low volume = opportunity
    
    "payout_spike_multiplier": 2,      # X times normal payout
    "payout_spike_min": 100,           # $ minimum to care
    
    # NEW APP/DOMAIN ALERTS
    "new_app_alert_min": 10,           # Alert if new app made >$10 today
    "new_domain_alert_min": 10,        # Alert if new domain made >$10 today
    "new_inventory_count": 5,          # Alert if 5+ new items detected
    
    # LOST INVENTORY ALERTS
    "lost_app_was_making_min": 50,     # Was making >$50/day
    "lost_app_alert": True,            # Alert when apps disappear
    "lost_inventory_count": 3,         # Alert if 3+ items disappeared
}

# ANTI-SPAM SETTINGS
COOLDOWN = {
    "minutes": 120,                    # Don't repeat same alert within 2 hours
    "min_alert_value": 50,             # Ignore alerts <$50 involved
    "group_similar": True,             # Group similar alerts together
    "max_alerts_per_run": 10,          # Don't spam >10 alerts at once
}

# OPERATING HOURS (ET timezone)
SCHEDULE = {
    "critical_hours": range(8, 21),    # 8 AM - 8 PM ET
    "important_hours": [9, 12, 15, 18],  # 9 AM, 12 PM, 3 PM, 6 PM ET
    "opportunities_hours": [10, 14, 18],  # 10 AM, 2 PM, 6 PM ET
    "discoveries_hours": [10, 16],     # 10 AM, 4 PM ET
}

# ALERT PRIORITIES (for grouping)
PRIORITY = {
    "critical": "🔴",
    "important": "⚠️",
    "growth": "🚀",
    "discovery": "💎"
}
