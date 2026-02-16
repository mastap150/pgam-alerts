"""
Slack Alert Delivery
Send formatted alerts to Slack webhook
"""

import os
import json
from urllib import request, error
from datetime import datetime

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

def format_currency(value):
    """Format value as currency"""
    if value >= 1000:
        return f"${value/1000:.1f}K"
    return f"${value:.0f}"

def format_percentage(value):
    """Format value as percentage"""
    return f"{value:+.1f}%" if value != 0 else "0%"

def send_alert(alert_data):
    """Send a single alert to Slack"""
    if not SLACK_WEBHOOK:
        print("[ERROR] SLACK_WEBHOOK not configured")
        return False
    
    try:
        # Build Slack message
        message = build_slack_message(alert_data)
        
        payload = {"text": message}
        data = json.dumps(payload).encode('utf-8')
        
        req = request.Request(
            SLACK_WEBHOOK,
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        
        with request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                print(f"[SLACK] ✅ Sent: {alert_data['type']}")
                return True
            else:
                print(f"[SLACK] ❌ Failed: HTTP {response.status}")
                return False
                
    except error.HTTPError as e:
        print(f"[SLACK] ❌ HTTP Error: {e.code}")
        return False
    except Exception as e:
        print(f"[SLACK] ❌ Error: {e}")
        return False

def build_slack_message(alert):
    """Build formatted Slack message"""
    priority_icon = alert.get('priority_icon', '⚠️')
    alert_type = alert.get('type', 'Alert')
    timestamp = datetime.now().strftime("%I:%M %p ET")
    
    # Header
    msg = f"{priority_icon} *{alert_type.upper()}* — {timestamp}\n\n"
    
    # Title
    if 'title' in alert:
        msg += f"*{alert['title']}*\n"
    
    # Details (bullet points)
    if 'details' in alert:
        for detail in alert['details']:
            msg += f"• {detail}\n"
    
    # Action
    if 'action' in alert:
        msg += f"\n_Action: {alert['action']}_"
    
    return msg

def send_grouped_alerts(alerts):
    """Send multiple alerts grouped together"""
    if not alerts:
        return False
    
    if not SLACK_WEBHOOK:
        print("[ERROR] SLACK_WEBHOOK not configured")
        return False
    
    try:
        # Group by priority
        critical = [a for a in alerts if a.get('priority') == 'critical']
        important = [a for a in alerts if a.get('priority') == 'important']
        growth = [a for a in alerts if a.get('priority') == 'growth']
        discovery = [a for a in alerts if a.get('priority') == 'discovery']
        
        timestamp = datetime.now().strftime("%I:%M %p ET")
        msg = f"📊 *PGAM Alerts Summary* — {timestamp}\n"
        msg += f"_{len(alerts)} alerts detected_\n\n"
        
        if critical:
            msg += f"🔴 *CRITICAL ({len(critical)})*\n"
            for alert in critical[:5]:  # Max 5 critical
                msg += f"• {alert.get('title', 'Alert')}\n"
            msg += "\n"
        
        if important:
            msg += f"⚠️ *IMPORTANT ({len(important)})*\n"
            for alert in important[:3]:  # Max 3 important
                msg += f"• {alert.get('title', 'Alert')}\n"
            msg += "\n"
        
        if growth:
            msg += f"🚀 *GROWTH ({len(growth)})*\n"
            for alert in growth[:3]:  # Max 3 growth
                msg += f"• {alert.get('title', 'Alert')}\n"
            msg += "\n"
        
        if discovery:
            msg += f"💎 *DISCOVERIES ({len(discovery)})*\n"
            for alert in discovery[:2]:  # Max 2 discoveries
                msg += f"• {alert.get('title', 'Alert')}\n"
        
        payload = {"text": msg}
        data = json.dumps(payload).encode('utf-8')
        
        req = request.Request(
            SLACK_WEBHOOK,
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        
        with request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                print(f"[SLACK] ✅ Sent grouped alerts: {len(alerts)} total")
                return True
            else:
                print(f"[SLACK] ❌ Failed: HTTP {response.status}")
                return False
                
    except Exception as e:
        print(f"[SLACK] ❌ Error: {e}")
        return False
