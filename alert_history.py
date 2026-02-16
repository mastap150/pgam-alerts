"""
Alert History Tracker
Prevents spam by tracking when alerts were last fired
"""

import json
import os
from datetime import datetime, timedelta

HISTORY_FILE = "/tmp/pgam_alert_history.json"

def load_history():
    """Load alert history from file"""
    if not os.path.exists(HISTORY_FILE):
        return {}
    try:
        with open(HISTORY_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_history(history):
    """Save alert history to file"""
    try:
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
    except Exception as e:
        print(f"[WARNING] Could not save alert history: {e}")

def generate_alert_key(alert_type, entity):
    """Generate unique key for alert tracking"""
    # e.g., "revenue_crash:Illumin Display EU"
    return f"{alert_type}:{entity}"

def should_fire_alert(alert_type, entity, cooldown_minutes=120):
    """Check if alert should fire (not in cooldown)"""
    history = load_history()
    alert_key = generate_alert_key(alert_type, entity)
    
    if alert_key not in history:
        return True
    
    last_fired = datetime.fromisoformat(history[alert_key])
    now = datetime.now()
    
    if now - last_fired > timedelta(minutes=cooldown_minutes):
        return True
    
    return False

def record_alert(alert_type, entity):
    """Record that an alert was fired"""
    history = load_history()
    alert_key = generate_alert_key(alert_type, entity)
    history[alert_key] = datetime.now().isoformat()
    save_history(history)

def cleanup_old_history(days=7):
    """Remove history older than N days"""
    history = load_history()
    now = datetime.now()
    cutoff = now - timedelta(days=days)
    
    cleaned = {}
    for key, timestamp_str in history.items():
        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp > cutoff:
            cleaned[key] = timestamp_str
    
    save_history(cleaned)
    return len(history) - len(cleaned)  # Return number cleaned

def get_recent_alerts(hours=24):
    """Get all alerts fired in last N hours"""
    history = load_history()
    now = datetime.now()
    cutoff = now - timedelta(hours=hours)
    
    recent = []
    for key, timestamp_str in history.items():
        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp > cutoff:
            alert_type, entity = key.split(':', 1)
            recent.append({
                'type': alert_type,
                'entity': entity,
                'timestamp': timestamp_str
            })
    
    return sorted(recent, key=lambda x: x['timestamp'], reverse=True)
