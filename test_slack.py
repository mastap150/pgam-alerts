"""
Simple test to verify Slack webhook is working
This will ALWAYS send an alert regardless of data
"""

import os
import json
from urllib import request

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

if not SLACK_WEBHOOK:
    print("❌ ERROR: SLACK_WEBHOOK environment variable not set!")
    print("Set it in Render: Environment → Add → SLACK_WEBHOOK")
    exit(1)

print(f"Testing Slack webhook...")
print(f"Webhook: {SLACK_WEBHOOK[:50]}...")

message = {
    "text": """🧪 *TEST ALERT* — PGAM Alert System

This is a test message to verify Slack integration is working.

If you see this message in Slack, your alert system is configured correctly! ✅

Next steps:
• Wait for real alerts to fire based on thresholds
• Or lower thresholds temporarily to trigger test alerts
• Check Render logs to see what conditions are being checked
"""
}

try:
    data = json.dumps(message).encode('utf-8')
    req = request.Request(
        SLACK_WEBHOOK,
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    
    with request.urlopen(req, timeout=10) as response:
        if response.status == 200:
            print("✅ SUCCESS! Test alert sent to Slack")
            print("Check your Slack channel now!")
        else:
            print(f"❌ FAILED: HTTP {response.status}")
            
except Exception as e:
    print(f"❌ ERROR sending to Slack: {e}")
    print("Check your SLACK_WEBHOOK URL is correct")
