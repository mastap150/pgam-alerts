"""
PGAM Intelligence v6 Configuration
"""
import os

# API Configuration
API_BASE_URL = "http://stats.ortb.net/v1/stats"
CLIENT_KEY = "pgam"
SECRET_KEY = "202730f50d518bb0594096848cd2c67a"

# SendGrid API Key (from environment)
SENDGRID_KEY = os.environ.get('SENDGRID_KEY', '')

# Email Recipients (from environment or defaults)
EMAIL_TO_ENV = os.environ.get('EMAIL_TO', '')
if EMAIL_TO_ENV:
    RECIPIENTS = [email.strip() for email in EMAIL_TO_ENV.split(',')]
else:
    RECIPIENTS = [
        "ppatel@pgammedia.com",
        "sagar@pgammedia.com",
        "vivek@pgammedia.com",
        "bgoldberg@pgammedia.com"
    ]

# Email Settings (from environment or defaults)
SENDER_EMAIL = os.environ.get('EMAIL_FROM', 'reports@pgammedia.com')
SENDER_NAME = "PGAM Intelligence"

# Slack Webhook (from environment)
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK', '')

# Report Settings
REPORT_TIME = "05:00"  # 5 AM PT / 10 AM ET
TIMEZONE = "America/New_York"

# Color Constants
GREEN = "#10b981"
RED = "#ef4444"
YELLOW = "#f59e0b"
