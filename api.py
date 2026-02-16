# ============================================================
#  PGAM Intelligence Suite v2 — API + Helpers
# ============================================================
import requests
from datetime import datetime, timedelta
from config import API_BASE_URL, CLIENT_KEY, SECRET_KEY


def fetch(breakdown, metrics, start_date=None, end_date=None):
    if not start_date:
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = start_date
    params = {
        "clientKey": CLIENT_KEY, "secretKey": SECRET_KEY,
        "breakdown": breakdown, "metrics": ",".join(metrics),
        "startDate": start_date, "endDate": end_date, "output": "json",
    }
    try:
        r = requests.get(API_BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        d = r.json()
        return d.get("body", []) if d.get("status") == "SUCCESS" else []
    except Exception as e:
        print(f"[API ERROR] {e} | breakdown={breakdown}")
        return []


def yesterday():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def n_days_ago(n):
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


def sf(v):
    """Safe float — returns 0.0 for NaN/None/empty."""
    try:
        f = float(v)
        return 0.0 if f != f else f
    except: return 0.0


def pct(n, d):
    """Safe percentage: returns float 0-100."""
    return round(sf(n) / sf(d) * 100, 4) if sf(d) > 0 else 0.0


def fmt_usd(v):
    return f"${sf(v):,.2f}"


def fmt_n(v):
    v = sf(v)
    if v >= 1e9: return f"{v/1e9:.1f}B"
    if v >= 1e6: return f"{v/1e6:.1f}M"
    if v >= 1e3: return f"{v/1e3:.1f}K"
    return f"{v:,.0f}"


def fmt_pct(v):
    return f"{sf(v):.2f}%"


def arrow(v):
    if v is None: return ""
    return ("▲ " if v >= 0 else "▼ ") + f"{abs(v):.1f}%"


def arrow_color(v):
    if v is None: return "#64748B"  # DGRAY
    from config import GREEN, RED
    return GREEN if v >= 0 else RED
