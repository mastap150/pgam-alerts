"""
Opp Fill Rate Agent — PGAM Intelligence (Daily Email + Slack)
=============================================================
Monitors Opportunity Fill % using OPPORTUNITY_FILL_RATE metric directly
from the API (matches platform reporting exactly).
Threshold: must stay ABOVE 0.05% for the month to avoid additional fees.

Uses the same fetch() / api.py pattern as all other PGAM agents.

Outputs:
  1. MTD Aggregated Opp Fill % (weighted average of daily rates)
  2. Daily trend (current month)
  3. Diagnostic breakdown (only when below threshold):
       - By DEMAND_ID
       - By PUBLISHER + DEMAND_ID
       - By BUNDLE + DEMAND_ID
"""

from api import fetch, sf, pct
from datetime import date

# ── Config ────────────────────────────────────────────────────────────────────
OPP_FILL_THRESHOLD   = 0.0005   # 0.05%
MIN_OPPS_FOR_SIGNAL  = 1000     # ignore combos with fewer opportunities
TOP_N                = 10       # rows per diagnostic table

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_fill(wins: float, opps: float) -> float:
    return (wins / opps) if opps > 0 else 0.0


def fmt_pct(v: float) -> str:
    return f"{v * 100:.5f}%"


def fmt_num(v: float) -> str:
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"{v/1_000:.1f}K"
    return f"{v:.0f}"


def drag_impact(row_opps: float, row_wins: float,
                total_opps: float, total_wins: float) -> float:
    new_opps = total_opps - row_opps
    new_wins = total_wins - row_wins
    return safe_fill(new_wins, new_opps) - safe_fill(total_wins, total_opps)


def status_emoji(fill: float) -> str:
    return "✅" if fill >= OPP_FILL_THRESHOLD else "🚨"


# ── Diagnostic builder ────────────────────────────────────────────────────────

def build_diagnostic(rows: list, dim_keys: list,
                     total_opps: float, total_wins: float) -> list:
    out = []
    for r in rows:
        opps = sf(r.get("OPPORTUNITIES", 0))
        wins = sf(r.get("WINS", 0))
        if opps < MIN_OPPS_FOR_SIGNAL:
            continue
        fill  = safe_fill(wins, opps)
        drag  = drag_impact(opps, wins, total_opps, total_wins)
        label = " · ".join(str(r.get(k, "?")) for k in dim_keys)
        out.append({
            "label":           label,
            "opps":            opps,
            "wins":            wins,
            "fill_rate":       fill,
            "drag_delta":      drag,
            "below_threshold": fill < OPP_FILL_THRESHOLD,
        })
    out.sort(key=lambda x: (not x["below_threshold"], x["drag_delta"]))
    return out


# ── HTML helpers ──────────────────────────────────────────────────────────────

NAVY     = "#0F1521"
GRAY_100 = "#F3F4F6"
GRAY_600 = "#4B5563"
GREEN    = "#16A34A"
RED      = "#DC2626"
ORANGE   = "#F97316"


def _html_diag_table(title: str, icon: str, rows: list, mtd_fill: float) -> str:
    if not rows:
        return ""
    thead = "".join(
        f'<th style="padding:8px;text-align:{"left" if i==0 else "right"};color:white;'
        f'font-size:10px;font-weight:700;text-transform:uppercase;">{h}</th>'
        for i, h in enumerate(["Dimension", "Opportunities", "Wins", "Fill Rate", "If Removed →"])
    )
    tbody = ""
    for r in rows[:TOP_N]:
        fc      = RED if r["below_threshold"] else GREEN
        row_bg  = "#fff5f5" if r["below_threshold"] else "#ffffff"
        new_r   = mtd_fill + r["drag_delta"]
        new_col = GREEN if new_r >= OPP_FILL_THRESHOLD else ORANGE
        flg     = " 🚨" if r["below_threshold"] else ""
        tbody += f"""
        <tr style="background:{row_bg};border-bottom:1px solid {GRAY_100};">
          <td style="padding:8px;font-size:11px;font-weight:600;color:{NAVY};word-break:break-all;">{r['label']}{flg}</td>
          <td style="padding:8px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['opps'])}</td>
          <td style="padding:8px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['wins'])}</td>
          <td style="padding:8px;text-align:right;font-size:12px;font-weight:700;color:{fc};">{fmt_pct(r['fill_rate'])}</td>
          <td style="padding:8px;text-align:right;font-size:11px;font-weight:600;color:{new_col};">{fmt_pct(new_r)}</td>
        </tr>"""
    return f"""
    <div style="margin-bottom:24px;">
      <div style="font-size:14px;font-weight:700;color:{NAVY};margin-bottom:8px;">{icon} {title}</div>
      <table style="border-collapse:collapse;width:100%;font-size:12px;">
        <thead><tr style="background:{NAVY};">{thead}</tr></thead>
        <tbody>{tbody}</tbody>
      </table>
    </div>"""


# ── Core Agent ────────────────────────────────────────────────────────────────

def run_opp_fill_rate_agent() -> dict:
    today       = date.today()
    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    today_str   = today.strftime("%Y-%m-%d")

    # Use OPPORTUNITY_FILL_RATE directly from API (matches platform exactly)
    # plus OPPORTUNITIES and WINS for diagnostic breakdown
    metrics = ["OPPORTUNITY_FILL_RATE", "OPPORTUNITIES", "WINS", "IMPRESSIONS"]

    # 1. MTD daily rows
    mtd_raw  = fetch("DATE", metrics, month_start, today_str)

    # MTD fill = weighted average using total wins / total opps
    mtd_opps = sum(sf(r.get("OPPORTUNITIES", 0)) for r in mtd_raw)
    mtd_wins = sum(sf(r.get("WINS", 0)) for r in mtd_raw)
    mtd_imps = sum(sf(r.get("IMPRESSIONS", 0)) for r in mtd_raw)
    mtd_fill = safe_fill(mtd_wins, mtd_opps)  # weighted average across month
    alert    = mtd_fill < OPP_FILL_THRESHOLD

    # 2. Daily breakdown — use OPPORTUNITY_FILL_RATE per day for accuracy
    daily_rows = []
    for r in sorted(mtd_raw, key=lambda x: x.get("DATE", ""), reverse=True):
        # Use the direct metric per day if available, else compute
        day_rate_raw = sf(r.get("OPPORTUNITY_FILL_RATE", 0))
        opps         = sf(r.get("OPPORTUNITIES", 0))
        wins         = sf(r.get("WINS", 0))
        # OPPORTUNITY_FILL_RATE from API is a percentage (e.g. 0.05 = 0.05%)
        # convert to decimal for consistent comparison
        day_fill = day_rate_raw / 100 if day_rate_raw > 0 else safe_fill(wins, opps)
        daily_rows.append({
            "date":            r.get("DATE", ""),
            "opps":            int(opps),
            "wins":            int(wins),
            "impressions":     int(sf(r.get("IMPRESSIONS", 0))),
            "fill_rate":       day_fill,
            "below_threshold": day_fill < OPP_FILL_THRESHOLD,
        })

    # 3. Diagnostics — only when below threshold
    diag_demand     = []
    diag_pub_demand = []
    diag_bun_demand = []
    diag_metrics    = ["OPPORTUNITIES", "WINS", "IMPRESSIONS"]

    if alert:
        raw_d = fetch("DEMAND_ID", diag_metrics, month_start, today_str)
        diag_demand = build_diagnostic(raw_d, ["DEMAND_ID"], mtd_opps, mtd_wins)

        raw_pd = fetch("PUBLISHER,DEMAND_ID", diag_metrics, month_start, today_str)
        diag_pub_demand = build_diagnostic(
            raw_pd, ["PUBLISHER_NAME", "DEMAND_ID"], mtd_opps, mtd_wins
        )

        raw_bd = fetch("BUNDLE,DEMAND_ID", diag_metrics, month_start, today_str)
        diag_bun_demand = build_diagnostic(
            raw_bd, ["BUNDLE", "DEMAND_ID"], mtd_opps, mtd_wins
        )

    # ── Build HTML ────────────────────────────────────────────────────────────
    mtd_color  = RED if alert else GREEN
    mtd_bg     = "#fff3f3" if alert else "#f1f8e9"
    mtd_border = RED if alert else GREEN

    alert_banner = ""
    if alert:
        alert_banner = f"""
        <div style="background:{RED};color:#fff;padding:10px 16px;border-radius:6px;
                    margin-bottom:16px;font-weight:700;font-size:13px;">
          ⚠️ MTD Opp Fill Rate is BELOW the 0.05% threshold — additional fees may apply.
        </div>"""

    daily_html = ""
    for r in daily_rows:
        bg  = "#fff3f3" if r["below_threshold"] else "#ffffff"
        fc  = RED if r["below_threshold"] else GREEN
        flg = " 🚨" if r["below_threshold"] else ""
        daily_html += f"""
        <tr style="background:{bg};border-bottom:1px solid {GRAY_100};">
          <td style="padding:6px 10px;font-size:12px;color:{NAVY};">{r['date']}</td>
          <td style="padding:6px 10px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['opps'])}</td>
          <td style="padding:6px 10px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['wins'])}</td>
          <td style="padding:6px 10px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['impressions'])}</td>
          <td style="padding:6px 10px;text-align:right;font-size:12px;font-weight:700;color:{fc};">{fmt_pct(r['fill_rate'])}{flg}</td>
        </tr>"""

    diag_html = ""
    if alert:
        diag_html = f"""
        <div style="margin-top:28px;">
          <div style="font-size:15px;font-weight:800;color:{NAVY};border-bottom:2px solid {RED};
                      padding-bottom:6px;margin-bottom:16px;">
            🔬 Diagnostic Breakdown — What's Dragging Fill Rate Down
          </div>
          <p style="font-size:12px;color:{GRAY_600};margin:0 0 20px 0;">
            Combos with ≥{fmt_num(MIN_OPPS_FOR_SIGNAL)} opportunities only.
            <strong>"If Removed →"</strong> shows what MTD fill rate would be without that combo.
            Sorted worst offenders first.
          </p>
          {_html_diag_table("By Demand Partner", "📡", diag_demand, mtd_fill)}
          {_html_diag_table("By Publisher × Demand Partner", "🤝", diag_pub_demand, mtd_fill)}
          {_html_diag_table("By Bundle × Demand Partner", "📦", diag_bun_demand, mtd_fill)}
        </div>"""

    html = f"""
<!-- ═══════════════════════════════════════════════════════ -->
<!-- OPP FILL RATE AGENT                                     -->
<!-- ═══════════════════════════════════════════════════════ -->
<div style="font-family:Arial,sans-serif;margin:24px 0;">
  <h2 style="font-size:18px;font-weight:800;color:{NAVY};border-bottom:2px solid {NAVY};
             padding-bottom:6px;margin-bottom:6px;">
    📊 Opportunity Fill Rate Monitor
  </h2>
  <p style="font-size:12px;color:#666;margin:0 0 16px 0;">
    Threshold: ≥ 0.05% (WINS ÷ OPPORTUNITIES) — must hold for full month to avoid fee.
  </p>
  {alert_banner}
  <div style="background:{mtd_bg};border:1px solid {mtd_border};border-radius:8px;
              padding:16px 20px;margin-bottom:20px;display:inline-block;min-width:320px;">
    <div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px;">
      MTD Opp Fill Rate ({month_start} → {today_str})
    </div>
    <div style="font-size:32px;font-weight:800;color:{mtd_color};margin:6px 0;">{fmt_pct(mtd_fill)}</div>
    <div style="font-size:12px;color:#555;">
      {status_emoji(mtd_fill)} {fmt_num(mtd_wins)} wins &nbsp;/&nbsp;
      {fmt_num(mtd_opps)} opportunities &nbsp;|&nbsp; {fmt_num(mtd_imps)} impressions
    </div>
  </div>
  <div style="margin-top:4px;margin-bottom:8px;">
    <div style="font-size:14px;font-weight:700;color:{NAVY};margin-bottom:8px;">Daily Breakdown</div>
    <table style="border-collapse:collapse;width:100%;font-size:12px;">
      <thead>
        <tr style="background:{NAVY};color:white;">
          <th style="padding:8px 10px;text-align:left;">Date</th>
          <th style="padding:8px 10px;text-align:right;">Opportunities</th>
          <th style="padding:8px 10px;text-align:right;">Wins</th>
          <th style="padding:8px 10px;text-align:right;">Impressions</th>
          <th style="padding:8px 10px;text-align:right;">Fill Rate</th>
        </tr>
      </thead>
      <tbody>{daily_html}</tbody>
    </table>
  </div>
  {diag_html}
</div>"""

    return {
        "html":            html,
        "mtd_fill_rate":   mtd_fill,
        "mtd_opps":        int(mtd_opps),
        "mtd_wins":        int(mtd_wins),
        "daily_rows":      daily_rows,
        "diag_demand":     diag_demand,
        "diag_pub_demand": diag_pub_demand,
        "diag_bun_demand": diag_bun_demand,
        "alert":           alert,
    }


if __name__ == "__main__":
    result = run_opp_fill_rate_agent()
    print(f"\nMTD Fill Rate : {fmt_pct(result['mtd_fill_rate'])}")
    print(f"Alert         : {'YES 🚨' if result['alert'] else 'No ✅'}")
    print(f"MTD Opps      : {fmt_num(result['mtd_opps'])}")
    print(f"MTD Wins      : {fmt_num(result['mtd_wins'])}")
