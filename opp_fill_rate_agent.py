"""
Opp Fill Rate Agent — PGAM Intelligence (Daily Email + Slack)
=============================================================
Opp Fill % = IMPRESSIONS / OPPORTUNITIES
Threshold: must stay ABOVE 0.05% for the month to avoid additional fees.

Per-demand partner analysis:
  - Current week fill rate vs prior week (trend)
  - Partners dragging rate down (below threshold)
  - Partners improving
  - Contextual suggestions per partner

Uses the same fetch() / api.py pattern as all other PGAM agents.
"""

from api import fetch, sf, pct
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
OPP_FILL_THRESHOLD   = 0.0005     # 0.05%
MIN_OPPS_FOR_SIGNAL  = 1_000_000  # minimum opps to include in analysis
TOP_N                = 10

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_fill(imps: float, opps: float) -> float:
    """Opp Fill % = IMPRESSIONS / OPPORTUNITIES"""
    return (imps / opps) if opps > 0 else 0.0


def fmt_pct(v: float) -> str:
    return f"{v * 100:.5f}%"


def fmt_num(v: float) -> str:
    if v >= 1_000_000_000: return f"{v/1_000_000_000:.1f}B"
    if v >= 1_000_000:     return f"{v/1_000_000:.1f}M"
    if v >= 1_000:         return f"{v/1_000:.1f}K"
    return f"{v:.0f}"


def drag_impact(row_imps: float, row_opps: float,
                total_imps: float, total_opps: float) -> float:
    new_opps = total_opps - row_opps
    new_imps = total_imps - row_imps
    return safe_fill(new_imps, new_opps) - safe_fill(total_imps, total_opps)


def status_emoji(fill: float) -> str:
    return "✅" if fill >= OPP_FILL_THRESHOLD else "🚨"


def get_suggestion(name: str, fill: float, opps: float,
                   rev: float, trend: float | None) -> str:
    if fill < OPP_FILL_THRESHOLD:
        if opps > 50_000_000_000:
            return "Extremely high opp volume with low conversion — investigate floor config vs bid price"
        elif rev == 0:
            return "Zero revenue — review integration or consider pausing"
        elif trend is not None and trend < -0.0001:
            return "Fill rate declining week-on-week — check floor price alignment"
        else:
            return "Below threshold — review floor vs avg bid price"
    else:
        if trend is not None and trend > 0.0001:
            return "Fill rate improving — monitor and consider scaling volume"
        elif trend is not None and trend < -0.0001:
            return "Fill rate declining but still above threshold — keep an eye on it"
        else:
            return "Stable and healthy — maintain current config"


# ── Diagnostic builder (legacy) ───────────────────────────────────────────────

def build_diagnostic(rows: list, dim_keys: list,
                     total_imps: float, total_opps: float) -> list:
    out = []
    for r in rows:
        opps = sf(r.get("OPPORTUNITIES", 0))
        imps = sf(r.get("IMPRESSIONS", 0))
        if opps < MIN_OPPS_FOR_SIGNAL:
            continue
        fill  = safe_fill(imps, opps)
        drag  = drag_impact(imps, opps, total_imps, total_opps)
        label = " · ".join(str(r.get(k, "?")) for k in dim_keys)
        out.append({
            "label":           label,
            "opps":            opps,
            "imps":            imps,
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
BLUE     = "#2563EB"


def _html_partner_table(partners: list, mtd_fill: float) -> str:
    if not partners:
        return ""

    rows_html = ""
    for p in partners[:TOP_N]:
        fc      = RED if p["below_threshold"] else GREEN
        row_bg  = "#fff5f5" if p["below_threshold"] else "#f1f8e9" if (p.get("trend") or 0) > 0.0001 else "#ffffff"
        new_r   = mtd_fill + p["drag_delta"]
        new_col = GREEN if new_r >= OPP_FILL_THRESHOLD else ORANGE
        flg     = " 🚨" if p["below_threshold"] else ""

        # Trend indicator
        trend = p.get("trend")
        if trend is None:
            trend_str = "—"
            trend_col = GRAY_600
        elif trend > 0.0001:
            trend_str = f"▲ +{fmt_pct(trend)}"
            trend_col = GREEN
        elif trend < -0.0001:
            trend_str = f"▼ {fmt_pct(trend)}"
            trend_col = RED
        else:
            trend_str = "→ Stable"
            trend_col = GRAY_600

        rows_html += f"""
        <tr style="background:{row_bg};border-bottom:1px solid {GRAY_100};">
          <td style="padding:8px;font-size:11px;font-weight:700;color:{NAVY};">{p['name']}{flg}</td>
          <td style="padding:8px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(p['opps'])}</td>
          <td style="padding:8px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(p['imps'])}</td>
          <td style="padding:8px;text-align:right;font-size:12px;font-weight:700;color:{fc};">{fmt_pct(p['fill_rate'])}</td>
          <td style="padding:8px;text-align:right;font-size:11px;font-weight:600;color:{trend_col};">{trend_str}</td>
          <td style="padding:8px;text-align:right;font-size:11px;font-weight:600;color:{new_col};">{fmt_pct(new_r)}</td>
        </tr>
        <tr style="background:{row_bg};border-bottom:2px solid {GRAY_100};">
          <td colspan="6" style="padding:4px 8px 10px 8px;font-size:10px;color:#555;font-style:italic;">
            💡 {p['suggestion']}
          </td>
        </tr>"""

    thead = "".join(
        f'<th style="padding:8px;text-align:{"left" if i==0 else "right"};color:white;'
        f'font-size:10px;font-weight:700;text-transform:uppercase;">{h}</th>'
        for i, h in enumerate(["Demand Partner", "Opportunities", "Impressions",
                                "Fill Rate", "vs Prior Week", "If Removed →"])
    )

    return f"""
    <table style="border-collapse:collapse;width:100%;font-size:12px;">
      <thead><tr style="background:{NAVY};">{thead}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>"""


def _html_diag_table(title: str, icon: str, rows: list, mtd_fill: float) -> str:
    if not rows:
        return ""
    thead = "".join(
        f'<th style="padding:8px;text-align:{"left" if i==0 else "right"};color:white;'
        f'font-size:10px;font-weight:700;text-transform:uppercase;">{h}</th>'
        for i, h in enumerate(["Dimension", "Opportunities", "Impressions", "Fill Rate", "If Removed →"])
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
          <td style="padding:8px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['imps'])}</td>
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
    metrics     = ["OPPORTUNITIES", "IMPRESSIONS", "GROSS_REVENUE"]

    # 1. MTD daily rows
    mtd_raw  = fetch("DATE", metrics, month_start, today_str)
    mtd_opps = sum(sf(r.get("OPPORTUNITIES", 0)) for r in mtd_raw)
    mtd_imps = sum(sf(r.get("IMPRESSIONS", 0)) for r in mtd_raw)
    mtd_rev  = sum(sf(r.get("GROSS_REVENUE", 0)) for r in mtd_raw)
    mtd_fill = safe_fill(mtd_imps, mtd_opps)
    alert    = mtd_fill < OPP_FILL_THRESHOLD

    # 2. Daily breakdown
    daily_rows = []
    for r in sorted(mtd_raw, key=lambda x: x.get("DATE", ""), reverse=True):
        opps = sf(r.get("OPPORTUNITIES", 0))
        imps = sf(r.get("IMPRESSIONS", 0))
        fill = safe_fill(imps, opps)
        daily_rows.append({
            "date":            r.get("DATE", ""),
            "opps":            opps,
            "imps":            imps,
            "fill_rate":       fill,
            "below_threshold": fill < OPP_FILL_THRESHOLD,
        })

    # 3. Per-demand partner analysis — current week vs prior week
    partner_analysis = []
    cur_week_start  = (today - timedelta(days=6)).strftime("%Y-%m-%d")
    prev_week_start = (today - timedelta(days=13)).strftime("%Y-%m-%d")
    prev_week_end   = (today - timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        cur_rows  = fetch("DEMAND_PARTNER_NAME", metrics, cur_week_start, today_str)
        prev_rows = fetch("DEMAND_PARTNER_NAME", metrics, prev_week_start, prev_week_end)

        prev_map = {}
        for r in prev_rows:
            name = r.get("DEMAND_PARTNER_NAME", "").strip()
            if not name: continue
            opps = sf(r.get("OPPORTUNITIES", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            prev_map[name] = safe_fill(imps, opps)

        for r in cur_rows:
            name = r.get("DEMAND_PARTNER_NAME", "").strip()
            if not name: continue
            opps = sf(r.get("OPPORTUNITIES", 0))
            imps = sf(r.get("IMPRESSIONS", 0))
            rev  = sf(r.get("GROSS_REVENUE", 0))
            if opps < MIN_OPPS_FOR_SIGNAL: continue
            fill      = safe_fill(imps, opps)
            drag      = drag_impact(imps, opps, mtd_imps, mtd_opps)
            prev_fill = prev_map.get(name)
            trend     = (fill - prev_fill) if prev_fill is not None else None

            partner_analysis.append({
                "name":            name,
                "opps":            opps,
                "imps":            imps,
                "fill_rate":       fill,
                "prev_fill":       prev_fill,
                "trend":           trend,
                "revenue":         rev,
                "drag_delta":      drag,
                "below_threshold": fill < OPP_FILL_THRESHOLD,
                "suggestion":      get_suggestion(name, fill, opps, rev, trend),
            })

        partner_analysis.sort(key=lambda x: (not x["below_threshold"], x["drag_delta"]))

    except Exception:
        pass

    # 4. Legacy diagnostic calls (may 500 on some API versions — skip silently)
    diag_demand = diag_pub_demand = diag_bun_demand = []
    diag_metrics = ["OPPORTUNITIES", "IMPRESSIONS"]
    try:
        raw_d = fetch("DEMAND_ID", diag_metrics, month_start, today_str)
        diag_demand = build_diagnostic(raw_d, ["DEMAND_ID"], mtd_imps, mtd_opps)
    except Exception:
        pass
    try:
        raw_pd = fetch("PUBLISHER,DEMAND_ID", diag_metrics, month_start, today_str)
        diag_pub_demand = build_diagnostic(
            raw_pd, ["PUBLISHER_NAME", "DEMAND_ID"], mtd_imps, mtd_opps)
    except Exception:
        pass
    try:
        raw_bd = fetch("BUNDLE,DEMAND_ID", diag_metrics, month_start, today_str)
        diag_bun_demand = build_diagnostic(
            raw_bd, ["BUNDLE", "DEMAND_ID"], mtd_imps, mtd_opps)
    except Exception:
        pass

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
          <td style="padding:6px 10px;text-align:right;font-size:11px;color:{GRAY_600};">{fmt_num(r['imps'])}</td>
          <td style="padding:6px 10px;text-align:right;font-size:12px;font-weight:700;color:{fc};">{fmt_pct(r['fill_rate'])}{flg}</td>
        </tr>"""

    # Partner analysis HTML
    partner_html = ""
    if partner_analysis:
        dragging   = [p for p in partner_analysis if p["below_threshold"]]
        improving  = [p for p in partner_analysis if not p["below_threshold"]
                      and p.get("trend") is not None and p["trend"] > 0.0001]

        if dragging:
            partner_html += f"""
            <div style="margin-top:28px;">
              <div style="font-size:15px;font-weight:800;color:{RED};border-bottom:2px solid {RED};
                          padding-bottom:6px;margin-bottom:12px;">
                🚨 Partners Dragging Fill Rate Down
              </div>
              <p style="font-size:12px;color:{GRAY_600};margin:0 0 12px 0;">
                "If Removed →" shows estimated MTD fill rate without that partner.
                vs Prior Week compares last 7 days vs the 7 days before.
              </p>
              {_html_partner_table(dragging, mtd_fill)}
            </div>"""

        if improving:
            partner_html += f"""
            <div style="margin-top:24px;">
              <div style="font-size:15px;font-weight:800;color:{GREEN};border-bottom:2px solid {GREEN};
                          padding-bottom:6px;margin-bottom:12px;">
                📈 Partners Improving Week-on-Week
              </div>
              {_html_partner_table(improving, mtd_fill)}
            </div>"""

    diag_html = ""
    if alert and (diag_demand or diag_pub_demand or diag_bun_demand):
        diag_html = f"""
        <div style="margin-top:28px;">
          <div style="font-size:15px;font-weight:800;color:{NAVY};border-bottom:2px solid {RED};
                      padding-bottom:6px;margin-bottom:16px;">
            🔬 Additional Breakdown
          </div>
          {_html_diag_table("By Demand ID", "📡", diag_demand, mtd_fill)}
          {_html_diag_table("By Publisher × Demand ID", "🤝", diag_pub_demand, mtd_fill)}
          {_html_diag_table("By Bundle × Demand ID", "📦", diag_bun_demand, mtd_fill)}
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
    Threshold: ≥ 0.05% (IMPRESSIONS ÷ OPPORTUNITIES) — must hold for full month to avoid fee.
  </p>
  {alert_banner}
  <div style="background:{mtd_bg};border:1px solid {mtd_border};border-radius:8px;
              padding:16px 20px;margin-bottom:20px;display:inline-block;min-width:320px;">
    <div style="font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px;">
      MTD Opp Fill Rate ({month_start} → {today_str})
    </div>
    <div style="font-size:32px;font-weight:800;color:{mtd_color};margin:6px 0;">{fmt_pct(mtd_fill)}</div>
    <div style="font-size:12px;color:#555;">
      {status_emoji(mtd_fill)} {fmt_num(mtd_imps)} impressions &nbsp;/&nbsp;
      {fmt_num(mtd_opps)} opportunities
    </div>
  </div>
  <div style="margin-top:4px;margin-bottom:8px;">
    <div style="font-size:14px;font-weight:700;color:{NAVY};margin-bottom:8px;">Daily Breakdown</div>
    <table style="border-collapse:collapse;width:100%;font-size:12px;">
      <thead>
        <tr style="background:{NAVY};color:white;">
          <th style="padding:8px 10px;text-align:left;">Date</th>
          <th style="padding:8px 10px;text-align:right;">Opportunities</th>
          <th style="padding:8px 10px;text-align:right;">Impressions</th>
          <th style="padding:8px 10px;text-align:right;">Fill Rate</th>
        </tr>
      </thead>
      <tbody>{daily_html}</tbody>
    </table>
  </div>
  {partner_html}
  {diag_html}
</div>"""

    return {
        "html":             html,
        "mtd_fill_rate":    mtd_fill,
        "mtd_opps":         mtd_opps,
        "mtd_imps":         mtd_imps,
        "daily_rows":       daily_rows,
        "partner_analysis": partner_analysis,
        "diag_demand":      diag_demand,
        "diag_pub_demand":  diag_pub_demand,
        "diag_bun_demand":  diag_bun_demand,
        "alert":            alert,
    }


if __name__ == "__main__":
    result = run_opp_fill_rate_agent()
    print(f"\nMTD Fill Rate : {fmt_pct(result['mtd_fill_rate'])}")
    print(f"Alert         : {'YES 🚨' if result['alert'] else 'No ✅'}")
    print(f"MTD Opps      : {fmt_num(result['mtd_opps'])}")
    print(f"MTD Imps      : {fmt_num(result['mtd_imps'])}")
    if result["partner_analysis"]:
        print(f"\nPartner Analysis ({len(result['partner_analysis'])} partners):")
        for p in result["partner_analysis"][:5]:
            trend_str = f"{fmt_pct(p['trend'])}" if p["trend"] is not None else "N/A"
            print(f"  {'🚨' if p['below_threshold'] else '✅'} {p['name']:25s} "
                  f"fill={fmt_pct(p['fill_rate'])}  trend={trend_str}")
            print(f"     💡 {p['suggestion']}")
