"""
Stromify KPI Monitor - Dashboard
Streamlit-Frontend zur Visualisierung der Unternehmens-KPIs.
"""
import threading
import logging
import streamlit as st
import pandas as pd
import schedule
import time
from datetime import datetime, timedelta

import config
from data_loader import load_daily_kpis, load_monthly_kpis, load_targets, load_active_leads, is_using_dummy_data
import charts

logger = logging.getLogger(__name__)


def _run_cronjob():
    """Führt den KPI-Fetch im Hintergrund aus."""
    try:
        from cronjob.main import run_fetch
        run_fetch()
        logger.info("✅ Hintergrund-Cronjob abgeschlossen")
    except Exception as e:
        logger.error(f"❌ Hintergrund-Cronjob Fehler: {e}")


def _scheduler_loop():
    """Läuft dauerhaft im Background-Thread und führt den Schedule aus."""
    schedule.every().day.at("21:00").do(_run_cronjob)
    while True:
        schedule.run_pending()
        time.sleep(60)


# Scheduler einmalig starten (nicht bei jedem Streamlit-Rerun)
if "scheduler_started" not in st.session_state:
    st.session_state["scheduler_started"] = True
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()
    logger.info("🕐 Hintergrund-Scheduler gestartet (täglich 21:00)")

# --- Page Config ---
st.set_page_config(
    page_title="Stromify KPI Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --- Custom CSS ---
st.markdown("""
<style>
    .kpi-card {
        background: linear-gradient(135deg, #1A1F2E 0%, #252B3B 100%);
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid #FF6B35;
        margin-bottom: 10px;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #FAFAFA;
        margin: 0;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #8899AA;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .kpi-delta-positive { color: #2ECC71; font-size: 0.9rem; }
    .kpi-delta-negative { color: #E74C3C; font-size: 0.9rem; }
    .section-header {
        font-size: 1.1rem;
        color: #8899AA;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
        border-bottom: 1px solid #2A3040;
        padding-bottom: 0.5rem;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1A1F2E 0%, #252B3B 100%);
        border-radius: 12px;
        padding: 15px 20px;
        border-left: 4px solid #FF6B35;
    }
    .target-progress {
        background: #1A1F2E;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .progress-bar-bg {
        background: #2A3040;
        border-radius: 4px;
        height: 12px;
        overflow: hidden;
    }
    .progress-bar-fill {
        height: 100%;
        border-radius: 4px;
        transition: width 0.5s ease;
    }
    /* Sidebar ausblenden - Navigation ist im Content */
    [data-testid="stSidebar"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# Shared Data & Sidebar
# ============================================================

def load_all_data():
    """Lädt alle Daten einmalig."""
    daily_df = load_daily_kpis()
    monthly_df = load_monthly_kpis()
    targets_df = load_targets()
    return daily_df, monthly_df, targets_df


def render_header_nav(active: str):
    """Rendert den Titel + horizontale Navigation."""
    st.markdown("# ⚡ Stromify KPI Monitor")

    col_nav1, col_nav2, col_nav3, _ = st.columns([1, 1, 1, 4])
    with col_nav1:
        if st.button("📊 Dashboard", use_container_width=True,
                     type="primary" if active == "dashboard" else "secondary"):
            st.session_state.page = "dashboard"
            st.rerun()
    with col_nav2:
        if st.button("🎯 Jahresziele", use_container_width=True,
                     type="primary" if active == "targets" else "secondary"):
            st.session_state.page = "targets"
            st.rerun()
    with col_nav3:
        if is_using_dummy_data():
            st.warning("📊 Demo-Modus")

    st.markdown("---")


def render_filters(daily_df: pd.DataFrame) -> int:
    """Rendert Zeitraum-Filter und Meta-Info inline."""
    col1, col2, col3, _ = st.columns([1, 1, 1, 4])
    with col1:
        time_range = st.selectbox(
            "📅 Zeitraum",
            options=["7 Tage", "14 Tage", "30 Tage", "90 Tage"],
            index=2,
        )
        days = int(time_range.split()[0])
    with col2:
        if not daily_df.empty:
            last_date = daily_df["date"].max()
            st.markdown(f"**Letztes Update:** {last_date}")
    with col3:
        st.markdown(f"**Datenpunkte:** {len(daily_df)} Tage")
    return days


# ============================================================
# Dashboard Page
# ============================================================

def calculate_delta(df: pd.DataFrame, column: str, days: int = 7) -> tuple[float, float]:
    """Berechnet den Delta-Wert (Veränderung) gegenüber der Vorperiode."""
    if len(df) < days + 1:
        return 0.0, 0.0

    current = df[column].iloc[-1]
    previous = df[column].iloc[-(days + 1)]

    if previous == 0:
        return 0.0, 0.0

    delta_abs = current - previous
    delta_pct = ((current - previous) / previous) * 100
    return delta_abs, delta_pct


def render_kpi_cards(df: pd.DataFrame, days: int):
    """Rendert die KPI-Übersichtskarten."""
    if df.empty:
        st.warning("Keine Daten verfügbar.")
        return

    # Zeitraum-gefilterte Daten für Summen-KPIs
    period_df = df.tail(days)
    # Vorperiode für Delta-Berechnung
    prev_df = df.iloc[-2 * days:-days] if len(df) >= 2 * days else pd.DataFrame()

    # Snapshot-KPIs: letzter bekannter Wert (nur Zeilen mit echten Daten)
    latest = df.iloc[-1]
    customers_df = df[df["notion_customers_total"] > 0]
    customers_val = int(customers_df["notion_customers_total"].iloc[-1]) if not customers_df.empty else 0
    gwh_df = df[df["notion_yearly_consumption_gwh"] > 0]
    gwh_val = gwh_df["notion_yearly_consumption_gwh"].iloc[-1] if not gwh_df.empty else 0.0

    # Kumulative KPIs: Summe über Zeitraum
    ga_val = int(period_df["ga_visitors"].sum())
    li_val = int(period_df["li_impressions"].sum())

    # Deltas
    ga_prev = int(prev_df["ga_visitors"].sum()) if not prev_df.empty else 0
    ga_delta = f"{((ga_val - ga_prev) / ga_prev * 100):+.1f}%" if ga_prev > 0 else None

    li_prev = int(prev_df["li_impressions"].sum()) if not prev_df.empty else 0
    li_delta = f"{((li_val - li_prev) / li_prev * 100):+.1f}%" if li_prev > 0 else None

    customers_prev_df = df[df["notion_customers_total"] > 0].iloc[:-1] if len(customers_df) > 1 else pd.DataFrame()
    customers_prev = int(customers_prev_df["notion_customers_total"].iloc[-1]) if not customers_prev_df.empty else 0
    customers_delta = f"{customers_val - customers_prev:+.0f}" if customers_prev > 0 else None

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label=f"🌐 Website Besucher ({days}d)",
            value=f"{ga_val:,}",
            delta=ga_delta,
        )

    with col2:
        st.metric(
            label="👥 Kunden Gesamt",
            value=f"{customers_val:,}",
            delta=customers_delta,
        )

    with col3:
        leads_df = load_active_leads()
        if not leads_df.empty and "status" in leads_df.columns:
            active_leads_val = int(leads_df["status"].isin(["new", "active"]).sum())
            new_leads_val = int((leads_df["status"] == "new").sum())
            active_delta = f"davon {new_leads_val} neu" if new_leads_val > 0 else None
        else:
            active_leads_val = 0
            active_delta = None
        st.metric(
            label="🔄 Aktive Leads",
            value=f"{active_leads_val:,}",
            delta=active_delta,
        )

    with col4:
        st.metric(
            label="⚡ Yearly Consumption",
            value=f"{gwh_val:.1f} GWh",
        )


def render_website_section(df: pd.DataFrame):
    """Rendert die Website-Traffic Sektion."""
    st.markdown('<p class="section-header">🌐 Website Traffic</p>', unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = charts.line_chart(
            df, x="date", y=["ga_visitors", "ga_sessions"],
            labels={"ga_visitors": "Besucher", "ga_sessions": "Sessions"},
            title="Besucher & Sessions",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = charts.area_chart(
            df, x="date", y="ga_bounce_rate",
            title="Absprungrate (%)",
            color="#E74C3C",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_sales_section(df: pd.DataFrame):
    """Rendert die Sales / Lead Pipeline Sektion."""
    st.markdown('<p class="section-header">🎯 Sales & Lead Pipeline</p>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        leads_df = load_active_leads()
        if not leads_df.empty and "status" in leads_df.columns:
            counts = leads_df["status"].value_counts()
            total = len(leads_df)
            won = int(counts.get("won", 0))
            lost = int(counts.get("lost", 0))
            waiting = int(counts.get("waiting", 0))
            active_new = int(counts.get("new", 0))
            active_old = int(counts.get("active", 0))
        else:
            zoho_df = df[df["zoho_deals_total"] > 0]
            latest = zoho_df.iloc[-1] if not zoho_df.empty else df.iloc[-1]
            total = int(latest.get("zoho_deals_total", 0))
            won = lost = waiting = active_new = active_old = 0

        stages = ["Gesamt", "Aktiv", "Aktiv (Neu)", "Warteschleife", "Gewonnen", "Verloren"]
        values = [total, active_old, active_new, waiting, won, lost]
        fig = charts.funnel_chart(stages, values, title="Lead Pipeline (aktuell)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = charts.line_chart(
            df, x="date", y=["zoho_deals_total"],
            labels={"zoho_deals_total": "Leads Gesamt"},
            title="Leads Trend",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_active_leads_section():
    """Rendert Tabellen mit aktiven und neuen Leads aus Zoho."""
    st.markdown('<p class="section-header">📋 Aktive Leads</p>', unsafe_allow_html=True)

    leads_df = load_active_leads()

    if leads_df.empty:
        st.info("Keine Lead-Daten verfügbar. Cronjob noch nicht gelaufen.")
        return

    col_labels = {
        "name": "Name",
        "company": "Unternehmen",
        "stage": "Stage",
        "amount": "Betrag (€)",
        "created_date": "Erstellt",
        "closing_date": "Abschluss",
    }

    display_cols = ["name", "company", "stage", "amount", "created_date", "closing_date"]

    new_df = leads_df[leads_df["status"] == "new"][display_cols] if "status" in leads_df.columns else pd.DataFrame()
    active_df = leads_df[leads_df["status"] == "active"][display_cols] if "status" in leads_df.columns else pd.DataFrame()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"**🆕 Neu (letzte 14 Tage)** – {len(new_df)} Leads")
        if not new_df.empty:
            st.dataframe(new_df.rename(columns=col_labels), use_container_width=True, hide_index=True)
        else:
            st.caption("Keine neuen Leads in den letzten 14 Tagen.")

    with col2:
        st.markdown(f"**🔄 Aktiv (älter)** – {len(active_df)} Leads")
        if not active_df.empty:
            st.dataframe(active_df.rename(columns=col_labels), use_container_width=True, hide_index=True)
        else:
            st.caption("Keine weiteren aktiven Leads.")


def render_linkedin_energy_section(df: pd.DataFrame):
    """Rendert LinkedIn Performance & Energy nebeneinander."""
    st.markdown('<p class="section-header">💼 LinkedIn Performance &nbsp;&nbsp;&nbsp; ⚡ Energy</p>', unsafe_allow_html=True)
    col_li1, col_li2, col_energy = st.columns(3)
    with col_li1:
        fig = charts.line_chart(
            df, x="date", y=["li_impressions"],
            labels={"li_impressions": "Impressions"},
            title="LinkedIn Impressions",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col_li2:
        fig = charts.area_chart(
            df, x="date", y="li_views",
            title="LinkedIn Views",
            color="#4ECDC4",
        )
        st.plotly_chart(fig, use_container_width=True)
    with col_energy:
        fig = charts.area_chart(
            df, x="date", y="notion_yearly_consumption_gwh",
            title="Yearly Consumption (GWh)",
            color="#FFE66D",
        )
        st.plotly_chart(fig, use_container_width=True)


def page_dashboard():
    """Hauptseite: Dashboard."""
    daily_df, monthly_df, targets_df = load_all_data()

    render_header_nav("dashboard")
    days = render_filters(daily_df)

    # Daten filtern
    if not daily_df.empty:
        cutoff_date = daily_df["date"].max() - timedelta(days=days)
        filtered_df = daily_df[daily_df["date"] >= cutoff_date].copy()
    else:
        filtered_df = daily_df

    # KPI Cards
    render_kpi_cards(filtered_df, days)
    st.markdown("---")

    # Sektionen
    render_website_section(filtered_df)
    render_sales_section(filtered_df)
    render_active_leads_section()
    render_linkedin_energy_section(filtered_df)

    # Footer
    st.markdown("---")
    st.caption(f"Stromify KPI Monitor | Letztes Update: {datetime.now().strftime('%d.%m.%Y %H:%M')}")


# ============================================================
# Jahresziele Page
# ============================================================

def _calc_ytd_value(daily_df: pd.DataFrame, kpi: str) -> float:
    """
    Berechnet den Year-to-Date Wert für ein KPI.
    - Snapshot-KPIs (customers_total, consumption, deals_total): letzter bekannter Wert
    - zoho_deals_new: Anzahl Deals aus zoho_leads die dieses Jahr erstellt wurden
    - Kumulative KPIs (visitors, impressions): Summe über das Jahr
    """
    current_year = str(datetime.now().year)

    # zoho_deals_new: aus zoho_leads zählen (Deals erstellt in diesem Jahr)
    if kpi == "zoho_deals_new":
        leads_df = load_active_leads()
        if not leads_df.empty and "created_date" in leads_df.columns:
            return float((leads_df["created_date"].str.startswith(current_year)).sum())
        return 0.0

    if daily_df.empty:
        return 0

    year_df = daily_df[pd.to_datetime(daily_df["date"]).dt.year == int(current_year)]
    if year_df.empty:
        return 0

    # Snapshot-KPIs → letzter Wert
    if kpi in ("notion_customers_total", "notion_yearly_consumption_gwh", "zoho_deals_total"):
        real_df = year_df[year_df[kpi] > 0] if kpi != "notion_yearly_consumption_gwh" else year_df[year_df[kpi] > 0]
        return float(real_df[kpi].iloc[-1]) if not real_df.empty else 0.0

    # Kumulative KPIs → Summe
    return float(year_df[kpi].sum())


def render_yearly_targets(daily_df: pd.DataFrame, targets_df: pd.DataFrame):
    """Rendert die Jahresziel-Gauges in 2 Reihen à 3."""
    if targets_df.empty:
        st.info("Keine Zielwerte konfiguriert. Trage Ziele im Google Sheet 'kpi_targets' ein.")
        return

    now = datetime.now()
    day_of_year = now.timetuple().tm_yday
    days_in_year = 366 if (now.year % 4 == 0 and (now.year % 100 != 0 or now.year % 400 == 0)) else 365
    year_progress = day_of_year / days_in_year

    st.markdown(f"**📅 Jahresfortschritt: {year_progress:.0%}** ({day_of_year}/{days_in_year} Tage)")
    st.progress(year_progress)
    st.markdown("")

    # Feste 2 Reihen: Social (3 Gauges) + Sales (3 Gauges)
    gauge_rows = [
        {
            "label": "🎯 Sales & Energy",
            "kpis": ["notion_customers_total", "zoho_deals_new", "notion_yearly_consumption_gwh"],
        },
        {
            "label": "🌐 Social & Traffic",
            "kpis": ["ga_visitors", "li_impressions", "li_views"],
        },
    ]

    for row_def in gauge_rows:
        st.markdown(f'<p class="section-header">{row_def["label"]}</p>', unsafe_allow_html=True)
        cols = st.columns(3)

        for i, kpi in enumerate(row_def["kpis"]):
            # Target aus Sheet suchen
            target_rows = targets_df[targets_df["kpi"] == kpi]
            if target_rows.empty:
                continue

            target_row = target_rows.iloc[0]
            target_val = float(target_row["target_yearly"])
            kpi_info = config.KPI_DISPLAY.get(kpi, {})
            label = kpi_info.get("label", kpi)
            icon = kpi_info.get("icon", "📊")
            unit = target_row.get("unit", "")

            # YTD-Wert berechnen
            if kpi in daily_df.columns or kpi == "zoho_deals_new":
                ytd_val = _calc_ytd_value(daily_df, kpi)
            else:
                ytd_val = 0

            with cols[i]:
                fig = charts.gauge_chart(
                    value=ytd_val,
                    target=target_val,
                    title=f"{icon} {label}",
                    suffix=unit,
                )
                st.plotly_chart(fig, use_container_width=True)

                # On-Track Status
                on_track_target = target_val * year_progress
                if ytd_val >= on_track_target:
                    status = "🟢 On Track"
                elif ytd_val >= on_track_target * 0.7:
                    status = "🟡 Leicht hinter Plan"
                else:
                    status = "🔴 Hinter Plan"

                st.caption(
                    f"{status} · "
                    f"Soll (anteilig): {on_track_target:,.0f} · "
                    f"Ist: {ytd_val:,.0f} · "
                    f"Ziel: {target_val:,.0f}"
                )


def render_monthly_breakdown(daily_df: pd.DataFrame, monthly_df: pd.DataFrame, targets_df: pd.DataFrame):
    """Rendert eine monatliche Aufschlüsselung als Balkendiagramm."""
    if targets_df.empty or monthly_df.empty:
        return

    st.markdown('<p class="section-header">📊 Monatliche Aufschlüsselung</p>', unsafe_allow_html=True)

    # KPI-Auswahl
    kpi_options = {}
    for _, row in targets_df.iterrows():
        kpi = row["kpi"]
        label = config.KPI_DISPLAY.get(kpi, {}).get("label", kpi)
        kpi_options[label] = kpi

    selected_label = st.selectbox("KPI auswählen", list(kpi_options.keys()))
    selected_kpi = kpi_options[selected_label]
    target_row = targets_df[targets_df["kpi"] == selected_kpi].iloc[0]
    monthly_target = target_row["target_yearly"] / 12

    # Mapping: daily KPI → monthly column name
    monthly_col_map = {
        "ga_visitors": "ga_visitors_sum",
        "notion_customers_total": "notion_customers_end",
        "notion_yearly_consumption_gwh": "notion_yearly_consumption_gwh",
        "zoho_deals_total": "zoho_deals_total_end",
        "li_impressions": "li_impressions_sum",
        "li_views": "li_views_sum",
    }

    monthly_col = monthly_col_map.get(selected_kpi, selected_kpi)

    if monthly_col not in monthly_df.columns:
        st.warning(f"Keine Monatsdaten für {selected_label}")
        return

    chart_df = monthly_df[["month", monthly_col]].copy()
    chart_df["target"] = monthly_target
    chart_df = chart_df.rename(columns={monthly_col: "Ist"})

    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=chart_df["month"],
        y=chart_df["Ist"],
        name="Ist",
        marker_color=charts.COLORS["primary"],
    ))
    fig.add_trace(go.Scatter(
        x=chart_df["month"],
        y=chart_df["target"],
        name=f"Monatsziel ({monthly_target:,.0f})",
        mode="lines",
        line=dict(color=charts.COLORS["text_muted"], width=2, dash="dash"),
    ))

    fig.update_layout(
        title=f"{selected_label} – Monatlich vs. Ziel (1/12 Jahresziel)",
        **charts.CHART_LAYOUT,
    )
    fig.update_layout(height=400, barmode="group")
    st.plotly_chart(fig, use_container_width=True)


def page_targets():
    """Seite: Jahresziele."""
    daily_df, monthly_df, targets_df = load_all_data()

    render_header_nav("targets")

    st.markdown("## 🎯 Jahresziele 2026")
    st.markdown("---")

    render_yearly_targets(daily_df, targets_df)

    st.markdown("---")

    render_monthly_breakdown(daily_df, monthly_df, targets_df)

    # Footer
    st.markdown("---")
    st.caption(f"Stromify KPI Monitor | Letztes Update: {datetime.now().strftime('%d.%m.%Y %H:%M')}")


# ============================================================
# Main Router
# ============================================================

def main():
    if "page" not in st.session_state:
        st.session_state.page = "dashboard"

    if st.session_state.page == "targets":
        page_targets()
    else:
        page_dashboard()


if __name__ == "__main__":
    main()
