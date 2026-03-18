"""
Stromify KPI Monitor - Data Loader
Liest KPI-Daten aus Google Sheets oder liefert Dummy-Daten als Fallback.
"""
import json
import base64
import logging
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

import config

logger = logging.getLogger(__name__)


def _parse_number(val) -> float:
    """
    Parst einen Wert zu float, unterstützt deutsches und englisches Format.
    - "5.13"      → 5.13   (englisch, Dezimalpunkt)
    - "5,13"      → 5.13   (deutsch, Dezimalkomma)
    - "1.234,56"  → 1234.56 (deutsch, 1000er-Punkt + Dezimalkomma)
    - "1,234.56"  → 1234.56 (englisch, 1000er-Komma + Dezimalpunkt)
    """
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s == "None":
        return 0.0
    has_dot = "." in s
    has_comma = "," in s
    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif has_comma and not has_dot:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _get_gspread_client():
    """Erstellt einen gspread Client mit Service Account Credentials."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

        if not config.GOOGLE_SERVICE_ACCOUNT_JSON:
            return None

        # Support both base64-encoded and raw JSON
        try:
            sa_json = base64.b64decode(config.GOOGLE_SERVICE_ACCOUNT_JSON)
            sa_info = json.loads(sa_json)
        except Exception:
            sa_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_JSON)

        credentials = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        logger.warning(f"Google Sheets Verbindung fehlgeschlagen: {e}")
        return None


def _generate_dummy_daily_data() -> pd.DataFrame:
    """Generiert realistische Dummy-Daten für die letzten 90 Tage."""
    import random

    random.seed(42)
    dates = [datetime.now().date() - timedelta(days=i) for i in range(90)]
    dates.reverse()

    rows = []
    base_visitors = 250
    base_customers = 72
    base_gwh = 1.2

    for i, date in enumerate(dates):
        day_of_week = date.weekday()
        weekend_factor = 0.6 if day_of_week >= 5 else 1.0
        growth = 1 + (i * 0.003)

        visitors = int(base_visitors * growth * weekend_factor * random.uniform(0.8, 1.3))
        sessions = int(visitors * random.uniform(1.2, 1.8))
        bounce_rate = round(random.uniform(35, 55), 1)
        customers = base_customers + (i // 5)
        gwh = round(base_gwh + (i * 0.015) + random.uniform(-0.1, 0.1), 2)
        deals_new = int(random.uniform(2, 15) * weekend_factor)
        deals_total = 30 + (i // 3)
        deals_won = int(random.uniform(0, 4))
        impressions = int(random.uniform(800, 2500) * growth)
        views = int(impressions * random.uniform(0.1, 0.2))

        rows.append({
            "date": date,
            "ga_visitors": visitors,
            "ga_sessions": sessions,
            "ga_bounce_rate": bounce_rate,
            "notion_customers_total": customers,
            "notion_yearly_consumption_gwh": gwh,
            "zoho_deals_new": deals_new,
            "zoho_deals_total": deals_total,
            "zoho_deals_won": deals_won,
            "li_impressions": impressions,
            "li_views": views,
        })

    return pd.DataFrame(rows)


def _generate_dummy_monthly_data() -> pd.DataFrame:
    """Generiert Dummy-Monatsdaten aus den Tagesdaten."""
    daily = _generate_dummy_daily_data()
    daily["month"] = pd.to_datetime(daily["date"]).dt.to_period("M").astype(str)

    monthly = daily.groupby("month").agg(
        ga_visitors_sum=("ga_visitors", "sum"),
        ga_visitors_avg=("ga_visitors", "mean"),
        notion_customers_end=("notion_customers_total", "last"),
        notion_customers_new=("notion_customers_total", lambda x: x.iloc[-1] - x.iloc[0]),
        notion_yearly_consumption_gwh=("notion_yearly_consumption_gwh", "last"),
        zoho_deals_sum=("zoho_deals_new", "sum"),
        zoho_deals_won_sum=("zoho_deals_won", "sum"),
        li_impressions_sum=("li_impressions", "sum"),
        li_views_sum=("li_views", "sum"),
    ).reset_index()

    monthly["ga_visitors_avg"] = monthly["ga_visitors_avg"].round(0).astype(int)

    return monthly


def _generate_dummy_targets() -> pd.DataFrame:
    """Generiert Dummy-Zielwerte (Jahresziele)."""
    return pd.DataFrame([
        {"kpi": "ga_visitors", "target_yearly": 120000, "unit": "Besucher", "category": "Website"},
        {"kpi": "notion_customers_total", "target_yearly": 100, "unit": "Kunden", "category": "Sales"},
        {"kpi": "notion_yearly_consumption_gwh", "target_yearly": 10.0, "unit": "GWh", "category": "Energy"},
        {"kpi": "zoho_deals_new", "target_yearly": 500, "unit": "Deals", "category": "Sales"},
        {"kpi": "li_impressions", "target_yearly": 600000, "unit": "Impressions", "category": "Social"},
        {"kpi": "li_views", "target_yearly": 100000, "unit": "Views", "category": "Social"},
    ])


@st.cache_data(ttl=config.CACHE_TTL)
def load_daily_kpis() -> pd.DataFrame:
    """Lädt tagesaktuelle KPI-Daten aus Google Sheets oder Dummy-Daten."""
    client = _get_gspread_client()
    if client and config.GOOGLE_SHEETS_ID:
        try:
            sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
            worksheet = sheet.worksheet(config.SHEET_DAILY)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            # Numerische Spalten konvertieren (DE/EN Locale-sicher)
            numeric_cols = [c for c in df.columns if c != "date"]
            for col in numeric_cols:
                df[col] = df[col].apply(_parse_number)
            return df
        except Exception as e:
            logger.error(f"Fehler beim Laden der Daily KPIs: {e}")

    logger.info("Verwende Dummy-Daten (kein Google Sheet konfiguriert)")
    return _generate_dummy_daily_data()


@st.cache_data(ttl=config.CACHE_TTL)
def load_monthly_kpis() -> pd.DataFrame:
    """Lädt monatlich aggregierte KPIs aus Google Sheets oder Dummy-Daten."""
    client = _get_gspread_client()
    if client and config.GOOGLE_SHEETS_ID:
        try:
            sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
            worksheet = sheet.worksheet(config.SHEET_MONTHLY)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            numeric_cols = [c for c in df.columns if c != "month"]
            for col in numeric_cols:
                df[col] = df[col].apply(_parse_number)
            return df
        except Exception as e:
            logger.error(f"Fehler beim Laden der Monthly KPIs: {e}")

    return _generate_dummy_monthly_data()


@st.cache_data(ttl=config.CACHE_TTL)
def load_targets() -> pd.DataFrame:
    """Lädt KPI-Zielwerte aus Google Sheets oder Dummy-Daten."""
    client = _get_gspread_client()
    if client and config.GOOGLE_SHEETS_ID:
        try:
            sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
            worksheet = sheet.worksheet(config.SHEET_TARGETS)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            df["target_yearly"] = pd.to_numeric(df["target_yearly"], errors="coerce")
            return df
        except Exception as e:
            logger.error(f"Fehler beim Laden der Targets: {e}")

    return _generate_dummy_targets()


def is_using_dummy_data() -> bool:
    """Prüft ob Dummy-Daten verwendet werden."""
    return not (config.GOOGLE_SHEETS_ID and config.GOOGLE_SERVICE_ACCOUNT_JSON)
