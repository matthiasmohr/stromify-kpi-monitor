"""
Stromify KPI Cronjob - Google Sheets Writer
Schreibt KPI-Daten in das Google Sheet.
"""
import json
import base64
import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

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
    # Hat sowohl Punkt als auch Komma → Format erkennen
    has_dot = "." in s
    has_comma = "," in s
    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            # Deutsch: 1.234,56 → Punkt ist 1000er, Komma ist Dezimal
            s = s.replace(".", "").replace(",", ".")
        else:
            # Englisch: 1,234.56 → Komma ist 1000er, Punkt ist Dezimal
            s = s.replace(",", "")
    elif has_comma and not has_dot:
        # Nur Komma: 5,13 → Dezimalkomma
        s = s.replace(",", ".")
    # Nur Punkt oder keine Trennzeichen: Standard-Float
    try:
        return float(s)
    except ValueError:
        return 0.0

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_client() -> gspread.Client:
    """Erstellt einen authentifizierten gspread Client."""
    try:
        sa_json = base64.b64decode(config.GOOGLE_SERVICE_ACCOUNT_JSON)
        sa_info = json.loads(sa_json)
    except Exception:
        sa_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_JSON)

    credentials = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(credentials)


def _ensure_headers(worksheet, expected_headers: list[str]):
    """Stellt sicher, dass die Header-Zeile korrekt ist."""
    existing = worksheet.row_values(1)
    if existing != expected_headers:
        worksheet.update("A1", [expected_headers])
        logger.info(f"Header aktualisiert: {expected_headers}")


def write_daily_row(data: dict):
    """
    Schreibt eine Tageszeile in das kpi_daily Sheet.
    Wenn heute schon ein Eintrag existiert, wird er aktualisiert.

    Args:
        data: dict mit allen KPI-Werten für heute
    """
    client = _get_client()
    sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
    worksheet = sheet.worksheet(config.SHEET_DAILY)

    # Header sicherstellen
    _ensure_headers(worksheet, config.DAILY_COLUMNS)

    today_str = datetime.now().strftime("%Y-%m-%d")

    # Zeile zusammenbauen
    row = [
        today_str,
        data.get("ga_visitors", 0),
        data.get("ga_sessions", 0),
        data.get("ga_bounce_rate", 0.0),
        data.get("notion_customers_total", 0),
        data.get("notion_yearly_consumption_gwh", 0.0),
        data.get("zoho_deals_total", 0),
        data.get("li_impressions", 0),
        data.get("li_views", 0),
    ]

    # Alle Werte als String für RAW-Modus (verhindert Locale-Probleme mit Komma/Punkt)
    row = [str(v) for v in row]

    # Prüfe ob heute schon ein Eintrag existiert
    all_dates = worksheet.col_values(1)  # Spalte A (date)
    if today_str in all_dates:
        row_index = all_dates.index(today_str) + 1  # 1-basiert
        worksheet.update(f"A{row_index}", [row], value_input_option="RAW")
        logger.info(f"Tageseintrag für {today_str} aktualisiert (Zeile {row_index})")
    else:
        worksheet.append_row(row, value_input_option="RAW")
        logger.info(f"Neuer Tageseintrag für {today_str} hinzugefügt")


def backfill_ga_rows(ga_history: dict):
    """
    Schreibt historische GA4-Tageswerte ins Sheet – überspringt bereits vorhandene Daten.

    Args:
        ga_history: {date_str: {ga_visitors, ga_sessions, ga_bounce_rate}}
    """
    client = _get_client()
    sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
    worksheet = sheet.worksheet(config.SHEET_DAILY)

    _ensure_headers(worksheet, config.DAILY_COLUMNS)

    existing_dates = set(worksheet.col_values(1)[1:])  # Zeile 1 = Header überspringen

    new_rows = []
    skipped = 0
    for date_str in sorted(ga_history.keys()):
        if date_str in existing_dates:
            skipped += 1
            continue
        d = ga_history[date_str]
        row = [
            date_str,
            d.get("ga_visitors", 0),
            d.get("ga_sessions", 0),
            d.get("ga_bounce_rate", 0.0),
            0,    # notion_customers_total – unbekannt für historische Tage
            0.0,  # notion_yearly_consumption_gwh
            0,    # zoho_deals_total
            0,    # li_impressions
            0,    # li_views
        ]
        new_rows.append([str(v) for v in row])

    if new_rows:
        worksheet.append_rows(new_rows, value_input_option="RAW")
        logger.info(f"Backfill: {len(new_rows)} historische GA4-Zeilen geschrieben, {skipped} übersprungen")
    else:
        logger.info(f"Backfill: Alle {skipped} Tage bereits vorhanden")


def write_active_leads(leads: list):
    """
    Schreibt ALLE Deals in das Sheet 'zoho_leads' mit Status-Attribut.
    Status: new | active | won | lost | waiting
    Überschreibt immer den kompletten Inhalt.
    """
    client = _get_client()
    sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)

    try:
        worksheet = sheet.worksheet("zoho_leads")
    except Exception:
        worksheet = sheet.add_worksheet(title="zoho_leads", rows=500, cols=8)

    headers = ["name", "company", "stage", "status", "amount", "created_date", "closing_date"]
    rows = [headers]
    for lead in leads:
        rows.append([
            lead.get("name", ""),
            lead.get("company", ""),
            lead.get("stage", ""),
            lead.get("status", ""),
            str(lead.get("amount", "") or ""),
            lead.get("created_date", ""),
            lead.get("closing_date", ""),
        ])

    worksheet.clear()
    worksheet.update("A1", rows, value_input_option="RAW")
    logger.info(f"zoho_leads Sheet aktualisiert: {len(leads)} Deals total")


def _calc_customers_new(df, current_month: str) -> int:
    """
    Berechnet Neukunden im Monat.
    Vergleicht letzten Wert des aktuellen Monats mit dem letzten Wert des Vormonats.
    Falls kein Vormonat existiert (erster Monat), wird der aktuelle Endwert genommen.
    Nur Zeilen mit Kunden > 0 werden berücksichtigt (Backfill-Zeilen haben 0).
    """
    # Nur Zeilen mit echten Kundendaten (Backfill-Zeilen haben 0)
    df_real = df[df["notion_customers_total"] > 0]

    month_real = df_real[df_real["month"] == current_month]
    if month_real.empty:
        return 0
    current_end = int(month_real["notion_customers_total"].iloc[-1])

    # Vormonat finden
    prev_months = df_real[df_real["month"] < current_month]
    if prev_months.empty:
        return current_end

    prev_end = int(prev_months["notion_customers_total"].iloc[-1])
    return max(0, current_end - prev_end)


def update_monthly_aggregation():
    """
    Berechnet und aktualisiert die monatliche Aggregation im kpi_monthly Sheet.
    Aggregiert die Tagesdaten des aktuellen Monats.
    """
    import pandas as pd

    client = _get_client()
    sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)

    # Tagesdaten laden
    daily_ws = sheet.worksheet(config.SHEET_DAILY)
    daily_data = daily_ws.get_all_records()

    if not daily_data:
        logger.warning("Keine Tagesdaten vorhanden für monatliche Aggregation")
        return

    df = pd.DataFrame(daily_data)
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M").astype(str)

    # Nach Datum sortieren – wichtig damit iloc[-1] immer den neuesten Eintrag liefert
    # (Backfill-Zeilen werden ans Ende angehängt, sind aber zeitlich frühere Tage)
    df = df.sort_values("date").reset_index(drop=True)

    # Numerische Spalten sauber konvertieren
    # Deutsche Locale: 1.234,56 → 1234.56 / RAW-Modus: 5.13 bleibt 5.13
    numeric_cols = [c for c in df.columns if c not in ("date", "month")]
    for col in numeric_cols:
        df[col] = df[col].apply(_parse_number)

    # Aktuellen Monat aggregieren
    current_month = datetime.now().strftime("%Y-%m")
    month_df = df[df["month"] == current_month]

    if month_df.empty:
        logger.warning(f"Keine Daten für Monat {current_month}")
        return

    # Für Snapshot-Werte (customers, gwh) nur Zeilen mit echten Daten nutzen
    # (Backfill-Zeilen haben 0 für Notion-Felder)
    month_real = month_df[month_df["notion_customers_total"] > 0]
    customers_end = int(month_real["notion_customers_total"].iloc[-1]) if not month_real.empty else 0
    gwh_end = round(float(month_real["notion_yearly_consumption_gwh"].iloc[-1]), 2) if not month_real.empty else 0.0

    # Zoho-Snapshot: letzter bekannter Gesamtwert des Monats
    zoho_df = month_df[month_df["zoho_deals_total"] > 0]
    zoho_total_end = int(zoho_df["zoho_deals_total"].iloc[-1]) if not zoho_df.empty else 0

    monthly_row = [
        current_month,
        int(month_df["ga_visitors"].sum()),
        int(month_df["ga_visitors"].mean()),
        customers_end,
        int(_calc_customers_new(df, current_month)),
        gwh_end,
        zoho_total_end,
        int(month_df["li_impressions"].sum()),
        int(month_df["li_views"].sum()),
    ]

    # Monthly Sheet aktualisieren
    monthly_ws = sheet.worksheet(config.SHEET_MONTHLY)
    _ensure_headers(monthly_ws, config.MONTHLY_COLUMNS)

    monthly_row = [str(v) for v in monthly_row]

    all_months = monthly_ws.col_values(1)
    if current_month in all_months:
        row_index = all_months.index(current_month) + 1
        monthly_ws.update(f"A{row_index}", [monthly_row], value_input_option="RAW")
        logger.info(f"Monatsdaten für {current_month} aktualisiert")
    else:
        monthly_ws.append_row(monthly_row, value_input_option="RAW")
        logger.info(f"Neue Monatsdaten für {current_month} hinzugefügt")
