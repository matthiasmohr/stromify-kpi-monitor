"""
Stromify KPI Monitor - Zentrale Konfiguration
"""
import os
from dotenv import load_dotenv

load_dotenv()

# --- Google Sheets ---
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# Sheet-Namen
SHEET_DAILY = "kpi_daily"
SHEET_MONTHLY = "kpi_monthly"
SHEET_TARGETS = "kpi_targets"

# --- Google Analytics ---
GA_PROPERTY_ID = os.getenv("GA_PROPERTY_ID", "")

# --- Zoho CRM ---
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.eu")
ZOHO_ACCOUNTS_URL = os.getenv("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.eu")

# Custom-Modul "Verträge" in Zoho CRM (Nachfolger der Notion Kunden-/Malos-/Provisionen-DBs).
# Der Cron-Token hat nur den Scope ZohoCRM.modules.READ – COQL steht damit NICHT zur
# Verfügung, die Aggregation passiert deshalb in Python (siehe cronjob/fetch_zoho.py).
ZOHO_CONTRACTS_MODULE = "Vertr_ge"
ZOHO_CONTRACT_TYPE_ENERGY = "Energie + Einsparvergütung"
ZOHO_CONTRACT_TYPE_LICENSE = "Lizenz"

# --- Auth0 Management API (Machine-to-Machine App) ---
# Wird vom Cronjob für die MAU-Metrik genutzt – NICHT für Login.
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN", "")
AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID", "")
AUTH0_CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET", "")

# --- Dashboard Login (Auth0 OIDC, "Regular Web Application") ---
# Die eigentliche Login-Konfiguration (client_id/secret/domain) liest Streamlit
# aus .streamlit/secrets.toml ([auth]-Block); auf Railway wird diese von start.sh
# aus den AUTH0_LOGIN_* Env-Variablen generiert.
# Nur E-Mail-Adressen mit dieser Domain dürfen das Dashboard sehen.
ALLOWED_EMAIL_DOMAIN = os.getenv("ALLOWED_EMAIL_DOMAIN", "@stromify.de")

# Notausgang für die LOKALE Entwicklung: deaktiviert den Login komplett.
# NIEMALS in der Produktion (Railway) setzen! Default = aktiv (fail-closed).
DISABLE_AUTH = os.getenv("DISABLE_AUTH", "").strip().lower() in ("1", "true", "yes")

# --- KPI Spalten-Mapping ---
# Die notion_*-Spalten und manual_license_revenue werden seit der Umstellung auf Zoho
# (Sept. 2026) nicht mehr befüllt, bleiben aber für die Historie im Sheet erhalten.
# Die zoho_*-Nachfolger stehen am Ende, damit bestehende Zeilen nicht verrutschen.
DAILY_COLUMNS = [
    "date",
    "ga_visitors",
    "ga_sessions",
    "ga_bounce_rate",
    "notion_customers_total",          # eingefroren (Notion)
    "notion_yearly_consumption_gwh",   # eingefroren (Notion)
    "notion_provision_eur",            # eingefroren (Notion)
    "manual_license_revenue",          # eingefroren (nie genutzt)
    "zoho_deals_total",
    "zoho_deals_new",
    "zoho_deals_active",
    "zoho_deals_won",
    "zoho_deals_lost",
    "zoho_deals_waiting",
    "auth0_mau",
    "zoho_contracts_active",           # Verträge "Energie + Einsparvergütung", Lieferende leer oder > heute
    "zoho_yearly_consumption_gwh",     # Σ JVP Strom + JVP Gas (Energie-Verträge)
    "zoho_provision_eur",              # Σ CLV Vertrag (Energie-Verträge)
    "zoho_license_revenue_eur",        # Σ CLV Vertrag (Lizenz-Verträge)
]

MONTHLY_COLUMNS = [
    "month",
    "ga_visitors_sum",
    "ga_visitors_avg",
    "notion_customers_end",            # eingefroren (Notion)
    "notion_customers_new",            # eingefroren (Notion)
    "notion_yearly_consumption_gwh",   # eingefroren (Notion)
    "notion_provision_eur",            # eingefroren (Notion)
    "manual_license_revenue",          # eingefroren (nie genutzt)
    "zoho_deals_total_end",
    "zoho_deals_new_end",
    "zoho_deals_active_end",
    "zoho_deals_won_end",
    "zoho_deals_lost_end",
    "zoho_deals_waiting_end",
    "auth0_mau_end",
    "zoho_contracts_active_end",
    "zoho_contracts_new",
    "zoho_yearly_consumption_gwh",
    "zoho_provision_eur",
    "zoho_license_revenue_eur",
]

TARGET_COLUMNS = ["kpi", "target_yearly", "unit", "category"]

# --- Quellen-neutrale KPI-Namen (Dashboard) ---
# Das Dashboard arbeitet mit diesen Namen. data_loader.apply_source_fallback() befüllt sie
# aus der zoho_*-Spalte und fällt für Tage vor der Umstellung auf die notion_*-Spalte zurück.
# Reihenfolge: (Dashboard-Name, Zoho-Spalte, Notion-Spalte)
KPI_SOURCE_FALLBACK = [
    ("contracts_active", "zoho_contracts_active", "notion_customers_total"),
    ("yearly_consumption_gwh", "zoho_yearly_consumption_gwh", "notion_yearly_consumption_gwh"),
    ("provision_eur", "zoho_provision_eur", "notion_provision_eur"),
    ("license_revenue_eur", "zoho_license_revenue_eur", "manual_license_revenue"),
]

# Gleiche Logik für die Monatstabelle
MONTHLY_SOURCE_FALLBACK = [
    ("contracts_active_end", "zoho_contracts_active_end", "notion_customers_end"),
    ("contracts_new", "zoho_contracts_new", "notion_customers_new"),
    ("yearly_consumption_gwh", "zoho_yearly_consumption_gwh", "notion_yearly_consumption_gwh"),
    ("provision_eur", "zoho_provision_eur", "notion_provision_eur"),
    ("license_revenue_eur", "zoho_license_revenue_eur", "manual_license_revenue"),
]

# Alte KPI-Schlüssel im kpi_targets-Sheet → neue Dashboard-Namen.
# Damit müssen bestehende Zielwerte im Sheet nicht umbenannt werden.
KPI_ALIASES = {
    "notion_customers_total": "contracts_active",
    "notion_yearly_consumption_gwh": "yearly_consumption_gwh",
    "notion_provision_eur": "provision_eur",
    "manual_license_revenue": "license_revenue_eur",
}

# --- KPI Display Konfiguration ---
KPI_DISPLAY = {
    "ga_visitors": {"label": "Website Besucher", "icon": "🌐", "format": "{:,.0f}", "category": "Website"},
    "ga_sessions": {"label": "Sessions", "icon": "📊", "format": "{:,.0f}", "category": "Website"},
    "ga_bounce_rate": {"label": "Absprungrate", "icon": "↩️", "format": "{:.1f}%", "category": "Website"},
    "contracts_active": {"label": "Aktive Verträge Energie", "icon": "📑", "format": "{:,.0f}", "category": "Sales"},
    "yearly_consumption_gwh": {"label": "Yearly Consumption", "icon": "⚡", "format": "{:,.1f} GWh", "category": "Energy"},
    "provision_eur": {"label": "Provision Energie (CLV)", "icon": "💰", "format": "{:,.0f} €", "category": "Revenue"},
    "license_revenue_eur": {"label": "Lizenzumsatz (CLV)", "icon": "📄", "format": "{:,.0f} €", "category": "Revenue"},
    "zoho_deals_new": {"label": "Neue Leads", "icon": "🎯", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_total": {"label": "Leads Gesamt", "icon": "📋", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_won": {"label": "Leads gewonnen", "icon": "✅", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_lost": {"label": "Leads verloren", "icon": "❌", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_waiting": {"label": "Warteschleife", "icon": "⏳", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_active": {"label": "Leads aktiv", "icon": "🔄", "format": "{:,.0f}", "category": "Sales"},
    "auth0_mau": {"label": "Active Users", "icon": "📱", "format": "{:,.0f}", "category": "Product"},
}

# --- Streamlit Cache TTL (Sekunden) ---
CACHE_TTL = 300  # 5 Minuten
