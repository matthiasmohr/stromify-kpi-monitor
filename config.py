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

# --- Notion ---
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_CUSTOMERS_DB_ID = os.getenv("NOTION_CUSTOMERS_DB_ID", "")
NOTION_MALOS_DB_ID = os.getenv("NOTION_MALOS_DB_ID", "")

# --- Zoho CRM ---
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID", "")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN", "")
ZOHO_API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.eu")
ZOHO_ACCOUNTS_URL = os.getenv("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.eu")

# --- LinkedIn ---
LINKEDIN_ACCESS_TOKEN = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
LINKEDIN_ORG_ID = os.getenv("LINKEDIN_ORG_ID", "")

# --- KPI Spalten-Mapping ---
DAILY_COLUMNS = [
    "date",
    "ga_visitors",
    "ga_sessions",
    "ga_bounce_rate",
    "notion_customers_total",
    "notion_yearly_consumption_gwh",
    "zoho_deals_total",
    "li_impressions",
    "li_views",
]

MONTHLY_COLUMNS = [
    "month",
    "ga_visitors_sum",
    "ga_visitors_avg",
    "notion_customers_end",
    "notion_customers_new",
    "notion_yearly_consumption_gwh",
    "zoho_deals_total_end",
    "li_impressions_sum",
    "li_views_sum",
]

TARGET_COLUMNS = ["kpi", "target_yearly", "unit", "category"]

# --- KPI Display Konfiguration ---
KPI_DISPLAY = {
    "ga_visitors": {"label": "Website Besucher", "icon": "🌐", "format": "{:,.0f}", "category": "Website"},
    "ga_sessions": {"label": "Sessions", "icon": "📊", "format": "{:,.0f}", "category": "Website"},
    "ga_bounce_rate": {"label": "Absprungrate", "icon": "↩️", "format": "{:.1f}%", "category": "Website"},
    "notion_customers_total": {"label": "Kunden Gesamt", "icon": "👥", "format": "{:,.0f}", "category": "Sales"},
    "notion_yearly_consumption_gwh": {"label": "Yearly Consumption", "icon": "⚡", "format": "{:,.1f} GWh", "category": "Energy"},
    "zoho_deals_new": {"label": "Neue Leads", "icon": "🎯", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_total": {"label": "Leads Gesamt", "icon": "📋", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_won": {"label": "Leads gewonnen", "icon": "✅", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_lost": {"label": "Leads verloren", "icon": "❌", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_waiting": {"label": "Warteschleife", "icon": "⏳", "format": "{:,.0f}", "category": "Sales"},
    "zoho_deals_active": {"label": "Leads aktiv", "icon": "🔄", "format": "{:,.0f}", "category": "Sales"},
    "li_impressions": {"label": "LinkedIn Impressions", "icon": "👁️", "format": "{:,.0f}", "category": "Social"},
    "li_views": {"label": "LinkedIn Views", "icon": "📺", "format": "{:,.0f}", "category": "Social"},
}

# --- Streamlit Cache TTL (Sekunden) ---
CACHE_TTL = 300  # 5 Minuten
