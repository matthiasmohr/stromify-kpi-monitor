# ⚡ Stromify KPI Monitor

Zentrales Dashboard zur Visualisierung der wichtigsten Unternehmens-KPIs. Aggregiert Daten aus Google Analytics, Notion, Zoho CRM und LinkedIn in einem Streamlit-Frontend.

## Architektur

```
Google Analytics ─┐
Notion ───────────┤
Zoho CRM ─────────┤──▶ Python Cronjob ──▶ Google Sheet ──▶ Streamlit Dashboard
LinkedIn ─────────┘       (scheduled)        (Datenhaltung)     (Frontend)
```

## KPIs

| Quelle | Kennzahlen |
|---|---|
| Google Analytics | Website-Besucher, Sessions, Absprungrate |
| Notion | Kunden Gesamt, Yearly Consumption (GWh) |
| Zoho CRM | Neue Deals, Deals Gesamt, Deals gewonnen |
| LinkedIn | Impressions, Views |

## Projektstruktur

```
stromify-kpi-monitor/
├── app.py                    # Streamlit Dashboard
├── config.py                 # Zentrale Konfiguration
├── data_loader.py            # Google Sheets Leselogik + Dummy-Fallback
├── charts.py                 # Plotly Chart-Funktionen
├── requirements.txt          # Dependencies
├── Procfile                  # Railway Deployment
├── railway.toml              # Railway Config
├── .streamlit/
│   └── config.toml           # Stromify Theme
├── cronjob/
│   ├── main.py               # Orchestrator
│   ├── sheet_writer.py       # Google Sheets Schreiblogik
│   ├── fetch_ga.py           # Google Analytics Data API v4
│   ├── fetch_notion.py       # Notion API
│   ├── fetch_zoho.py         # Zoho CRM API
│   └── fetch_linkedin.py     # LinkedIn API
└── .env.example              # Vorlage für Umgebungsvariablen
```

## Setup

### 1. Abhängigkeiten installieren

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Umgebungsvariablen konfigurieren

```bash
cp .env.example .env
# .env mit echten API-Keys befüllen
```

### 3. Google Sheet vorbereiten

Erstelle ein Google Sheet mit drei Blättern:

- **`kpi_daily`** – Tagesaktuelle KPI-Werte (Spalten: `date`, `ga_visitors`, `ga_sessions`, `ga_bounce_rate`, `notion_customers_total`, `notion_yearly_consumption_gwh`, `zoho_deals_new`, `zoho_deals_total`, `zoho_deals_won`, `li_impressions`, `li_views`)
- **`kpi_monthly`** – Monatliche Aggregation (Spalten: `month`, `ga_visitors_sum`, `ga_visitors_avg`, `notion_customers_end`, `notion_customers_new`, `notion_yearly_consumption_gwh`, `zoho_deals_sum`, `zoho_deals_won_sum`, `li_impressions_sum`, `li_views_sum`)
- **`kpi_targets`** – Zielwerte für Soll/Ist-Vergleich (Spalten: `kpi`, `target_monthly`, `unit`, `category`)

Teile das Sheet mit der E-Mail des Google Service Accounts.

### 4. Dashboard starten

```bash
streamlit run app.py
```

Ohne konfigurierte Google Sheets Verbindung zeigt das Dashboard automatisch Demo-Daten an.

## Cronjob

Der Cronjob sammelt Daten aus allen Quellen und schreibt sie ins Google Sheet.

```bash
# Einmalig ausführen
python -m cronjob.main

# für historische Nachbeladung
python3 -m cronjob.main --backfill 30

# Als dauerhafter Prozess (täglich um 22:00)
python -m cronjob.main --schedule

# Mit benutzerdefiniertem Intervall (alle 60 Minuten)
python -m cronjob.main --interval 60
```

## Deployment (Railway)

```bash
# Railway CLI installieren (falls noch nicht vorhanden)
npm install -g @railway/cli

# Login & Deploy
railway login
railway up
```

Anschließend alle Umgebungsvariablen aus `.env.example` in den Railway Environment Variables setzen.

## Umgebungsvariablen

| Variable | Beschreibung |
|---|---|
| `GOOGLE_SHEETS_ID` | ID des Google Sheets |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Service Account Key (Base64-encoded) |
| `GA_PROPERTY_ID` | Google Analytics Property (z.B. `properties/123456789`) |
| `NOTION_API_KEY` | Notion Integration API Key |
| `NOTION_CUSTOMERS_DB_ID` | Notion Kunden-Datenbank ID |
| `ZOHO_CLIENT_ID` | Zoho OAuth Client ID |
| `ZOHO_CLIENT_SECRET` | Zoho OAuth Client Secret |
| `ZOHO_REFRESH_TOKEN` | Zoho OAuth Refresh Token |
| `ZOHO_API_DOMAIN` | Zoho API Domain (Default: `https://www.zohoapis.eu`) |
| `ZOHO_ACCOUNTS_URL` | Zoho Accounts URL (Default: `https://accounts.zoho.eu`) |
| `LINKEDIN_ACCESS_TOKEN` | LinkedIn OAuth2 Access Token |
| `LINKEDIN_ORG_ID` | LinkedIn Organization ID |

## Tech Stack

- **Frontend:** Streamlit + Plotly
- **Datenhaltung:** Google Sheets (via gspread)
- **APIs:** Google Analytics Data API v4, Notion API, Zoho CRM API v5, LinkedIn Marketing API v2
- **Deployment:** Railway
