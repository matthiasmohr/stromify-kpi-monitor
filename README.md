# ⚡ Stromify KPI Monitor

Zentrales Dashboard zur Visualisierung der wichtigsten Unternehmens-KPIs. Aggregiert Daten aus Google Analytics, Notion und Zoho CRM in einem Streamlit-Frontend.

## Architektur

```
Google Analytics ─┐
Notion ───────────┤──▶ Python Cronjob ──▶ Google Sheet ──▶ Streamlit Dashboard
Zoho CRM ─────────┘       (scheduled)        (Datenhaltung)     (Frontend)
```

## KPIs

| Quelle | Kennzahlen |
|---|---|
| Google Analytics | Website-Besucher, Sessions, Absprungrate |
| Notion | Kunden Gesamt, Yearly Consumption (GWh) |
| Zoho CRM | Neue Deals, Deals Gesamt, Deals gewonnen |

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
│   └── fetch_zoho.py         # Zoho CRM API
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

- **`kpi_daily`** – Tagesaktuelle KPI-Werte (Spalten: `date`, `ga_visitors`, `ga_sessions`, `ga_bounce_rate`, `notion_customers_total`, `notion_yearly_consumption_gwh`, `zoho_deals_new`, `zoho_deals_total`, `zoho_deals_won`)
- **`kpi_monthly`** – Monatliche Aggregation (Spalten: `month`, `ga_visitors_sum`, `ga_visitors_avg`, `notion_customers_end`, `notion_customers_new`, `notion_yearly_consumption_gwh`, `zoho_deals_sum`, `zoho_deals_won_sum`)
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
python -m cronjob.main --backfill 30
```

In Produktion wird der Cronjob als separater Railway-Service mit Cron Schedule ausgeführt (siehe [Deployment](#deployment-railway)).

## Deployment (Railway)

```bash
# Railway CLI installieren (falls noch nicht vorhanden)
npm install -g @railway/cli

# Login & Deploy
railway login
railway up
```

Anschließend alle Umgebungsvariablen aus `.env.example` in den Railway Environment Variables setzen.

### Cronjob als separater Railway-Service

Der KPI-Fetch läuft **nicht** mehr im Streamlit-Prozess (App-Restarts/Idle haben Runs verschluckt). Stattdessen läuft er als zweiter Service (`kpi-batch-report`) im selben Railway-Projekt mit eigener Config-Datei `railway.cron.toml`:

```toml
[deploy]
startCommand = "python -m cronjob.main"
cronSchedule = "0 21 * * *"   # täglich 21:00 UTC = 23:00 MESZ / 22:00 MEZ
restartPolicyType = "never"
```

Setup im Railway-Dashboard:

1. Service `kpi-batch-report` → **Settings → Config-as-Code → Config Path:** `railway.cron.toml` setzen.
2. **Settings → Networking:** Public Networking deaktivieren (kein HTTP nötig).
3. **Settings → Healthcheck:** leeren.
4. Alle Env-Vars vom Web-Service übernehmen (Project → **Shared Variables** ist der bequemste Weg).
5. Deploy triggern. Logs pro Run sind im Service-Dashboard einsehbar (`KPI-Fetch gestartet: …` markiert den Start).

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
| `AUTH0_DOMAIN` / `AUTH0_CLIENT_ID` / `AUTH0_CLIENT_SECRET` | Auth0 **M2M-App** für die MAU-Metrik (Cronjob) und die "Letzte Logins"-Ansicht. Benötigt die Scopes `read:stats` **und** `read:users`. |
| `AUTH0_LOGIN_DOMAIN` | Auth0-Tenant-Domain für den Dashboard-Login |
| `AUTH0_LOGIN_CLIENT_ID` / `AUTH0_LOGIN_CLIENT_SECRET` | Auth0 **Regular Web App** für den Login |
| `AUTH0_REDIRECT_URI` | Callback-URL, z.B. `https://<app>.up.railway.app/oauth2callback` |
| `AUTH0_COOKIE_SECRET` | Zufalls-String zum Signieren des Login-Cookies (`openssl rand -hex 32`) |
| `ALLOWED_EMAIL_DOMAIN` | Erlaubte Login-Domain (Default: `@stromify.de`) |

## Login / Authentifizierung

Das Dashboard ist mit **Auth0 (OIDC)** geschützt – Login nur mit `@stromify.de`-Account.
Genutzt wird die native Streamlit-Auth (`st.login` / `st.user`, ab Streamlit 1.42).

**Auth0 einrichten:**

1. Im Auth0-Dashboard eine **Regular Web Application** anlegen (separat von der M2M-App des Cronjobs).
2. Unter *Settings → Allowed Callback URLs* die Redirect-URL eintragen:
   - lokal: `http://localhost:8501/oauth2callback`
   - prod: `https://<app>.up.railway.app/oauth2callback`
3. *Allowed Logout URLs* analog auf die App-Basis-URL setzen.
4. Client ID / Secret / Tenant-Domain notieren.

**Domain-Beschränkung:** Die App lässt nur eingeloggte Nutzer mit verifizierter
E-Mail der Domain aus `ALLOWED_EMAIL_DOMAIN` durch. Zusätzlich empfiehlt sich in
Auth0 eine Login-Action/Rule, die Logins fremder Domains gar nicht erst zulässt
(Defense-in-Depth).

**Lokal:** `.streamlit/secrets.toml` aus `.streamlit/secrets.toml.example` erstellen
und befüllen, dann `streamlit run app.py`.

**Railway:** Die `AUTH0_LOGIN_*`-, `AUTH0_REDIRECT_URI`- und `AUTH0_COOKIE_SECRET`-Variablen
setzen – `start.sh` generiert daraus beim Start automatisch die `.streamlit/secrets.toml`.

## Tech Stack

- **Frontend:** Streamlit + Plotly
- **Datenhaltung:** Google Sheets (via gspread)
- **APIs:** Google Analytics Data API v4, Notion API, Zoho CRM API v5
- **Deployment:** Railway
