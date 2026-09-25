# ⚡ Stromify KPI Monitor

Zentrales Dashboard zur Visualisierung der wichtigsten Unternehmens-KPIs. Aggregiert Daten aus Google Analytics, Zoho CRM und Auth0 in einem Streamlit-Frontend.

## Architektur

```
Google Analytics ─┐
Zoho CRM ─────────┤──▶ Python Cronjob ──▶ Google Sheet ──▶ Streamlit Dashboard
Auth0 ────────────┘       (scheduled)        (Datenhaltung)     (Frontend)
```

## KPIs

| Quelle | Kennzahlen |
|---|---|
| Google Analytics | Website-Besucher, Sessions, Absprungrate |
| Zoho CRM – Deals | Lead-Pipeline Energie & Lizenzen (neu, aktiv, gewonnen, verloren, Warteschleife) |
| Zoho CRM – Verträge | Aktive Verträge Energie, Yearly Consumption (GWh), Provision Energie (CLV), Lizenzumsatz (CLV) |
| Auth0 | Monthly Active Users, letzte Logins externer Nutzer |

### Vertrags-KPIs (Zoho Custom-Modul `Vertr_ge`)

Seit September 2026 kommen Kunden-, Verbrauchs- und Erlös-KPIs aus dem Zoho-Modul **Verträge**
(vorher Notion). Definitionen:

| Sheet-Spalte | Definition |
|---|---|
| `zoho_contracts_active` | Anzahl Verträge mit Vertragsart **„Energie + Einsparvergütung“**, die aktiv sind: kein Lieferende **oder** heute < Lieferende. Verträge mit Lieferbeginn in der Zukunft zählen mit. |
| `zoho_yearly_consumption_gwh` | Σ `JVP Strom` + `JVP Gas` aller Verträge „Energie + Einsparvergütung“, in GWh |
| `zoho_provision_eur` | Σ `CLV Vertrag` aller Verträge „Energie + Einsparvergütung“ |
| `zoho_license_revenue_eur` | Σ `CLV Vertrag` aller Verträge mit Vertragsart **„Lizenz“** |

Die Summen sind – wie früher in Notion – nicht auf aktive Verträge eingeschränkt.
Die alten Spalten `notion_*` und `manual_license_revenue` bleiben eingefroren im Sheet
(Historie ab Feb. 2026); das Dashboard nutzt für alte Tage automatisch diese Werte als Fallback
(`data_loader.apply_source_fallback`).

Der Cron-Token hat nur den Scope `ZohoCRM.modules.READ` – COQL-Aggregation ist damit nicht
möglich, die Berechnung passiert in Python (`cronjob/fetch_zoho.py::calc_contract_kpis`).
Prüfen mit `python test_zoho_contracts.py`.

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
│   ├── fetch_zoho.py         # Zoho CRM API (Deals + Verträge)
│   └── fetch_auth0.py        # Auth0 Management API (MAU)
├── test_zoho_contracts.py    # Verifikation der Vertrags-KPIs
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

- **`kpi_daily`** – Tagesaktuelle KPI-Werte. Spalten siehe `config.DAILY_COLUMNS` (der Cronjob legt die Header selbst an), u. a. `zoho_contracts_active`, `zoho_yearly_consumption_gwh`, `zoho_provision_eur`, `zoho_license_revenue_eur`, `zoho_deals_*`, `auth0_mau`.
- **`kpi_monthly`** – Monatliche Aggregation, Spalten siehe `config.MONTHLY_COLUMNS`.
- **`kpi_targets`** – Jahresziele für Soll/Ist-Vergleich (Spalten: `kpi`, `target_yearly`, `unit`, `category`). Als `kpi` die Dashboard-Namen `contracts_active`, `yearly_consumption_gwh`, `provision_eur`, `license_revenue_eur`, `ga_visitors`, `zoho_deals_new` verwenden. Die alten Schlüssel `notion_customers_total` und `notion_yearly_consumption_gwh` werden per `config.KPI_ALIASES` weiterhin verstanden.

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
- **APIs:** Google Analytics Data API v4, Zoho CRM API v8 (Deals + Custom-Modul Verträge), Auth0 Management API
- **Deployment:** Railway
