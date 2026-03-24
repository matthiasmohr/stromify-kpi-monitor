"""
Google Analytics Test-Skript
Debuggt GA4 Verbindung und zeigt Rohdaten.
"""
import json
import base64
import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

PROPERTY_ID = os.getenv("GA_PROPERTY_ID", "")
SA_JSON_RAW = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

print("=" * 60)
print("🔍 Google Analytics 4 – Debug Test")
print("=" * 60)

# 1. Config prüfen
print(f"\n1️⃣  Config:")
print(f"   GA_PROPERTY_ID: {PROPERTY_ID or '❌ NICHT GESETZT'}")
if not PROPERTY_ID:
    print("   → Bitte GA_PROPERTY_ID in .env setzen (z.B. 'properties/123456789')")
    exit(1)

# Property-Format prüfen
if not PROPERTY_ID.startswith("properties/"):
    print(f"   ⚠️  Property ID sollte mit 'properties/' beginnen!")
    print(f"   → Versuche automatisch: properties/{PROPERTY_ID}")
    PROPERTY_ID = f"properties/{PROPERTY_ID}"
else:
    print(f"   ✅ Format korrekt: {PROPERTY_ID}")

# 2. Credentials laden
print(f"\n2️⃣  Credentials laden...")
try:
    try:
        sa_info = json.loads(base64.b64decode(SA_JSON_RAW))
        print("   ✅ Base64-dekodiert")
    except Exception:
        sa_info = json.loads(SA_JSON_RAW)
        print("   ✅ Direkt als JSON geladen")
    print(f"   Service Account: {sa_info.get('client_email', '?')}")
except Exception as e:
    print(f"   ❌ Fehler: {e}")
    exit(1)

from google.oauth2.service_account import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Metric, Dimension, OrderBy
)

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
credentials = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
client = BetaAnalyticsDataClient(credentials=credentials)
print("   ✅ Client erstellt")

# 3. Teste verschiedene Datumsbereiche
print(f"\n3️⃣  Teste verschiedene Datumsbereiche...")
date_ranges = [
    ("today", "today", "Heute"),
    ("yesterday", "yesterday", "Gestern"),
    ("7daysAgo", "today", "Letzte 7 Tage"),
    ("30daysAgo", "today", "Letzte 30 Tage"),
]

for start, end, label in date_ranges:
    try:
        req = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date=start, end_date=end)],
            metrics=[
                Metric(name="totalUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
            ],
        )
        resp = client.run_report(req)
        if resp.rows:
            row = resp.rows[0]
            users = row.metric_values[0].value
            sessions = row.metric_values[1].value
            views = row.metric_values[2].value
            print(f"   📅 {label}: {users} Besucher | {sessions} Sessions | {views} Pageviews")
        else:
            print(f"   📅 {label}: Keine Daten (leere Antwort)")
    except Exception as e:
        print(f"   📅 {label}: ❌ {e}")

# 4. Letzte 7 Tage nach Datum aufschlüsseln
print(f"\n4️⃣  Tagesweise Aufschlüsselung (letzte 7 Tage)...")
try:
    req = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="sessions"),
        ],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
    )
    resp = client.run_report(req)
    if resp.rows:
        for row in resp.rows:
            d = row.dimension_values[0].value
            users = row.metric_values[0].value
            sessions = row.metric_values[1].value
            # Datum formatieren: YYYYMMDD → YYYY-MM-DD
            d_fmt = f"{d[:4]}-{d[4:6]}-{d[6:]}"
            print(f"   {d_fmt}: {users} Besucher, {sessions} Sessions")
    else:
        print("   Keine Tagesdaten vorhanden")
except Exception as e:
    print(f"   ❌ {e}")

# 5. Bounce Rate separat testen
print(f"\n5️⃣  Bounce Rate (letzte 7 Tage)...")
try:
    req = RunReportRequest(
        property=PROPERTY_ID,
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
        metrics=[Metric(name="bounceRate")],
    )
    resp = client.run_report(req)
    if resp.rows:
        br = float(resp.rows[0].metric_values[0].value)
        print(f"   Bounce Rate: {round(br * 100, 1)}%")
    else:
        print("   Keine Daten")
except Exception as e:
    print(f"   ❌ bounceRate nicht verfügbar: {e}")
    print("   → Versuche 'engagementRate' als Alternative...")
    try:
        req = RunReportRequest(
            property=PROPERTY_ID,
            date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
            metrics=[Metric(name="engagementRate")],
        )
        resp = client.run_report(req)
        if resp.rows:
            er = float(resp.rows[0].metric_values[0].value)
            print(f"   Engagement Rate: {round(er * 100, 1)}% (= 1 - BounceRate)")
    except Exception as e2:
        print(f"   ❌ {e2}")

# 6. Verfügbare Metriken anzeigen
print(f"\n6️⃣  Teste ob Property ID korrekt ist (Metadata)...")
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import GetMetadataRequest
    meta_req = GetMetadataRequest(name=f"{PROPERTY_ID}/metadata")
    meta = client.get_metadata(meta_req)
    metric_names = [m.api_name for m in meta.metrics[:10]]
    print(f"   ✅ Property erreichbar!")
    print(f"   Erste 10 verfügbare Metriken: {', '.join(metric_names)}")
except Exception as e:
    print(f"   ❌ {e}")

print("\n" + "=" * 60)
print("✅ Test abgeschlossen")
print("=" * 60)
