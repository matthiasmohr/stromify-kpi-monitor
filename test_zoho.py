"""
Schnelltest: Zoho CRM Verbindung prüfen und Feldnamen/Stages herausfinden.

Verwendung:
    python test_zoho.py
"""
import sys
import json

from dotenv import load_dotenv
load_dotenv()

import requests
import config


def refresh_token():
    """Holt einen frischen Access Token."""
    response = requests.post(
        f"{config.ZOHO_ACCOUNTS_URL}/oauth/v2/token",
        params={
            "refresh_token": config.ZOHO_REFRESH_TOKEN,
            "client_id": config.ZOHO_CLIENT_ID,
            "client_secret": config.ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    token = data.get("access_token")
    if not token:
        print(f"❌ Kein Access Token erhalten: {data}")
        sys.exit(1)
    return token


def api_get(access_token, path, params=None):
    """Einfacher GET-Request an die Zoho CRM API."""
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    url = f"{config.ZOHO_API_DOMAIN}/crm/v8/{path}"
    response = requests.get(url, headers=headers, params=params or {}, timeout=30)
    return response


def main():
    print("=" * 60)
    print("🔍 Zoho CRM Explorer")
    print("=" * 60)

    # 1. Auth testen
    print("\n1️⃣  Authentifizierung...")
    if not all([config.ZOHO_CLIENT_ID, config.ZOHO_CLIENT_SECRET, config.ZOHO_REFRESH_TOKEN]):
        print("❌ Zoho Credentials nicht in .env konfiguriert")
        sys.exit(1)

    access_token = refresh_token()
    print(f"   ✅ Access Token erhalten")

    # 2. Verfügbare Module auflisten
    print("\n2️⃣  Verfügbare Module...")
    resp = api_get(access_token, "settings/modules")
    if resp.status_code == 200:
        modules = resp.json().get("modules", [])
        for m in modules:
            api_name = m.get("api_name", "")
            plural = m.get("plural_label", "")
            singular = m.get("singular_label", "")
            count = m.get("record_count", "?")
            if api_name in ["Leads", "Deals", "Contacts", "Accounts"]:
                print(f"   📋 {api_name} ({singular} / {plural})")
    else:
        print(f"   ⚠️  Module konnten nicht geladen werden: {resp.status_code}")

    # 3. Deals (Abschlüsse) untersuchen
    print("\n3️⃣  Deals / Abschlüsse - Felder untersuchen...")
    resp = api_get(access_token, "settings/fields", {"module": "Deals"})
    if resp.status_code == 200:
        fields = resp.json().get("fields", [])
        print(f"   Gefundene Felder: {len(fields)}")
        print()
        print("   Wichtige Felder:")
        print("   " + "-" * 50)
        for f in fields:
            api_name = f.get("api_name", "")
            display = f.get("display_label", "")
            field_type = f.get("data_type", "")

            # Nur relevante Felder anzeigen
            if api_name in ["Stage", "Status", "Deal_Name", "Amount", "Closing_Date",
                            "Created_Time", "Pipeline", "Probability"]:
                print(f"   • {api_name} ({display}) [{field_type}]")

                # Bei Stage/Status: Picklist-Werte anzeigen
                if field_type == "picklist" and f.get("pick_list_values"):
                    for v in f["pick_list_values"]:
                        print(f"     → \"{v.get('display_value', '')}\" (api: \"{v.get('actual_value', '')}\")")
    else:
        print(f"   ⚠️  Deal-Felder konnten nicht geladen werden: {resp.status_code}")
        print(f"   Response: {resp.text[:500]}")

    # 4. Leads untersuchen
    print("\n4️⃣  Leads - Felder untersuchen...")
    resp = api_get(access_token, "settings/fields", {"module": "Leads"})
    if resp.status_code == 200:
        fields = resp.json().get("fields", [])
        print(f"   Gefundene Felder: {len(fields)}")
        print()
        print("   Wichtige Felder:")
        print("   " + "-" * 50)
        for f in fields:
            api_name = f.get("api_name", "")
            display = f.get("display_label", "")
            field_type = f.get("data_type", "")

            if api_name in ["Lead_Status", "Status", "Lead_Source", "Created_Time",
                            "First_Name", "Last_Name", "Company"]:
                print(f"   • {api_name} ({display}) [{field_type}]")
                if field_type == "picklist" and f.get("pick_list_values"):
                    for v in f["pick_list_values"]:
                        print(f"     → \"{v.get('display_value', '')}\" (api: \"{v.get('actual_value', '')}\")")
    else:
        print(f"   ⚠️  Lead-Felder konnten nicht geladen werden: {resp.status_code}")

    # 5. Erste Deals anzeigen
    print("\n5️⃣  Die letzten 5 Deals...")
    resp = api_get(access_token, "Deals", {"fields": "Deal_Name,Stage,Amount,Closing_Date,Created_Time", "per_page": "5", "sort_by": "Created_Time", "sort_order": "desc"})
    if resp.status_code == 200:
        deals = resp.json().get("data", [])
        for d in deals:
            name = d.get("Deal_Name", "?")
            stage = d.get("Stage", "?")
            amount = d.get("Amount", "?")
            closing = d.get("Closing_Date", "?")
            print(f"   • \"{name}\" | Stage: \"{stage}\" | Betrag: {amount} | Closing: {closing}")
        if not deals:
            print("   (keine Deals vorhanden)")
    elif resp.status_code == 204:
        print("   (keine Deals vorhanden)")
    else:
        print(f"   ⚠️  Deals konnten nicht geladen werden: {resp.status_code}")
        print(f"   Response: {resp.text[:500]}")

    # 6. Erste Leads anzeigen
    print("\n6️⃣  Die letzten 5 Leads...")
    resp = api_get(access_token, "Leads", {"fields": "First_Name,Last_Name,Company,Lead_Status,Created_Time", "per_page": "5", "sort_by": "Created_Time", "sort_order": "desc"})
    if resp.status_code == 200:
        leads = resp.json().get("data", [])
        for l in leads:
            name = f"{l.get('First_Name', '')} {l.get('Last_Name', '')}".strip()
            company = l.get("Company", "?")
            status = l.get("Lead_Status", "?")
            print(f"   • \"{name}\" ({company}) | Status: \"{status}\"")
        if not leads:
            print("   (keine Leads vorhanden)")
    elif resp.status_code == 204:
        print("   (keine Leads vorhanden)")
    else:
        print(f"   ⚠️  Leads konnten nicht geladen werden: {resp.status_code}")
        print(f"   Response: {resp.text[:500]}")

    print("\n" + "=" * 60)
    print("Fertig! Schick mir die Ausgabe, dann passe ich den Code an.")
    print("=" * 60)


if __name__ == "__main__":
    main()
