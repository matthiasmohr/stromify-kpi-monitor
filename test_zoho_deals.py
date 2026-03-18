"""
Schnelltest: Zoho CRM Deals-Abfrage verifizieren.
Testet genau die Logik aus fetch_zoho.py (mit Paginierung + korrekten Stages).

Verwendung:
    python test_zoho_deals.py
"""
import sys
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import requests
import config

# Stages die als "gewonnen" zählen (identisch mit fetch_zoho.py)
WON_STAGES = [
    "Gewonnen, Freigabe erhalten",
    "Abgeschlossen, gewonnen",
    "Abgewickelt, -> OPS",
]


def refresh_token():
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
    token = response.json().get("access_token")
    if not token:
        print(f"❌ Auth fehlgeschlagen: {response.json()}")
        sys.exit(1)
    return token


def api_get(token, path, params=None):
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    return requests.get(
        f"{config.ZOHO_API_DOMAIN}/crm/v8/{path}",
        headers=headers, params=params or {}, timeout=30,
    )


def count_all_pages(token, path, params=None):
    """Zählt alle Records über Paginierung hinweg."""
    params = dict(params or {})
    params["per_page"] = "200"
    total = 0
    page = 1
    while True:
        params["page"] = str(page)
        resp = api_get(token, path, params)
        if resp.status_code == 204:
            break
        resp.raise_for_status()
        data = resp.json()
        records = data.get("data", [])
        total += len(records)
        if not data.get("info", {}).get("more_records", False):
            break
        page += 1
    return total


def main():
    print("=" * 60)
    print("🔍 Zoho Deals Verification (v2)")
    print("=" * 60)

    token = refresh_token()
    print("✅ Auth OK\n")

    # --- 1. Deals Gesamt (mit Paginierung) ---
    print("1️⃣  Deals Gesamt (paginiert)...")
    total = count_all_pages(token, "Deals", {"fields": "id"})
    print(f"   → {total} Deals gesamt")

    # --- 2. Neue Deals heute (DateTime-Format) ---
    today_start = datetime.now().strftime("%Y-%m-%dT00:00:00+02:00")
    today_end = datetime.now().strftime("%Y-%m-%dT23:59:59+02:00")
    print(f"\n2️⃣  Neue Deals heute ({datetime.now().strftime('%Y-%m-%d')})...")
    resp = api_get(token, "Deals/search", {
        "criteria": f"(Created_Time:greater_equal:{today_start})and(Created_Time:less_equal:{today_end})",
        "fields": "Deal_Name,Stage,Created_Time",
    })
    if resp.status_code == 200:
        deals = resp.json().get("data", [])
        print(f"   → {len(deals)} neue Deals heute")
        for d in deals:
            print(f'     • "{d.get("Deal_Name")}" | Stage: "{d.get("Stage")}"')
    elif resp.status_code == 204:
        print("   → 0 neue Deals heute")
    else:
        print(f"   ❌ Fehler {resp.status_code}: {resp.text[:300]}")

    # --- 3. Gewonnene Deals (3 Stages) ---
    print(f"\n3️⃣  Gewonnene Deals (3 Stages)...")
    deals_won_total = 0
    for stage in WON_STAGES:
        resp = api_get(token, "Deals/search", {
            "criteria": f"(Stage:equals:{stage})",
            "fields": "Deal_Name,Stage,Amount,Closing_Date",
        })
        if resp.status_code == 200:
            deals = resp.json().get("data", [])
            deals_won_total += len(deals)
            print(f'   "{stage}": {len(deals)} Deals')
            for d in deals:
                print(f'     • "{d.get("Deal_Name")}" | Betrag: {d.get("Amount")} | Closing: {d.get("Closing_Date")}')
        elif resp.status_code == 204:
            print(f'   "{stage}": 0 Deals')
        else:
            print(f'   "{stage}": ❌ Fehler {resp.status_code}: {resp.text[:200]}')

    print(f"\n   📊 Gewonnen gesamt: {deals_won_total}")

    # --- 4. Zusammenfassung (wie fetch_zoho.py liefern würde) ---
    print(f"\n{'=' * 60}")
    print(f"📋 Ergebnis (wie fetch_zoho.py):")
    print(f"   zoho_deals_total:  {total}")
    print(f"   zoho_deals_new:    (siehe oben)")
    print(f"   zoho_deals_won:    {deals_won_total}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
