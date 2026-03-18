"""
Schnelltest: Notion API - Kunden-DB + Malos-DB verifizieren.
Unterstützt Multi-Source-Datenbanken (API 2025-09-03).

Verwendung:
    python test_notion.py
"""
import sys
import requests
from dotenv import load_dotenv
load_dotenv()

import config

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2025-09-03"


def get_headers():
    return {
        "Authorization": f"Bearer {config.NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def get_data_sources(db_id):
    """Holt Data Source IDs einer Datenbank."""
    resp = requests.get(f"{NOTION_API_URL}/databases/{db_id}", headers=get_headers(), timeout=30)
    if resp.status_code != 200:
        return None, resp
    db = resp.json()
    ds_ids = []
    for ds in db.get("data_sources", []):
        ds_id = ds.get("id") or ds.get("data_source_id")
        if ds_id:
            ds_ids.append(ds_id)
    return ds_ids, resp


def query_all_ds(ds_id):
    """Alle Einträge einer Data Source mit Pagination (POST)."""
    all_results = []
    start_cursor = None
    while True:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = requests.post(
            f"{NOTION_API_URL}/data_sources/{ds_id}/query",
            headers=get_headers(), json=body, timeout=30,
        )
        if resp.status_code != 200:
            print(f"   ❌ Query Fehler: {resp.status_code} - {resp.text[:200]}")
            break
        data = resp.json()
        all_results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
    return all_results


def extract_value(prop):
    """Extrahiert einen lesbaren Wert aus einer Property."""
    ptype = prop.get("type", "")
    if ptype == "number":
        return prop.get("number")
    elif ptype == "title":
        arr = prop.get("title", [])
        return arr[0].get("plain_text", "") if arr else ""
    elif ptype == "rich_text":
        arr = prop.get("rich_text", [])
        return arr[0].get("plain_text", "") if arr else ""
    elif ptype == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None
    elif ptype == "status":
        st = prop.get("status")
        return st.get("name") if st else None
    elif ptype == "rollup":
        rollup = prop.get("rollup", {})
        return rollup.get("number") if rollup.get("type") == "number" else str(rollup)[:80]
    elif ptype == "formula":
        formula = prop.get("formula", {})
        return formula.get("number") if formula.get("type") == "number" else str(formula)[:80]
    elif ptype == "date":
        d = prop.get("date")
        return d.get("start") if d else None
    elif ptype == "relation":
        rels = prop.get("relation", [])
        return f"{len(rels)} Verknüpfung(en)" if rels else "0"
    return f"({ptype})"


def main():
    print("=" * 60)
    print("🔍 Notion API Verification (API 2025-09-03)")
    print("=" * 60)

    api_key = config.NOTION_API_KEY
    if not api_key:
        print("❌ NOTION_API_KEY fehlt")
        sys.exit(1)

    # --- 1. Bot-Check ---
    print("\n1️⃣  Teste API Key...")
    resp = requests.get(f"{NOTION_API_URL}/users/me", headers=get_headers(), timeout=30)
    if resp.status_code == 200:
        print(f"   ✅ Bot: {resp.json().get('name')}")
    else:
        print(f"   ❌ {resp.status_code}: {resp.text[:200]}")
        sys.exit(1)

    # ========================================================
    # KUNDEN-DB: Nur Anzahl zählen
    # ========================================================
    print(f"\n{'='*60}")
    print(f"📂 KUNDEN-Datenbank ({config.NOTION_CUSTOMERS_DB_ID})")
    print(f"{'='*60}")

    ds_ids, resp = get_data_sources(config.NOTION_CUSTOMERS_DB_ID)
    if not ds_ids:
        print(f"   ❌ Fehler: {resp.status_code if hasattr(resp, 'status_code') else resp}")
    else:
        # Erste Data Source = Kunden-Einträge
        kunden = query_all_ds(ds_ids[0])
        print(f"   ✅ Kunden gesamt: {len(kunden)}")

    # ========================================================
    # MALOS-DB: kWh p.a. summieren + unique Kunden zählen
    # ========================================================
    print(f"\n{'='*60}")
    print(f"📂 MALOS-Datenbank ({config.NOTION_MALOS_DB_ID})")
    print(f"{'='*60}")

    malos_db_id = config.NOTION_MALOS_DB_ID
    if not malos_db_id:
        print("   ❌ NOTION_MALOS_DB_ID fehlt in .env")
        sys.exit(1)

    # Verbindung prüfen
    resp = requests.get(f"{NOTION_API_URL}/databases/{malos_db_id}", headers=get_headers(), timeout=30)
    if resp.status_code != 200:
        print(f"   ❌ Zugriff fehlgeschlagen: {resp.status_code} - {resp.text[:300]}")
        print(f"   → Ist die Integration mit der Malos-DB verbunden?")
        sys.exit(1)

    db = resp.json()
    db_title = db["title"][0].get("plain_text", "?") if db.get("title") else "?"
    print(f"   ✅ Datenbank: \"{db_title}\"")

    # Data Sources
    ds_ids_malos, _ = get_data_sources(malos_db_id)
    if not ds_ids_malos:
        print("   ❌ Keine Data Sources gefunden")
        sys.exit(1)

    print(f"   📂 {len(ds_ids_malos)} Data Source(s)")

    # Erste Data Source: Properties anzeigen
    resp = requests.get(f"{NOTION_API_URL}/data_sources/{ds_ids_malos[0]}", headers=get_headers(), timeout=30)
    if resp.status_code == 200:
        ds_props = resp.json().get("properties", {})
        print(f"   Properties ({len(ds_props)}):")
        for name, prop in ds_props.items():
            print(f"     • \"{name}\" → {prop.get('type')}")

    # Erste 3 Einträge anzeigen
    print(f"\n   📋 Erste 3 Einträge:")
    resp = requests.post(
        f"{NOTION_API_URL}/data_sources/{ds_ids_malos[0]}/query",
        headers=get_headers(), json={"page_size": 3}, timeout=30,
    )
    if resp.status_code == 200:
        for i, page in enumerate(resp.json().get("results", [])):
            props = page.get("properties", {})
            print(f"\n      --- Malo {i+1} ---")
            for pname, pval in props.items():
                print(f"        \"{pname}\": {extract_value(pval)}")
    else:
        print(f"   ❌ Query: {resp.status_code} - {resp.text[:200]}")

    # Alle Malos laden
    print(f"\n   📊 Vollständige Zählung...")
    all_malos = query_all_ds(ds_ids_malos[0])
    print(f"   Malos gesamt: {len(all_malos)}")

    # kWh summieren + unique Kunden
    total_kwh = 0.0
    kwh_count = 0
    kunden_set = set()

    for page in all_malos:
        props = page.get("properties", {})

        # JVP (kWh) suchen
        for pname in ["JVP (kWh)", "kWh p.a.", "kWh", "JVP"]:
            kwh_prop = props.get(pname, {})
            if kwh_prop:
                ptype = kwh_prop.get("type", "")
                val = None
                if ptype == "number":
                    val = kwh_prop.get("number")
                elif ptype == "formula":
                    val = kwh_prop.get("formula", {}).get("number")
                elif ptype == "rollup":
                    rollup = kwh_prop.get("rollup", {})
                    val = rollup.get("number") if rollup.get("type") == "number" else None
                if val and val > 0:
                    total_kwh += val
                    kwh_count += 1
                break

        # Kunde (Name oder Relation)
        for pname in ["Kunde", "kunde", "Customer", "Name"]:
            kunde_prop = props.get(pname, {})
            if kunde_prop:
                ptype = kunde_prop.get("type", "")
                if ptype == "title":
                    arr = kunde_prop.get("title", [])
                    if arr:
                        kunden_set.add(arr[0].get("plain_text", ""))
                elif ptype == "rich_text":
                    arr = kunde_prop.get("rich_text", [])
                    if arr:
                        kunden_set.add(arr[0].get("plain_text", ""))
                elif ptype == "relation":
                    rels = kunde_prop.get("relation", [])
                    for rel in rels:
                        kunden_set.add(rel.get("id", ""))
                elif ptype == "select":
                    sel = kunde_prop.get("select")
                    if sel:
                        kunden_set.add(sel.get("name", ""))
                break

    total_gwh = round(total_kwh / 1_000_000, 2)

    print(f"\n   ⚡ JVP Einträge mit Wert > 0: {kwh_count}/{len(all_malos)}")
    print(f"   ⚡ Summe kWh: {total_kwh:,.0f}")
    print(f"   ⚡ Summe GWh: {total_gwh}")
    print(f"   👥 Unique Kunden (aus Malos): {len(kunden_set)}")

    # ========================================================
    # Zusammenfassung
    # ========================================================
    customers_total = len(kunden) if 'kunden' in dir() else 0
    print(f"\n{'='*60}")
    print(f"📋 Ergebnis (für fetch_notion.py):")
    print(f"   notion_customers_total:          {customers_total} (aus Kunden-DB)")
    print(f"   notion_yearly_consumption_gwh:   {total_gwh} (aus Malos-DB)")
    if kunden_set:
        print(f"   (Unique Kunden aus Malos:        {len(kunden_set)})")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
