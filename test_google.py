"""
Schnelltest: Google Service Account + Sheets Verbindung prüfen.

Verwendung:
    python test_google.py
"""
import json
import base64
import sys

from dotenv import load_dotenv
load_dotenv()

import config


def test_credentials():
    """Testet ob die Service Account Credentials gültig sind."""
    print("=" * 50)
    print("🔑 Google Service Account Test")
    print("=" * 50)

    # 1. Env-Variablen prüfen
    if not config.GOOGLE_SERVICE_ACCOUNT_JSON:
        print("❌ GOOGLE_SERVICE_ACCOUNT_JSON ist leer!")
        print("   → Setze die Variable in .env")
        return False

    if not config.GOOGLE_SHEETS_ID:
        print("⚠️  GOOGLE_SHEETS_ID ist leer (Credentials können trotzdem getestet werden)")

    # 2. JSON parsen
    print("\n1️⃣  Credentials parsen...")
    try:
        try:
            sa_json = base64.b64decode(config.GOOGLE_SERVICE_ACCOUNT_JSON)
            sa_info = json.loads(sa_json)
            print("   ✅ Base64-decoded JSON erfolgreich")
        except Exception:
            sa_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_JSON)
            print("   ✅ Raw JSON erfolgreich")

        print(f"   📧 Service Account: {sa_info.get('client_email', '???')}")
        print(f"   🏗️  Projekt: {sa_info.get('project_id', '???')}")
    except Exception as e:
        print(f"   ❌ JSON-Parsing fehlgeschlagen: {e}")
        print("   → Prüfe ob der Wert korrekt base64-encoded ist:")
        print("     base64 -i service-account-key.json | tr -d '\\n'")
        return False

    # 3. Google Auth testen
    print("\n2️⃣  Google Authentifizierung...")
    try:
        from google.oauth2.service_account import Credentials
        credentials = Credentials.from_service_account_info(
            sa_info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        print(f"   ✅ Credentials erstellt (gültig: {credentials.valid or 'wird beim ersten Request aktiviert'})")
    except Exception as e:
        print(f"   ❌ Auth fehlgeschlagen: {e}")
        return False

    # 4. gspread Verbindung testen
    print("\n3️⃣  gspread Client erstellen...")
    try:
        import gspread
        client = gspread.authorize(credentials)
        print("   ✅ gspread Client bereit")
    except Exception as e:
        print(f"   ❌ gspread Fehler: {e}")
        return False

    # 5. Sheet-Zugriff testen
    if config.GOOGLE_SHEETS_ID:
        print(f"\n4️⃣  Google Sheet öffnen ({config.GOOGLE_SHEETS_ID[:20]}...)...")
        try:
            sheet = client.open_by_key(config.GOOGLE_SHEETS_ID)
            print(f"   ✅ Sheet gefunden: '{sheet.title}'")

            worksheets = sheet.worksheets()
            print(f"   📄 Blätter: {[ws.title for ws in worksheets]}")

            # Prüfe ob die erwarteten Blätter existieren
            expected = [config.SHEET_DAILY, config.SHEET_MONTHLY, config.SHEET_TARGETS]
            for name in expected:
                if any(ws.title == name for ws in worksheets):
                    ws = sheet.worksheet(name)
                    rows = len(ws.get_all_values())
                    print(f"   ✅ '{name}' vorhanden ({rows - 1} Datenzeilen)")
                else:
                    print(f"   ⚠️  '{name}' fehlt noch → muss noch angelegt werden")

        except gspread.exceptions.SpreadsheetNotFound:
            print("   ❌ Sheet nicht gefunden!")
            print("   → Prüfe die GOOGLE_SHEETS_ID")
            print("   → Ist das Sheet mit dem Service Account geteilt?")
            print(f"     Teile es mit: {sa_info.get('client_email', '???')}")
            return False
        except gspread.exceptions.APIError as e:
            print(f"   ❌ API-Fehler: {e}")
            print("   → Sind Google Sheets API + Drive API aktiviert?")
            return False
    else:
        print("\n4️⃣  Sheet-Test übersprungen (GOOGLE_SHEETS_ID nicht gesetzt)")

    print("\n" + "=" * 50)
    print("✅ Alles OK!")
    print("=" * 50)
    return True


if __name__ == "__main__":
    success = test_credentials()
    sys.exit(0 if success else 1)
