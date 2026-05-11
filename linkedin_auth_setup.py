"""
LinkedIn OAuth2 Setup - Einmaliger Autorisierungsflow.

Führe dieses Skript einmal aus, um Access Token + Refresh Token zu erhalten.
Die Tokens werden anschließend in der .env Datei gespeichert.

Verwendung:
    python linkedin_auth_setup.py
"""
import os
import sys
import json
import urllib.parse
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET", "")
REDIRECT_URI = "http://localhost:8765/callback"

SCOPES = [
    "r_organization_social",   # Impressions, Shares, Engagement
    "r_organization_admin",    # Page Views
    "rw_organization_admin",   # (Community App benötigt dies oft)
]

AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"

received_code = None


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global received_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            received_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<h1>Autorisierung erfolgreich!</h1><p>Du kannst dieses Fenster schliessen.</p>")
        elif "error" in params:
            error = params.get("error", [""])[0]
            desc = params.get("error_description", [""])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"<h1>Fehler: {error}</h1><p>{desc}</p>".encode())
        else:
            self.send_response(200)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Kein HTTP-Log-Output


def get_auth_url():
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": " ".join(SCOPES),
        "state": "stromify_kpi",
    }
    return f"{AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_tokens(code: str) -> dict:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Fehler: LINKEDIN_CLIENT_ID und LINKEDIN_CLIENT_SECRET muessen in der .env gesetzt sein.")
        sys.exit(1)

    print("=" * 60)
    print("LinkedIn OAuth2 Setup")
    print("=" * 60)
    print()
    print("Oeffne diese URL in deinem Browser und autorisiere die App:")
    print()
    auth_url = get_auth_url()
    print(auth_url)
    print()
    print("Warte auf Callback auf http://localhost:8765 ...")

    server = HTTPServer(("localhost", 8765), CallbackHandler)
    server.handle_request()  # Wartet auf genau einen Request

    if not received_code:
        print("Kein Autorisierungscode erhalten.")
        sys.exit(1)

    print()
    print("Autorisierungscode empfangen. Tausche gegen Tokens...")

    try:
        tokens = exchange_code_for_tokens(received_code)
    except requests.HTTPError as e:
        print(f"Fehler beim Token-Austausch: {e}")
        print(e.response.text)
        sys.exit(1)

    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    expires_in = tokens.get("expires_in", 0)
    refresh_expires_in = tokens.get("refresh_token_expires_in", 0)

    print()
    print("=" * 60)
    print("Tokens erhalten!")
    print("=" * 60)
    print(f"Access Token (laeuft in {expires_in // 3600:.0f}h ab): {access_token[:40]}...")
    if refresh_token:
        print(f"Refresh Token (laeuft in {refresh_expires_in // 86400:.0f} Tagen ab): {refresh_token[:40]}...")
    else:
        print("Kein Refresh Token erhalten (pruefe App-Einstellungen).")
    print()
    print("Fuege folgende Zeilen zu deiner .env Datei / Railway-Konfiguration hinzu:")
    print()
    print(f"LINKEDIN_CLIENT_ID={CLIENT_ID}")
    print(f"LINKEDIN_CLIENT_SECRET={CLIENT_SECRET}")
    print(f"LINKEDIN_ACCESS_TOKEN={access_token}")
    if refresh_token:
        print(f"LINKEDIN_REFRESH_TOKEN={refresh_token}")
    print()

    # Org ID aus der API ermitteln
    print("Ermittle deine Organisation ID...")
    try:
        resp = requests.get(
            "https://api.linkedin.com/v2/organizationAcls",
            headers={
                "Authorization": f"Bearer {access_token}",
                "X-Restli-Protocol-Version": "2.0.0",
            },
            params={"q": "roleAssignee"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        elements = data.get("elements", [])
        if elements:
            print("Gefundene Organisationen:")
            for el in elements:
                org_urn = el.get("organization", "")
                org_id = org_urn.split(":")[-1] if org_urn else "?"
                role = el.get("role", "")
                print(f"  - ID: {org_id}  (Role: {role})  → LINKEDIN_ORG_ID={org_id}")
        else:
            print("Keine Organisationen gefunden. Pruefe die App-Scopes.")
    except Exception as e:
        print(f"Konnte Org ID nicht automatisch ermitteln: {e}")
        print("Setze LINKEDIN_ORG_ID manuell (numerische ID aus der LinkedIn URL).")


if __name__ == "__main__":
    main()
