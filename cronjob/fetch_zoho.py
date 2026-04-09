"""
Stromify KPI Cronjob - Zoho CRM Data Fetcher
Holt Deal-Pipeline und Conversion-Daten aus Zoho CRM.
"""
import logging
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)


def _refresh_access_token(client_id: str, client_secret: str, refresh_token: str, accounts_url: str) -> str:
    """Erneuert den Zoho Access Token via Refresh Token."""
    logger.info(f"Zoho Token-Refresh: accounts_url={accounts_url}, client_id={client_id[:8]}...")
    response = requests.post(
        f"{accounts_url}/oauth/v2/token",
        params={
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    if response.status_code != 200:
        logger.error(f"Zoho Token-Refresh fehlgeschlagen: HTTP {response.status_code}, Body: {response.text}")
        response.raise_for_status()
    data = response.json()

    access_token = data.get("access_token")
    if not access_token:
        raise ValueError(f"Kein Access Token erhalten: {data}")

    return access_token


def _get_records_count(api_domain: str, access_token: str, module: str, criteria: str = "") -> int:
    """Zählt Datensätze in einem Zoho CRM Modul (mit Paginierung)."""
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    params = {"fields": "id", "per_page": "200"}
    if criteria:
        params["criteria"] = criteria

    total = 0
    page = 1
    while True:
        params["page"] = str(page)
        url = f"{api_domain}/crm/v8/{module}/search" if criteria else f"{api_domain}/crm/v8/{module}"
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 204:
            break

        response.raise_for_status()
        data = response.json()
        records = data.get("data", [])
        total += len(records)

        info = data.get("info", {})
        if not info.get("more_records", False):
            break
        page += 1

    return total


def _get_records(api_domain: str, access_token: str, module: str, criteria: str = "") -> list:
    """Holt alle Datensätze aus einem Zoho CRM Modul (mit Paginierung)."""
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    params = {"per_page": "200"}
    if criteria:
        params["criteria"] = criteria

    all_records = []
    page = 1
    while True:
        params["page"] = str(page)
        url = f"{api_domain}/crm/v8/{module}/search" if criteria else f"{api_domain}/crm/v8/{module}"
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 204:
            break

        response.raise_for_status()
        data = response.json()
        all_records.extend(data.get("data", []))

        info = data.get("info", {})
        if not info.get("more_records", False):
            break
        page += 1

    return all_records


def fetch_zoho_all_leads(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    api_domain: str = "https://www.zohoapis.eu",
    accounts_url: str = "https://accounts.zoho.eu",
    access_token: str = None,
) -> list:
    """
    Holt ALLE Deals aus Zoho mit Status-Attribut.
    Status: "new" | "active" | "won" | "lost" | "waiting"
    "new" = aktiv + in den letzten 14 Tagen erstellt
    "active" = aktiv + älter als 14 Tage

    access_token: optional vorher geholter Token (verhindert doppelten Token-Refresh)
    """
    WON_STAGES = ["Gewonnen, Freigabe erhalten", "Abgeschlossen, gewonnen", "Abgewickelt, -> OPS"]
    LOST_STAGES = ["Abgeschlossen, verloren"]
    WAITING_STAGES = ["Warteschleife"]
    cutoff_14d = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")

    try:
        if not access_token:
            access_token = _refresh_access_token(client_id, client_secret, refresh_token, accounts_url)
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        all_deals = []
        page = 1
        while True:
            params = {
                "fields": "Deal_Name,Account_Name,Stage,Amount,Created_Time,Closing_Date,Pipeline",
                "per_page": "200",
                "page": str(page),
            }
            response = requests.get(f"{api_domain}/crm/v8/Deals", headers=headers, params=params, timeout=30)
            if response.status_code == 204:
                break
            response.raise_for_status()
            data = response.json()
            all_deals.extend(data.get("data", []))
            if not data.get("info", {}).get("more_records", False):
                break
            page += 1

        # Nur Deals der Pipeline "Energie" berücksichtigen
        energie_deals = [d for d in all_deals if d.get("Pipeline", "") == "Energie"]
        logger.info(f"Zoho Deals gesamt: {len(all_deals)}, davon Pipeline 'Energie': {len(energie_deals)}")

        result = []
        for deal in energie_deals:
            stage = deal.get("Stage", "")
            created_str = deal.get("Created_Time", "")[:10]
            account = deal.get("Account_Name")

            if stage in WON_STAGES:
                status = "won"
            elif stage in LOST_STAGES:
                status = "lost"
            elif stage in WAITING_STAGES:
                status = "waiting"
            elif created_str >= cutoff_14d:
                status = "new"
            else:
                status = "active"

            result.append({
                "name": deal.get("Deal_Name", ""),
                "company": account.get("name", "") if isinstance(account, dict) else str(account or ""),
                "stage": stage,
                "status": status,
                "amount": deal.get("Amount"),
                "created_date": created_str,
                "closing_date": deal.get("Closing_Date", ""),
            })

        counts = {s: sum(1 for d in result if d["status"] == s) for s in ("new", "active", "won", "lost", "waiting")}
        logger.info(f"Zoho alle Leads: {len(result)} gesamt – {counts}")
        return result

    except Exception as e:
        logger.error(f"Fehler beim Abrufen aller Zoho-Leads: {e}")
        return []


def fetch_zoho_data(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    api_domain: str = "https://www.zohoapis.eu",
    accounts_url: str = "https://accounts.zoho.eu",
    access_token: str = None,
) -> tuple:
    """
    Holt Deal-Pipeline und Conversion-Daten aus Zoho CRM.

    Returns:
        (dict mit zoho_deals_total, access_token) – Token wird zurückgegeben
        damit fetch_zoho_all_leads ihn wiederverwenden kann (kein doppelter Refresh).
    """
    try:
        if not access_token:
            access_token = _refresh_access_token(client_id, client_secret, refresh_token, accounts_url)
        deals_total = _get_records_count(api_domain, access_token, "Deals", criteria="(Pipeline:equals:Energie)")
        logger.info(f"Zoho Daten: {deals_total} Deals (Pipeline Energie)")
        return {"zoho_deals_total": deals_total}, access_token

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Zoho-Daten: {e}")
        return {"zoho_deals_total": 0}, None
