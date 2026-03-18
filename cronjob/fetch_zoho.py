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


def fetch_zoho_data(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    api_domain: str = "https://www.zohoapis.eu",
    accounts_url: str = "https://accounts.zoho.eu",
) -> dict:
    """
    Holt Deal-Pipeline und Conversion-Daten aus Zoho CRM.

    Args:
        client_id: Zoho OAuth Client ID
        client_secret: Zoho OAuth Client Secret
        refresh_token: Zoho OAuth Refresh Token
        api_domain: Zoho API Domain (EU/US)
        accounts_url: Zoho Accounts URL (EU/US)

    Returns:
        dict mit zoho_deals_new, zoho_deals_total, zoho_deals_won
    """
    # Stages die als "gewonnen" zählen
    WON_STAGES = [
        "Gewonnen, Freigabe erhalten",
        "Abgeschlossen, gewonnen",
        "Abgewickelt, -> OPS",
    ]

    try:
        # Access Token erneuern
        access_token = _refresh_access_token(client_id, client_secret, refresh_token, accounts_url)

        # Heute (DateTime-Format für Created_Time)
        today_start = datetime.now().strftime("%Y-%m-%dT00:00:00+02:00")
        today_end = datetime.now().strftime("%Y-%m-%dT23:59:59+02:00")

        # Deals Gesamt
        deals_total = _get_records_count(api_domain, access_token, "Deals")

        # Neue Deals (heute erstellt)
        new_deals_criteria = f"(Created_Time:greater_equal:{today_start})and(Created_Time:less_equal:{today_end})"
        deals_new = _get_records_count(api_domain, access_token, "Deals", new_deals_criteria)

        # Gewonnene Deals (alle 3 gewonnenen Stages zusammenzählen)
        deals_won = 0
        for stage in WON_STAGES:
            won_criteria = f"(Stage:equals:{stage})"
            deals_won += _get_records_count(api_domain, access_token, "Deals", won_criteria)

        logger.info(
            f"Zoho Daten: {deals_new} neue Deals, {deals_total} gesamt, "
            f"{deals_won} gewonnen"
        )

        return {
            "zoho_deals_new": deals_new,
            "zoho_deals_total": deals_total,
            "zoho_deals_won": deals_won,
        }

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Zoho-Daten: {e}")
        return {
            "zoho_deals_new": 0,
            "zoho_deals_total": 0,
            "zoho_deals_won": 0,
        }
