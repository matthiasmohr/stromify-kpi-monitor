"""
Stromify KPI Cronjob - LinkedIn Data Fetcher
Holt Impressions und Views der LinkedIn Company Page.
"""
import logging
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)

LINKEDIN_API_URL = "https://api.linkedin.com/v2"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Erneuert den Access Token mit dem Refresh Token. Gibt den neuen Access Token zurück."""
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    new_token = data.get("access_token", "")
    logger.info("LinkedIn Access Token erfolgreich erneuert.")
    return new_token


def _get_org_page_statistics(access_token: str, org_id: str) -> dict:
    """Holt die täglichen Page Statistics einer LinkedIn Organisation."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
    }

    # Zeitraum: gestern (LinkedIn liefert heutige Daten oft noch nicht vollständig)
    yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(yesterday.timestamp() * 1000)
    end_ms = int((yesterday + timedelta(days=1)).timestamp() * 1000)

    params = {
        "q": "organization",
        "organization": f"urn:li:organization:{org_id}",
        "timeIntervals.timeGranularityType": "DAY",
        "timeIntervals.timeRange.start": start_ms,
        "timeIntervals.timeRange.end": end_ms,
    }

    response = requests.get(
        f"{LINKEDIN_API_URL}/organizationalEntityShareStatistics",
        headers=headers,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_linkedin_data(
    access_token: str,
    org_id: str,
    client_id: str = "",
    client_secret: str = "",
    refresh_token: str = "",
) -> dict:
    """
    Holt LinkedIn Company Page Impressions und Views.
    Erneuert den Access Token automatisch wenn ein Refresh Token vorhanden ist.

    Returns:
        dict mit li_impressions, li_views
    """
    try:
        try:
            data = _get_org_page_statistics(access_token, org_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401 and client_id and client_secret and refresh_token:
                logger.warning("LinkedIn Access Token abgelaufen, erneuere mit Refresh Token...")
                access_token = refresh_access_token(client_id, client_secret, refresh_token)
                data = _get_org_page_statistics(access_token, org_id)
            else:
                raise

        impressions = 0
        views = 0

        elements = data.get("elements", [])
        for element in elements:
            stats = element.get("totalShareStatistics", {})
            impressions += stats.get("impressionCount", 0)
            views += stats.get("clickCount", 0) + stats.get("engagement", 0)

        logger.info(f"LinkedIn Daten: {impressions} Impressions, {views} Views")

        return {
            "li_impressions": impressions,
            "li_views": views,
        }

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der LinkedIn-Daten: {e}")
        return {
            "li_impressions": 0,
            "li_views": 0,
        }
