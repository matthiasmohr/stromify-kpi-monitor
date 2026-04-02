"""
Stromify KPI Cronjob - Auth0 Data Fetcher
Holt Monthly Active Users über die Auth0 Management API.
"""
import logging
import requests

logger = logging.getLogger(__name__)


def fetch_auth0_data(domain: str, client_id: str, client_secret: str) -> dict:
    """
    Holt Monthly Active Users aus Auth0.

    Args:
        domain: Auth0 Domain (z.B. 'stromify.eu.auth0.com')
        client_id: M2M App Client ID
        client_secret: M2M App Client Secret

    Returns:
        dict mit auth0_mau
    """
    try:
        # 1. Access Token holen (Client Credentials Flow)
        token_resp = requests.post(
            f"https://{domain}/oauth/token",
            json={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "audience": f"https://{domain}/api/v2/",
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        # 2. Active Users abrufen
        stats_resp = requests.get(
            f"https://{domain}/api/v2/stats/active-users",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        stats_resp.raise_for_status()
        mau = int(stats_resp.json())

        logger.info(f"Auth0 Daten: {mau} Monthly Active Users")
        return {"auth0_mau": mau}

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Auth0-Daten: {e}")
        return {"auth0_mau": 0}
