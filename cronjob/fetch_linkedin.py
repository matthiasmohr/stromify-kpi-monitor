"""
Stromify KPI Cronjob - LinkedIn Data Fetcher
Holt Impressions und Views der LinkedIn Company Page.
"""
import logging
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)

LINKEDIN_API_URL = "https://api.linkedin.com/v2"


def _get_org_page_statistics(access_token: str, org_id: str) -> dict:
    """Holt die täglichen Page Statistics einer LinkedIn Organisation."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
    }

    # Zeitraum: heute (Unix Timestamps in Millisekunden)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(today.timestamp() * 1000)
    end_ms = int((today + timedelta(days=1)).timestamp() * 1000)

    # Organization Page Statistics
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


def fetch_linkedin_data(access_token: str, org_id: str) -> dict:
    """
    Holt LinkedIn Company Page Impressions und Views.

    Args:
        access_token: LinkedIn OAuth2 Access Token
        org_id: LinkedIn Organization ID

    Returns:
        dict mit li_impressions, li_views
    """
    try:
        data = _get_org_page_statistics(access_token, org_id)

        impressions = 0
        views = 0

        elements = data.get("elements", [])
        for element in elements:
            stats = element.get("totalShareStatistics", {})
            impressions += stats.get("impressionCount", 0)
            views += stats.get("clickCount", 0) + stats.get("engagement", 0)

            # Page views aus pageStatistics
            page_stats = element.get("pageStatistics", {})
            page_views = page_stats.get("views", {})
            views += (
                page_views.get("allPageViews", {}).get("pageViews", 0)
            )

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
