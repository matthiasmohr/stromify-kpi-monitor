"""
Stromify KPI Cronjob - Google Analytics Data Fetcher
Verwendet die Google Analytics Data API v4.
"""
import logging
from datetime import date

logger = logging.getLogger(__name__)


def fetch_ga_data(property_id: str, credentials) -> dict:
    """
    Holt Website-KPIs aus Google Analytics für den heutigen Tag.

    Args:
        property_id: GA4 Property ID (z.B. "properties/123456789")
        credentials: google.oauth2 Credentials Objekt

    Returns:
        dict mit ga_visitors, ga_sessions, ga_bounce_rate
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest,
        DateRange,
        Metric,
        Dimension,
    )

    try:
        client = BetaAnalyticsDataClient(credentials=credentials)

        request = RunReportRequest(
            property=property_id,
            date_ranges=[DateRange(start_date="today", end_date="today")],
            metrics=[
                Metric(name="totalUsers"),
                Metric(name="sessions"),
                Metric(name="bounceRate"),
            ],
        )

        response = client.run_report(request)

        if response.rows:
            row = response.rows[0]
            visitors = int(row.metric_values[0].value)
            sessions = int(row.metric_values[1].value)
            bounce_rate = round(float(row.metric_values[2].value) * 100, 1)
        else:
            visitors = 0
            sessions = 0
            bounce_rate = 0.0

        logger.info(f"GA4 Daten: {visitors} Besucher, {sessions} Sessions, {bounce_rate}% Bounce Rate")

        return {
            "ga_visitors": visitors,
            "ga_sessions": sessions,
            "ga_bounce_rate": bounce_rate,
        }

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der GA4-Daten: {e}")
        return {
            "ga_visitors": 0,
            "ga_sessions": 0,
            "ga_bounce_rate": 0.0,
        }
