"""
Stromify KPI Cronjob - Google Analytics Data Fetcher
Verwendet die Google Analytics Data API v4.
"""
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def fetch_ga_data(property_id: str, credentials) -> dict:
    """
    Holt Website-KPIs aus Google Analytics für gestern (vollständiger Tag).

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
            date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
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


def fetch_ga_historical(property_id: str, credentials, days: int = 90) -> dict:
    """
    Holt GA4-Daten tagesweise für die letzten N Tage.

    Returns:
        dict: {date_str (YYYY-MM-DD): {ga_visitors, ga_sessions, ga_bounce_rate}}
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, DateRange, Metric, Dimension, OrderBy
    )

    end = date.today() - timedelta(days=1)  # gestern (heute noch nicht vollständig)
    start = end - timedelta(days=days - 1)

    client = BetaAnalyticsDataClient(credentials=credentials)

    try:
        request = RunReportRequest(
            property=property_id,
            date_ranges=[DateRange(
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d"),
            )],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="totalUsers"),
                Metric(name="sessions"),
                Metric(name="bounceRate"),
            ],
            order_bys=[OrderBy(
                dimension=OrderBy.DimensionOrderBy(dimension_name="date")
            )],
        )
        response = client.run_report(request)

        result = {}
        for row in response.rows:
            raw_date = row.dimension_values[0].value  # YYYYMMDD
            date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            visitors = int(row.metric_values[0].value)
            sessions = int(row.metric_values[1].value)
            bounce_rate = round(float(row.metric_values[2].value) * 100, 1)
            result[date_str] = {
                "ga_visitors": visitors,
                "ga_sessions": sessions,
                "ga_bounce_rate": bounce_rate,
            }

        logger.info(f"GA4 historisch: {len(result)} Tage abgerufen ({start} bis {end})")
        return result

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der GA4-Historik: {e}")
        return {}
