"""
Stromify KPI Cronjob - Notion Data Fetcher
Holt Kundenzahl aus Kunden-DB und Yearly Consumption GWh aus Malos-DB.
Verwendet API-Version 2025-09-03 für Multi-Source-Datenbanken.
"""
import logging
import requests

logger = logging.getLogger(__name__)

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2025-09-03"


def _get_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def _get_first_data_source_id(api_key: str, database_id: str) -> str | None:
    """Holt die erste Data Source ID einer Datenbank."""
    resp = requests.get(
        f"{NOTION_API_URL}/databases/{database_id}",
        headers=_get_headers(api_key),
        timeout=30,
    )
    resp.raise_for_status()
    db = resp.json()
    for ds in db.get("data_sources", []):
        ds_id = ds.get("id") or ds.get("data_source_id")
        if ds_id:
            return ds_id
    return None


def _query_all(api_key: str, data_source_id: str) -> list:
    """Fragt eine Data Source ab und gibt alle Ergebnisse zurück (mit Pagination)."""
    headers = _get_headers(api_key)
    all_results = []
    start_cursor = None

    while True:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        response = requests.post(
            f"{NOTION_API_URL}/data_sources/{data_source_id}/query",
            headers=headers,
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        all_results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")

    return all_results


def fetch_notion_data(api_key: str, database_id: str, malos_db_id: str = "") -> dict:
    """
    Holt Kundenzahl und Yearly Consumption GWh aus Notion.

    - Kunden-DB: Einträge zählen = Kunden gesamt
    - Malos-DB: "JVP (kWh)" summieren und in GWh umrechnen

    Args:
        api_key: Notion Integration API Key
        database_id: ID der Kunden-Datenbank
        malos_db_id: ID der Malos-Datenbank

    Returns:
        dict mit notion_customers_total, notion_yearly_consumption_gwh
    """
    try:
        # --- Kunden zählen ---
        ds_id = _get_first_data_source_id(api_key, database_id)
        if not ds_id:
            raise ValueError(f"Keine Data Source in Kunden-DB {database_id}")

        kunden = _query_all(api_key, ds_id)
        customers_total = len(kunden)

        # --- GWh aus Malos-DB ---
        total_gwh = 0.0
        if malos_db_id:
            malos_ds_id = _get_first_data_source_id(api_key, malos_db_id)
            if not malos_ds_id:
                raise ValueError(f"Keine Data Source in Malos-DB {malos_db_id}")

            malos = _query_all(api_key, malos_ds_id)
            total_kwh = 0.0
            for page in malos:
                props = page.get("properties", {})
                kwh_prop = props.get("JVP (kWh)", {})
                if kwh_prop.get("type") == "number":
                    val = kwh_prop.get("number")
                    if val and val > 0:
                        total_kwh += val
            total_gwh = round(total_kwh / 1_000_000, 2)

        logger.info(f"Notion Daten: {customers_total} Kunden, {total_gwh} GWh Jahresverbrauch")

        return {
            "notion_customers_total": customers_total,
            "notion_yearly_consumption_gwh": total_gwh,
        }

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Notion-Daten: {e}")
        return {
            "notion_customers_total": 0,
            "notion_yearly_consumption_gwh": 0.0,
        }
