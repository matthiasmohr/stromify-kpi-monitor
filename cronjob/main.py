"""
Stromify KPI Cronjob - Orchestrator
Ruft alle Datenquellen ab und schreibt die KPIs in Google Sheets.

Verwendung:
    # Einmalig ausführen:
    python -m cronjob.main

    # Als Cronjob (z.B. täglich um 22:00):
    0 22 * * * cd /path/to/stromify-kpi-monitor && python -m cronjob.main

    # Oder als dauerhafter Prozess mit Schedule:
    python -m cronjob.main --schedule
"""
import sys
import json
import base64
import logging
import argparse
from datetime import datetime

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("cronjob")

# Projektroot zum Pfad hinzufügen
sys.path.insert(0, ".")

import config
from cronjob.fetch_ga import fetch_ga_data, fetch_ga_historical
from cronjob.fetch_notion import fetch_notion_data
from cronjob.fetch_zoho import fetch_zoho_data, fetch_zoho_all_leads
from cronjob.fetch_linkedin import fetch_linkedin_data
from cronjob.fetch_auth0 import fetch_auth0_data
from cronjob.sheet_writer import write_daily_row, update_monthly_aggregation, backfill_ga_rows, write_active_leads


def _get_google_credentials():
    """Erstellt Google OAuth2 Credentials für GA4."""
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/analytics.readonly"]

    try:
        sa_json = base64.b64decode(config.GOOGLE_SERVICE_ACCOUNT_JSON)
        sa_info = json.loads(sa_json)
    except Exception:
        sa_info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_JSON)

    return Credentials.from_service_account_info(sa_info, scopes=scopes)


def run_fetch():
    """Führt einen kompletten KPI-Fetch-Zyklus durch."""
    logger.info("=" * 60)
    logger.info(f"KPI-Fetch gestartet: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    all_data = {}
    errors = []

    # 1. Google Analytics
    logger.info("📊 Hole Google Analytics Daten...")
    if config.GA_PROPERTY_ID and config.GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            credentials = _get_google_credentials()
            ga_data = fetch_ga_data(config.GA_PROPERTY_ID, credentials)
            all_data.update(ga_data)
        except Exception as e:
            errors.append(f"GA4: {e}")
            logger.error(f"GA4 Fehler: {e}")
    else:
        logger.warning("GA4 nicht konfiguriert (GA_PROPERTY_ID oder GOOGLE_SERVICE_ACCOUNT_JSON fehlt)")

    # 2. Notion
    logger.info("📝 Hole Notion Daten...")
    if config.NOTION_API_KEY and config.NOTION_CUSTOMERS_DB_ID:
        try:
            notion_data = fetch_notion_data(
                config.NOTION_API_KEY,
                config.NOTION_CUSTOMERS_DB_ID,
                config.NOTION_MALOS_DB_ID,
            )
            all_data.update(notion_data)
        except Exception as e:
            errors.append(f"Notion: {e}")
            logger.error(f"Notion Fehler: {e}")
    else:
        logger.warning("Notion nicht konfiguriert (NOTION_API_KEY oder NOTION_CUSTOMERS_DB_ID fehlt)")

    # 3. Zoho CRM
    logger.info("🎯 Hole Zoho CRM Daten...")
    active_leads = []
    if config.ZOHO_CLIENT_ID and config.ZOHO_CLIENT_SECRET and config.ZOHO_REFRESH_TOKEN:
        try:
            zoho_data = fetch_zoho_data(
                config.ZOHO_CLIENT_ID,
                config.ZOHO_CLIENT_SECRET,
                config.ZOHO_REFRESH_TOKEN,
                config.ZOHO_API_DOMAIN,
                config.ZOHO_ACCOUNTS_URL,
            )
            all_data.update(zoho_data)
        except Exception as e:
            errors.append(f"Zoho: {e}")
            logger.error(f"Zoho Fehler: {e}")

        try:
            # Alle Leads mit Status für Dashboard-Tabelle (separat, damit Sheets-Fehler
            # nicht als Zoho-Fehler geloggt werden)
            active_leads = fetch_zoho_all_leads(
                config.ZOHO_CLIENT_ID,
                config.ZOHO_CLIENT_SECRET,
                config.ZOHO_REFRESH_TOKEN,
                config.ZOHO_API_DOMAIN,
                config.ZOHO_ACCOUNTS_URL,
            )
            # Status-Counts für tägliche Historisierung
            if active_leads:
                status_counts = {}
                for lead in active_leads:
                    s = lead.get("status", "")
                    status_counts[s] = status_counts.get(s, 0) + 1
                all_data["zoho_deals_new"] = status_counts.get("new", 0)
                all_data["zoho_deals_active"] = status_counts.get("active", 0)
                all_data["zoho_deals_won"] = status_counts.get("won", 0)
                all_data["zoho_deals_lost"] = status_counts.get("lost", 0)
                all_data["zoho_deals_waiting"] = status_counts.get("waiting", 0)
                logger.info(f"Zoho Status-Counts: {status_counts}")
        except Exception as e:
            errors.append(f"Zoho Leads: {e}")
            logger.error(f"Zoho Leads Fehler: {e}")
    else:
        logger.warning("Zoho nicht konfiguriert (ZOHO_CLIENT_ID, SECRET oder REFRESH_TOKEN fehlt)")

    # 4. LinkedIn
    logger.info("💼 Hole LinkedIn Daten...")
    if (
        config.LINKEDIN_ACCESS_TOKEN
        and config.LINKEDIN_ORG_ID
        and config.LINKEDIN_ACCESS_TOKEN != "your_access_token"
        and config.LINKEDIN_ORG_ID != "your_organization_id"
    ):
        try:
            li_data = fetch_linkedin_data(config.LINKEDIN_ACCESS_TOKEN, config.LINKEDIN_ORG_ID)
            all_data.update(li_data)
        except Exception as e:
            errors.append(f"LinkedIn: {e}")
            logger.error(f"LinkedIn Fehler: {e}")
    else:
        logger.warning("LinkedIn nicht konfiguriert – übersprungen")

    # 5. Auth0
    logger.info("📱 Hole Auth0 Daten...")
    if config.AUTH0_DOMAIN and config.AUTH0_CLIENT_ID and config.AUTH0_CLIENT_SECRET:
        try:
            auth0_data = fetch_auth0_data(
                config.AUTH0_DOMAIN,
                config.AUTH0_CLIENT_ID,
                config.AUTH0_CLIENT_SECRET,
            )
            all_data.update(auth0_data)
        except Exception as e:
            errors.append(f"Auth0: {e}")
            logger.error(f"Auth0 Fehler: {e}")
    else:
        logger.warning("Auth0 nicht konfiguriert – übersprungen")

    # 6. In Google Sheets schreiben
    logger.info("📝 Schreibe Daten in Google Sheets...")
    if config.GOOGLE_SHEETS_ID and config.GOOGLE_SERVICE_ACCOUNT_JSON:
        # 5a. KPI-Tagesdaten
        if all_data:
            try:
                write_daily_row(all_data)
                update_monthly_aggregation()
                logger.info("✅ KPI-Daten erfolgreich geschrieben")
            except Exception as e:
                errors.append(f"Sheet Writer: {e}")
                logger.error(f"Sheet Writer Fehler: {e}")

        # 5b. Zoho Lead-Liste (separat, damit Fehler hier nicht alles blockieren)
        if active_leads:
            try:
                write_active_leads(active_leads)
                logger.info("✅ Zoho Lead-Liste geschrieben")
            except Exception as e:
                errors.append(f"Zoho Leads Sheet: {e}")
                logger.error(f"Zoho Leads Sheet Fehler: {e}")
    else:
        logger.warning("Google Sheets nicht konfiguriert oder keine Daten vorhanden")
        if all_data:
            logger.info(f"Gesammelte Daten (nicht geschrieben): {all_data}")

    # Zusammenfassung
    logger.info("-" * 60)
    logger.info(f"Fetch abgeschlossen: {len(all_data)} KPIs gesammelt")
    if errors:
        logger.warning(f"Fehler: {len(errors)}")
        for err in errors:
            logger.warning(f"  - {err}")
    else:
        logger.info("Keine Fehler aufgetreten")
    logger.info("=" * 60)

    return all_data, errors


def run_backfill(days: int = 90):
    """Füllt historische GA4-Daten für fehlende Tage im Sheet nach."""
    logger.info("=" * 60)
    logger.info(f"GA4 Backfill gestartet: letzte {days} Tage")
    logger.info("=" * 60)

    if not config.GA_PROPERTY_ID or not config.GOOGLE_SERVICE_ACCOUNT_JSON:
        logger.error("GA4 nicht konfiguriert (GA_PROPERTY_ID oder GOOGLE_SERVICE_ACCOUNT_JSON fehlt)")
        return

    try:
        credentials = _get_google_credentials()
        ga_history = fetch_ga_historical(config.GA_PROPERTY_ID, credentials, days=days)
        if ga_history:
            backfill_ga_rows(ga_history)
            update_monthly_aggregation()
            logger.info("✅ Backfill abgeschlossen")
        else:
            logger.warning("Keine historischen GA4-Daten erhalten")
    except Exception as e:
        logger.error(f"Backfill Fehler: {e}")


def main():
    parser = argparse.ArgumentParser(description="Stromify KPI Cronjob")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Als dauerhafter Prozess mit Schedule ausführen (täglich um 22:00)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Intervall in Minuten für wiederkehrende Ausführung",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        metavar="DAYS",
        default=None,
        help="Historische GA4-Daten nachfüllen (z.B. --backfill 90)",
    )
    args = parser.parse_args()

    if args.backfill:
        run_backfill(days=args.backfill)
    elif args.schedule or args.interval:
        import schedule
        import time

        if args.interval:
            schedule.every(args.interval).minutes.do(run_fetch)
            logger.info(f"Scheduled: Alle {args.interval} Minuten")
        else:
            schedule.every().day.at("22:00").do(run_fetch)
            logger.info("Scheduled: Täglich um 22:00 Uhr")

        # Direkt einmal ausführen
        run_fetch()

        # Dann im Loop warten
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Einmalige Ausführung
        run_fetch()


if __name__ == "__main__":
    main()
