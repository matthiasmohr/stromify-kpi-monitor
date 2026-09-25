"""
Schnelltest: Zoho CRM Verträge (Custom-Modul Vertr_ge) – Vertrags-KPIs verifizieren.
Nutzt exakt die Logik aus cronjob/fetch_zoho.py (calc_contract_kpis) und listet
zusätzlich jeden Vertrag mit seiner Einstufung, damit die Zahlen nachvollziehbar sind.

Verwendung:
    python test_zoho_contracts.py
"""
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

import config
from cronjob.fetch_zoho import _refresh_access_token, _get_records, _parse_zoho_date, calc_contract_kpis


def main():
    print("=" * 70)
    print("🔍 Zoho Verträge – KPI-Verifikation")
    print("=" * 70)

    if not all([config.ZOHO_CLIENT_ID, config.ZOHO_CLIENT_SECRET, config.ZOHO_REFRESH_TOKEN]):
        print("❌ Zoho Credentials nicht in .env konfiguriert")
        sys.exit(1)

    token = _refresh_access_token(
        config.ZOHO_CLIENT_ID, config.ZOHO_CLIENT_SECRET, config.ZOHO_REFRESH_TOKEN, config.ZOHO_ACCOUNTS_URL
    )
    print("✅ Auth OK\n")

    contracts = _get_records(
        config.ZOHO_API_DOMAIN, token, config.ZOHO_CONTRACTS_MODULE,
        fields="Name,Vertragsart,JVP,JVP_Gas,CLV_Vertrag,Lieferende,Kunde",
    )
    today = date.today()
    print(f"📑 {len(contracts)} Verträge geladen (Stichtag {today})\n")

    header = f"{'Nr.':10} {'Typ':28} {'Kunde':38} {'Lieferende':11} {'aktiv':5} {'JVP+Gas kWh':>12} {'CLV €':>10}"
    print(header)
    print("-" * len(header))
    for c in sorted(contracts, key=lambda x: x.get("Name") or ""):
        typ = c.get("Vertragsart") or "-"
        kunde = c.get("Kunde") or {}
        kunde_name = kunde.get("name", "") if isinstance(kunde, dict) else str(kunde)
        lieferende = _parse_zoho_date(c.get("Lieferende"))
        is_energy = typ == config.ZOHO_CONTRACT_TYPE_ENERGY
        aktiv = "✓" if is_energy and (lieferende is None or today < lieferende) else ("✗" if is_energy else "–")
        kwh = float(c.get("JVP") or 0) + float(c.get("JVP_Gas") or 0)
        clv = float(c.get("CLV_Vertrag") or 0)
        print(f"{c.get('Name',''):10} {typ[:28]:28} {kunde_name[:38]:38} {str(lieferende or ''):11} {aktiv:^5} {kwh:>12,.0f} {clv:>10,.2f}")

    kpis = calc_contract_kpis(contracts, today)
    print("\n" + "=" * 70)
    print("📋 Ergebnis (wie fetch_zoho_contracts liefert):")
    for k, v in kpis.items():
        print(f"   {k:30} {v}")
    print("=" * 70)


if __name__ == "__main__":
    main()
