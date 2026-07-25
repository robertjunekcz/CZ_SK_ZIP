"""Vygeneruje statický lookup slovenských obcí, okresů a krajů z WFS služby GKÚ Bratislava.

Slovenská adresní data (data.slovensko.sk) obsahují pouze názvy obcí, okresů a krajů,
nikoliv jejich kódy. Tento skript je stáhne z registru administratívnych hraníc GKÚ
a uloží jako `lookup_sk_obec.csv`, který pak používá `ruian_data_processing.py`.

Skript stačí spustit občas - kódy obcí a okresů se mění jen výjimečně.

    python src/build_sk_lookup.py

Zdroj: https://zbgisws.skgeodesy.sk/ (ZBGIS administratívne hranice, CC-BY 4.0)
"""

from pathlib import Path

import pandas as pd
import requests


SK_WFS = "https://zbgisws.skgeodesy.sk/zbgis_administrativne_hranice_wfs/service.svc/get"

LOOKUP_FILE = Path(__file__).parent / "lookup_sk_obec.csv"

# Očekávaný rozsah - kontrola, že se stáhlo kompletní pokrytí
EXPECTED_OBCE = 2900
EXPECTED_OKRESY = 79
EXPECTED_KRAJE = 8


def fetch_obce() -> pd.DataFrame:
    """Stáhne vrstvu obcí z WFS GKÚ ve formátu CSV (včetně geometrie, kterou zahodíme)."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": "zbgis_administrativne_hranice_wfs:obec",
        "outputFormat": "CSV",
    }
    print(f"  Stahuji vrstvu obcí z {SK_WFS} (~75 MB, chvíli to trvá)...")
    response = requests.get(SK_WFS, params=params, timeout=600, stream=True)
    response.raise_for_status()

    tmp = LOOKUP_FILE.parent / "_sk_obec_raw.csv"
    with open(tmp, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)

    try:
        # Sloupec Shape obsahuje geometrii v S-JTSK, pro lookup ho nepotřebujeme
        df = pd.read_csv(tmp, dtype=str, usecols=lambda c: c != "Shape")
    finally:
        tmp.unlink(missing_ok=True)

    return df


def main():
    print("=== Lookup slovenských obcí (GKÚ ZBGIS) ===")
    df = fetch_obce()
    print(f"  Staženo záznamů: {len(df):,}")

    lookup = (
        df[["NM4", "NM3", "IDN4", "IDN3", "IDN2", "LAU1_CODE", "NUTS3_CODE"]]
        .rename(
            columns={
                "NM4": "municipality",
                "NM3": "county",
                "IDN4": "obec_kod",
                "IDN3": "okres_kod",
                "IDN2": "vusc_kod",
                "LAU1_CODE": "okres_lau1",
                "NUTS3_CODE": "region_nuts3",
            }
        )
        .sort_values(["county", "municipality"])
        .reset_index(drop=True)
    )

    # Kontroly kompletnosti
    ok = True
    checks = [
        ("obcí", len(lookup), EXPECTED_OBCE),
        ("okresů", lookup["okres_kod"].nunique(), EXPECTED_OKRESY),
        ("krajů", lookup["vusc_kod"].nunique(), EXPECTED_KRAJE),
    ]
    for label, actual, expected in checks:
        if actual < expected:
            print(f"  [CHYBA] {label}: {actual} (očekáváno alespoň {expected})")
            ok = False
        else:
            print(f"  [OK] {label}: {actual}")

    # Klíč (obec, okres) musí být unikátní, jinak by join v hlavním skriptu množil řádky
    dupl = lookup.duplicated(subset=["municipality", "county"])
    if dupl.any():
        print(f"  [CHYBA] Duplicitní kombinace (obec, okres): {dupl.sum()}")
        ok = False
    else:
        print("  [OK] Klíč (obec, okres) je unikátní")

    if not ok:
        raise SystemExit("Lookup neprošel kontrolami, soubor nebyl uložen.")

    lookup.to_csv(LOOKUP_FILE, sep=";", index=False, encoding="utf-8-sig")
    print(f"\nUloženo: {LOOKUP_FILE} ({len(lookup)} řádků)")


if __name__ == "__main__":
    main()
