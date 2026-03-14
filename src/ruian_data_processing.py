import tempfile
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd
import requests


CZ_KRAJ = "https://services.cuzk.gov.cz/sestavy/cis/UI_VUSC.zip"
CZ_OKRES = "https://services.cuzk.gov.cz/sestavy/cis/UI_OKRES.zip"
CZ_OBEC = "https://services.cuzk.gov.cz/sestavy/cis/UI_OBEC.zip"
CZ_ADRESY = "https://vdp.cuzk.gov.cz/vymenny_format/csv/20260228_OB_ADR_csv.zip"

SK_ADRESY = "https://data.slovensko.sk/download?id=6eba6ef5-24a2-455e-9f7a-eb8a139b3af7"

OUTPUT_FILE = Path(__file__).parent.parent / "seznampsc.csv"          # finální výstup s dedupem
OUTPUT_FILE_ALL = Path(__file__).parent.parent / "seznampsc_all.csv"  # soubor bez dedup 


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def download_and_extract(url: str, tmp_dir: str) -> list[Path]:
    """Stáhne ZIP z URL, uloží do tmp_dir a extrahuje. Vrátí seznam extrahovaných souborů."""
    print(f"  Stahuji: {url}")
    response = requests.get(url, timeout=600, stream=True)
    response.raise_for_status()

    Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    zip_path = Path(tmp_dir) / "download.zip"
    with open(zip_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)

    extract_dir = Path(tmp_dir) / "extracted"
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
        names = zf.namelist()

    return [extract_dir / name for name in names]


def read_csv_auto(path: Path, **kwargs) -> pd.DataFrame:
    """Načte CSV se středníkovým oddělovačem a automatickou detekcí kódování."""
    for enc in ("utf-8-sig", "utf-8", "cp1250"):
        try:
            return pd.read_csv(path, sep=";", dtype=str, encoding=enc, **kwargs)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Nepodařilo se načíst soubor {path} v žádném kódování (utf-8, cp1250).")


def _norm(s: str) -> str:
    """Odstraní diakritiku a převede na uppercase."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    ).upper()


def find_col(columns: list[str], *keywords: str) -> str | None:
    """Najde první sloupec, jehož název (bez diakritiky, uppercase) obsahuje všechna klíčová slova."""
    for col in columns:
        col_norm = _norm(col)
        if all(_norm(kw) in col_norm for kw in keywords):
            return col
    return None


# ---------------------------------------------------------------------------
# CZ pipeline
# ---------------------------------------------------------------------------

def load_cz_codebooks(tmp_dir: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Stáhne a načte číselníky krajů, okresů a obcí z ČÚZK."""

    def _load_codebook(url: str, label: str) -> pd.DataFrame:
        files = download_and_extract(url, tmp_dir + f"/{label}")
        csv_files = [f for f in files if f.suffix.lower() == ".csv"]
        if not csv_files:
            raise FileNotFoundError(f"Žádný CSV v archivu {label}. Nalezené soubory: {[f.name for f in files]}")
        df = read_csv_auto(csv_files[0])
        print(f"  [{label}] sloupce: {list(df.columns)}")
        return df

    print("Načítám číselník krajů (UI_VUSC)...")
    df_kraj = _load_codebook(CZ_KRAJ, "kraj")

    print("Načítám číselník okresů (UI_OKRES)...")
    df_okres = _load_codebook(CZ_OKRES, "okres")

    print("Načítám číselník obcí (UI_OBEC)...")
    df_obec = _load_codebook(CZ_OBEC, "obec")

    return df_kraj, df_okres, df_obec


def process_cz(tmp_dir: str) -> pd.DataFrame:
    """CZ pipeline: stáhne a zpracuje česká data z ČÚZK."""
    print("\n=== CZ pipeline ===")

    # --- Číselníky ---
    df_kraj, df_okres, df_obec = load_cz_codebooks(tmp_dir)

    # Určení klíčových sloupců v číselníku krajů
    kraj_kod = find_col(df_kraj.columns.tolist(), "KOD") or df_kraj.columns[0]
    kraj_nazev = find_col(df_kraj.columns.tolist(), "NAZEV") or df_kraj.columns[1]

    # Určení klíčových sloupců v číselníku okresů
    okres_cols = df_okres.columns.tolist()
    okres_kod = find_col(okres_cols, "KOD") or okres_cols[0]
    okres_nazev = find_col(okres_cols, "NAZEV") or okres_cols[1]
    okres_vusc = find_col(okres_cols, "VUSC") or find_col(okres_cols, "KRAJ")
    if not okres_vusc:
        raise KeyError(f"Nenalezen sloupec odkazující na kraj v číselníku okresů. Sloupce: {okres_cols}")

    # Určení klíčových sloupců v číselníku obcí
    obec_cols = df_obec.columns.tolist()
    obec_kod = find_col(obec_cols, "KOD") or obec_cols[0]
    obec_nazev = find_col(obec_cols, "NAZEV") or obec_cols[1]
    obec_okres = find_col(obec_cols, "OKRES") or find_col(obec_cols, "OKRE")
    if not obec_okres:
        raise KeyError(f"Nenalezen sloupec odkazující na okres v číselníku obcí. Sloupce: {obec_cols}")

    # Sestavit lookup tabulky: obec_kod → (municipality, okres_nazev, kraj_nazev)
    df_k = df_kraj[[kraj_kod, kraj_nazev]].rename(columns={kraj_kod: "vusc_kod", kraj_nazev: "region"})
    df_o = df_okres[[okres_kod, okres_nazev, okres_vusc]].rename(
        columns={okres_kod: "okres_kod", okres_nazev: "county", okres_vusc: "vusc_kod"}
    )
    df_b = df_obec[[obec_kod, obec_nazev, obec_okres]].rename(
        columns={obec_kod: "obec_kod", obec_nazev: "municipality", obec_okres: "okres_kod"}
    )

    # Normalize kódy (strip whitespace)
    for df, cols in [(df_k, ["vusc_kod"]), (df_o, ["okres_kod", "vusc_kod"]), (df_b, ["obec_kod", "okres_kod"])]:
        for c in cols:
            df[c] = df[c].astype(str).str.strip()

    # --- Adresní místa ---
    print("Stahuji adresní místa (může trvat déle)...")
    files = download_and_extract(CZ_ADRESY, tmp_dir + "/adresy")
    csv_files = sorted([f for f in files if f.suffix.lower() == ".csv"])
    if not csv_files:
        raise FileNotFoundError(f"Žádný CSV v archivu adres. Nalezené soubory: {[f.name for f in files]}")

    print(f"  Nalezeno {len(csv_files)} CSV souborů v archivu")

    # Detekce kódování a sloupců z prvního souboru
    enc_used = "utf-8-sig"
    for enc in ("utf-8-sig", "utf-8", "cp1250"):
        try:
            df_first = pd.read_csv(csv_files[0], sep=";", dtype=str, encoding=enc, nrows=2)
            enc_used = enc
            break
        except UnicodeDecodeError:
            continue

    adr_cols = df_first.columns.tolist()
    print(f"  Sloupce adres: {adr_cols}")

    psc_col = next(
        (c for c in adr_cols if c in {"PSČ", "PSC"} or _norm(c) == "PSC"), None
    )
    obec_adr_col = next(
        (c for c in adr_cols if c in {"Kód obce", "KOD_OBCE", "OBEC_KOD"} or _norm(c) in {"KOD OBCE", "KOD_OBCE", "OBEC_KOD"}),
        None,
    ) or find_col(adr_cols, "KOD", "OBEC")

    if not psc_col:
        raise KeyError(f"Nenalezen sloupec PSC v adresních datech. Sloupce: {adr_cols}")
    if not obec_adr_col:
        raise KeyError(f"Nenalezen sloupec kódu obce v adresních datech. Sloupce: {adr_cols}")
    print(f"  Používám: psc='{psc_col}', obec='{obec_adr_col}'")

    # Sloučení 6258 CSV souborů do jednoho (raw bytes, přeskočení hlaviček od 2. souboru)
    # Výrazně rychlejší než volat pandas 6258×
    print(f"  Slučuji {len(csv_files)} souborů do jednoho...")
    combined_path = Path(tmp_dir) / "adresy" / "combined.csv"
    with open(combined_path, "wb") as out:
        for i, f in enumerate(csv_files):
            with open(f, "rb") as inp:
                if i == 0:
                    out.write(inp.read())
                else:
                    inp.readline()  # přeskočit hlavičku
                    out.write(inp.read())

    print("  Načítám sloučený soubor...")
    df_adresy = pd.read_csv(
        combined_path, sep=";", dtype=str, encoding=enc_used,
        usecols=[psc_col, obec_adr_col],
    )
    print(f"  Celkem adresních míst: {len(df_adresy):,}")

    # Normalizace
    df_adresy[psc_col] = df_adresy[psc_col].astype(str).str.strip().str.replace(" ", "").str.zfill(5)
    df_adresy[obec_adr_col] = df_adresy[obec_adr_col].astype(str).str.strip()

    # Groupby PSČ + kód obce
    df_counts = (
        df_adresy.groupby([psc_col, obec_adr_col])
        .size()
        .reset_index(name="address_count")
    )
    df_counts.columns = ["zip", "obec_kod", "address_count"]

    # Join: obec → okres → kraj
    df_result = df_counts.merge(df_b, on="obec_kod", how="left")
    df_result = df_result.merge(df_o, on="okres_kod", how="left")
    df_result = df_result.merge(df_k, on="vusc_kod", how="left")

    # Diagnostika chybějících hodnot
    missing = df_result["county"].isna().sum()
    if missing > 0:
        print(f"  [VAROVÁNÍ] {missing} řádků bez přiřazeného okresu — zkontrolujte kódy obcí")

    df_result["zip"] = df_result["zip"].str.strip().str.zfill(5)
    df_result["country_code"] = "CZ"
    df_result["country_zip"] = "CZ-" + df_result["zip"]

    return df_result[["country_zip", "country_code", "zip", "municipality", "county", "region", "address_count"]]


# ---------------------------------------------------------------------------
# SK pipeline
# ---------------------------------------------------------------------------

def download_csv(url: str, dest: Path) -> None:
    """Stáhne soubor (ne ZIP) z URL a uloží do dest."""
    print(f"  Stahuji: {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=600, stream=True)
    response.raise_for_status()
    with open(dest, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)


def process_sk(tmp_dir: str) -> pd.DataFrame:
    """SK pipeline: stáhne a zpracuje slovenská adresní data z data.slovensko.sk."""
    print("\n=== SK pipeline ===")

    sk_path = Path(tmp_dir) / "sk" / "sk_adresy.csv"
    download_csv(SK_ADRESY, sk_path)

    df_sk = read_csv_auto(sk_path)
    print(f"  Sloupce SK: {list(df_sk.columns)}")
    print(f"  Celkem SK adresních míst: {len(df_sk):,}")

    # Struktura: IDENTIFIKATOR;KRAJ;OKRES;OBEC;CAST_OBCE;ULICA;SUPISNE_CISLO;ORIENTACNE_CISLO_CELE;PSC;ADRBOD_X;ADRBOD_Y
    df_sk["PSC"] = df_sk["PSC"].astype(str).str.strip().str.replace(" ", "").str.zfill(5)
    df_sk["KRAJ"] = df_sk["KRAJ"].astype(str).str.strip()
    df_sk["OKRES"] = df_sk["OKRES"].astype(str).str.strip()
    df_sk["OBEC"] = df_sk["OBEC"].astype(str).str.strip()

    df_counts = (
        df_sk.groupby(["PSC", "OBEC"])
        .agg(address_count=("IDENTIFIKATOR", "count"), OKRES=("OKRES", "first"), KRAJ=("KRAJ", "first"))
        .reset_index()
    )

    df_counts["zip"] = df_counts["PSC"].str.strip().str.zfill(5)
    df_counts["municipality"] = df_counts["OBEC"]
    df_counts["county"] = df_counts["OKRES"]
    df_counts["region"] = df_counts["KRAJ"] + " kraj"
    df_counts["country_code"] = "SK"
    df_counts["country_zip"] = "SK-" + df_counts["zip"]

    return df_counts[["country_zip", "country_code", "zip", "municipality", "county", "region", "address_count"]]


# ---------------------------------------------------------------------------
# Validace výstupu
# ---------------------------------------------------------------------------

def validate_output(df: pd.DataFrame, label: str = "", dedup: bool = True) -> bool:
    """Spustí sadu validačních kontrol nad výstupním DataFrame. Vrátí True pokud vše prošlo."""
    print(f"\n=== Validace výstupu{' (' + label + ')' if label else ''} ===")
    ok = True

    # 1. Struktura sloupců
    if dedup:
        expected_cols = ["country_zip", "country_code", "country", "zip", "municipality", "county", "region"]
    else:
        expected_cols = ["country_zip", "country_code", "country", "zip", "municipality", "county", "region", "address_count"]
    if list(df.columns) != expected_cols:
        print(f"  [CHYBA] Neočekávané sloupce: {list(df.columns)}")
        ok = False
    else:
        print(f"  [OK] Struktura: {expected_cols}")

    # 2. Formát country_zip
    bad = df[~df["country_zip"].str.match(r"^(CZ|SK)-\d{5}$", na=False)]
    if len(bad) > 0:
        print(f"  [CHYBA] Neplatný formát country_zip ({len(bad)} řádků): {bad['country_zip'].head(3).tolist()}")
        ok = False
    else:
        print(r"  [OK] Formát country_zip: ^(CZ|SK)-\d{5}$")

    # 3. Formát zip — 5 číslic, žádné mezery
    bad_zip = df[~df["zip"].str.match(r"^\d{5}$", na=False)]
    if len(bad_zip) > 0:
        print(f"  [CHYBA] Neplatný formát zip ({len(bad_zip)} řádků): {bad_zip['zip'].head(3).tolist()}")
        ok = False
    else:
        print("  [OK] Formát zip: 5 číslic")

    # 4. country_code pouze CZ nebo SK
    invalid_cc = df[~df["country_code"].isin(["CZ", "SK"])]
    if len(invalid_cc) > 0:
        print(f"  [CHYBA] Neplatný country_code: {invalid_cc['country_code'].unique().tolist()}")
        ok = False
    else:
        print("  [OK] country_code: pouze CZ/SK")

    # 5. Konzistence country_zip = country_code + "-" + zip
    expected_cz = df["country_code"] + "-" + df["zip"]
    bad_cons = df[df["country_zip"] != expected_cz]
    if len(bad_cons) > 0:
        print(f"  [CHYBA] Nekonzistentní country_zip ({len(bad_cons)} řádků)")
        ok = False
    else:
        print("  [OK] Konzistence country_zip")

    # 6. address_count — celé číslo > 0, žádné NaN (jen pro _all soubor)
    if not dedup:
        if df["address_count"].isna().any():
            print("  [CHYBA] NaN v address_count")
            ok = False
        elif (df["address_count"].astype(int) <= 0).any():
            print("  [CHYBA] address_count obsahuje hodnoty <= 0")
            ok = False
        else:
            print(f"  [OK] address_count: min={df['address_count'].min()}, max={df['address_count'].max()}")

    # 7. Počty záznamů
    cz_count = int((df["country_code"] == "CZ").sum())
    sk_count = int((df["country_code"] == "SK").sum())
    print(f"  Počty: CZ={cz_count}, SK={sk_count}, celkem={len(df)}")
    if cz_count < 2500:
        print(f"  [VAROVÁNÍ] CZ řádků ({cz_count}) je méně než očekávaných 2500")
    if sk_count < 1200:
        print(f"  [VAROVÁNÍ] SK řádků ({sk_count}) je méně než očekávaných 1200")

    # 8. Duplicity
    if dedup:
        # V deduplikovaném souboru musí být každé country_zip unikátní
        dupl = df.duplicated(subset=["country_zip"])
        if dupl.any():
            print(f"  [CHYBA] Duplicitní country_zip: {dupl.sum()} řádků")
            ok = False
        else:
            print("  [OK] Žádné duplicity country_zip")
    else:
        # V all souboru musí být unikátní kombinace (country_code, zip, municipality)
        dupl = df.duplicated(subset=["country_code", "zip", "municipality"])
        if dupl.any():
            print(f"  [CHYBA] Duplicitní kombinace (country_code, zip, municipality): {dupl.sum()} řádků")
            ok = False
        else:
            print("  [OK] Žádné duplicity (country_code, zip, municipality)")

    # 9. Prázdné hodnoty v municipality, county a region
    for col in ["municipality", "county", "region"]:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            print(f"  [CHYBA] {null_count} prázdných hodnot v '{col}'")
            ok = False
    if all(df[c].notna().all() for c in ["municipality", "county", "region"]):
        print("  [OK] Žádné prázdné hodnoty v municipality/county/region")

    # 10. Seřazení podle country_zip
    if not df["country_zip"].is_monotonic_increasing:
        print("  [CHYBA] Výstup není seřazen podle country_zip")
        ok = False
    else:
        print("  [OK] Seřazení podle country_zip")

    print(f"\n=== Výsledek validace: {'PASSED ✓' if ok else 'FAILED ✗'} ===")
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COUNTRY_NAMES = {"CZ": "Česko", "SK": "Slovensko"}


def main():
    with tempfile.TemporaryDirectory() as tmp_dir:
        df_cz = process_cz(tmp_dir)
        df_sk = process_sk(tmp_dir)

    df_cz["country"] = COUNTRY_NAMES["CZ"]
    df_sk["country"] = COUNTRY_NAMES["SK"]

    # Sloučit a seřadit
    df_all = pd.concat([df_cz, df_sk], ignore_index=True)
    df_all = df_all.sort_values("country_zip").reset_index(drop=True)
    df_all = df_all[["country_zip", "country_code", "country", "zip", "municipality", "county", "region", "address_count"]]

    # Deduplikace: pro každé country_zip ponechat řádek s nejvyšším address_count
    df_dedup = (
        df_all.sort_values("address_count", ascending=False)
        .drop_duplicates(subset=["country_zip"], keep="first")
        .sort_values("country_zip")
        .reset_index(drop=True)
        .drop(columns=["address_count"])
    )

    # Uložit oba soubory
    df_all.to_csv(OUTPUT_FILE_ALL, sep=";", index=False, encoding="utf-8")
    print(f"\nUloženo: {OUTPUT_FILE_ALL} ({len(df_all)} řádků)")

    df_dedup.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    print(f"Uloženo: {OUTPUT_FILE} ({len(df_dedup)} řádků)")

    validate_output(df_all, label="all", dedup=False)
    validate_output(df_dedup, label="dedup", dedup=True)


if __name__ == "__main__":
    main()
