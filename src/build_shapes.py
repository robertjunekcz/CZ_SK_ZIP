"""Vygeneruje TopoJSON hranice krajů a okresů ČR a SR pro použití v Power BI (Shape Map).

Postup:
  1. stáhne hranice územních prvků z ČÚZK (SHP, S-JTSK) a z GKÚ Bratislava (GeoJSON, WGS84),
  2. přeprojektuje česká data do WGS84 a sjednotí názvy atributů s `seznampsc.csv`,
  3. sloučí obě země, zjednoduší geometrii a uloží jako TopoJSON,
  4. ověří, že se klíče shodují s `seznampsc.csv` — na obou stranách, bez zbytků,
  5. přidá varianty `*_unknown` s útvarem pro neznámou hodnotu (`build_unknown_shapes.py`).

    python src/build_shapes.py

Vyžaduje Node.js kvůli mapshaperu, který se stáhne přes `npx` (nebo se použije
lokálně nainstalovaný, pokud je v PATH).

Zdroje:
  ČÚZK  – https://services.cuzk.gov.cz/shp/ (CC-BY 4.0)
  GKÚ   – https://zbgisws.skgeodesy.sk/ (CC-BY 4.0)
"""

import json
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import requests

import build_unknown_shapes
from ruian_data_processing import OUTPUT_FILE, REGION_ISO, read_csv_auto


CZ_HRANICE = "https://services.cuzk.gov.cz/shp/stat/epsg-5514/1.zip"
CZ_EPSG = "EPSG:5514"  # S-JTSK / Krovak East North

SK_WFS = "https://zbgisws.skgeodesy.sk/zbgis_administrativne_hranice_wfs/service.svc/get"

OUTPUT_DIR = Path(__file__).parent.parent / "shapes"

# Míra zjednodušení geometrie (Visvalingam). 5 % zachovává tvar hranic i při
# zvětšení na celou obrazovku a drží soubory hluboko pod limitem Shape Map.
SIMPLIFY = "5%"

# Definice obou úrovní. `cz_fields` / `sk_fields` mapují zdrojové atributy na
# názvy sloupců, které používá seznampsc.csv — jinak by join v Power BI nesedl.
LEVELS = {
    "regions": {
        "cz_layer": "VUSC_P",
        "sk_layer": "kraj",
        "cz_fields": {
            "region": "NAZEV",
            "vusc_kod": "String(KOD)",
            "region_nuts3": "NUTS3_KOD",
        },
        "sk_fields": {
            "region": 'this.properties["Kraj"]',
            "vusc_kod": 'String(this.properties["Číslo_kraja"])',
            "region_nuts3": 'this.properties["Kód_kraja"]',
        },
        "key": "region_nuts3",
        "name_col": "region",
        "expected": {"CZSK": 22, "CZ": 14, "SK": 8},
    },
    "districts": {
        "cz_layer": "OKRESY_P",
        "sk_layer": "okres",
        "cz_fields": {
            "county": "NAZEV",
            "okres_kod": "String(KOD)",
            "okres_lau1": "LAU1_KOD",
            "vusc_kod": "String(VUSC_KOD)",
            "region_nuts3": "NUTS3_KOD",
        },
        "sk_fields": {
            "county": 'this.properties["Okres"]',
            "okres_kod": 'String(this.properties["Číslo_okresu"])',
            "okres_lau1": 'this.properties["Kód_okresu"]',
            "vusc_kod": 'String(this.properties["Číslo_kraja"])',
            "region_nuts3": 'this.properties["Kód_kraja"]',
        },
        "key": "okres_lau1",
        "name_col": "county",
        "expected": {"CZSK": 156, "CZ": 77, "SK": 79},
    },
}

# Každá úroveň se vydává ve třech variantách — obě země pohromadě a každá zvlášť.
VARIANTS = {"CZSK": ("CZ", "SK"), "CZ": ("CZ",), "SK": ("SK",)}


# ---------------------------------------------------------------------------
# Nástroje
# ---------------------------------------------------------------------------

def mapshaper_cmd() -> list[str]:
    """Vrátí příkaz pro spuštění mapshaperu — lokální instalaci, jinak přes npx."""
    local = shutil.which("mapshaper")
    if local:
        return [local]
    if not shutil.which("npx"):
        raise RuntimeError(
            "Nenalezen mapshaper ani npx. Nainstalujte Node.js, nebo mapshaper globálně "
            "(`npm install -g mapshaper`)."
        )
    return ["npx", "--yes", "mapshaper@0.7"]


def run_mapshaper(args: list[str]) -> None:
    """Spustí mapshaper a vypíše jeho chybový výstup, pokud selže."""
    result = subprocess.run(mapshaper_cmd() + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"mapshaper selhal:\n{result.stdout}\n{result.stderr}")


def each_expr(country_code: str, fields: dict[str, str]) -> str:
    """Sestaví výraz pro `mapshaper -each`, který přejmenuje atributy na naše názvy."""
    parts = [f'country_code="{country_code}"']
    parts += [f"{target}={source}" for target, source in fields.items()]
    # region_iso není ani v jednom zdroji, doplňujeme ho z našeho mapování podle NUTS3
    parts.append(f"region_iso=({json.dumps(REGION_ISO, ensure_ascii=False)})[region_nuts3]")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Stažení zdrojů
# ---------------------------------------------------------------------------

def download_cz_shapes(tmp: Path) -> Path:
    """Stáhne archiv hranic ČÚZK a rozbalí z něj jen vrstvy krajů a okresů."""
    print(f"  Stahuji hranice ČÚZK (~240 MB): {CZ_HRANICE}")
    zip_path = tmp / "cz_hranice.zip"
    response = requests.get(CZ_HRANICE, timeout=1800, stream=True)
    response.raise_for_status()
    with open(zip_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)

    wanted = {level["cz_layer"] for level in LEVELS.values()}
    extract_dir = tmp / "cz"
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if Path(n).stem in wanted]
        if not members:
            raise FileNotFoundError(f"V archivu nejsou vrstvy {wanted}. Obsah: {zf.namelist()[:10]}")
        zf.extractall(extract_dir, members=members)
    zip_path.unlink()

    return extract_dir


def download_sk_layer(layer: str, dest: Path) -> Path:
    """Stáhne vrstvu z WFS GKÚ jako GeoJSON rovnou ve WGS84."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"zbgis_administrativne_hranice_wfs:{layer}",
        "outputFormat": "GEOJSON",
        "srsName": "EPSG:4326",
    }
    print(f"  Stahuji vrstvu GKÚ '{layer}'...")
    response = requests.get(SK_WFS, params=params, timeout=1800, stream=True)
    response.raise_for_status()
    with open(dest, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)
    return dest


# ---------------------------------------------------------------------------
# Sestavení TopoJSON
# ---------------------------------------------------------------------------

def prepare_sources(name: str, level: dict, cz_dir: Path, tmp: Path) -> dict[str, Path]:
    """Připraví GeoJSON obou zemí ve WGS84 s atributy pojmenovanými jako sloupce v CSV."""
    fields = ["country_code", *level["cz_fields"], "region_iso"]

    cz_shp = next(cz_dir.rglob(f"{level['cz_layer']}.shp"))
    cz_prep = tmp / f"cz_{name}.json"
    print("  Přeprojektuji česká data z S-JTSK do WGS84...")
    run_mapshaper([
        "-i", str(cz_shp), "encoding=win1250",
        "-proj", f"from={CZ_EPSG}", "EPSG:4326",
        "-each", each_expr("CZ", level["cz_fields"]),
        "-filter-fields", ",".join(fields),
        "-o", str(cz_prep), "format=geojson",
    ])

    sk_raw = download_sk_layer(level["sk_layer"], tmp / f"sk_{name}_raw.json")
    sk_prep = tmp / f"sk_{name}.json"
    run_mapshaper([
        "-i", str(sk_raw),
        "-each", each_expr("SK", level["sk_fields"]),
        "-filter-fields", ",".join(fields),
        "-o", str(sk_prep), "format=geojson",
    ])

    return {"CZ": cz_prep, "SK": sk_prep}


def build_variant(name: str, variant: str, sources: list[Path]) -> Path:
    """Sloučí zadané země, zjednoduší geometrii a uloží jako TopoJSON."""
    out = OUTPUT_DIR / f"{variant}_{name}.topo.json"

    args = ["-i", *[str(p) for p in sources]]
    if len(sources) > 1:
        args += ["combine-files", "-merge-layers"]
    args += [
        "-simplify", SIMPLIFY, "keep-shapes",
        "-clean",
        # Vrstva se jmenuje podle úrovně, ne podle souboru — vizuály, které na ni
        # odkazují (např. Deneb), pak fungují se všemi třemi variantami stejně.
        "-rename-layers", name,
        "-o", str(out), "format=topojson", "force",
    ]
    run_mapshaper(args)

    print(f"  Uloženo: {out.name} ({out.stat().st_size / 1024:.0f} kB)")
    return out


# ---------------------------------------------------------------------------
# Ověření proti seznampsc.csv
# ---------------------------------------------------------------------------

def verify(path: Path, name: str, variant: str, level: dict, df: pd.DataFrame) -> bool:
    """Ověří, že se klíče v TopoJSONu přesně kryjí s klíči v seznampsc.csv."""
    topo = json.loads(path.read_text(encoding="utf-8"))
    geometries = topo["objects"][name]["geometries"]
    props = [g.get("properties", {}) for g in geometries]

    ok = True
    key = level["key"]
    expected = level["expected"][variant]

    if len(props) != expected:
        print(f"  [CHYBA] {path.name}: počet útvarů {len(props)} (očekáváno {expected})")
        ok = False

    missing_prop = [p for p in props if not p.get(key)]
    if missing_prop:
        print(f"  [CHYBA] {len(missing_prop)} útvarů bez klíče '{key}'")
        ok = False

    in_shape = {p.get(key) for p in props}
    in_csv = set(df[key].dropna())

    only_shape = sorted(in_shape - in_csv)
    only_csv = sorted(in_csv - in_shape)
    if only_shape:
        print(f"  [CHYBA] V mapě, ale ne v CSV ({len(only_shape)}): {only_shape[:5]}")
        ok = False
    if only_csv:
        print(f"  [CHYBA] V CSV, ale ne v mapě ({len(only_csv)}): {only_csv[:5]}")
        ok = False
    if not only_shape and not only_csv:
        print(f"  [OK] {path.name}: {len(in_shape)} útvarů, klíč '{key}' se kryje s CSV")

    # Názvy nejsou klíčem, ale nesoulad by mátl v tooltipech
    name_col = level["name_col"]
    csv_names = dict(zip(df[key], df[name_col]))
    mismatched = [
        (p[key], p.get(name_col), csv_names.get(p[key]))
        for p in props
        if p.get(key) in csv_names and p.get(name_col) != csv_names[p[key]]
    ]
    if mismatched:
        print(f"  [VAROVÁNÍ] Odlišný název u {len(mismatched)} útvarů: {mismatched[:3]}")

    return ok


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    if not OUTPUT_FILE.exists():
        raise FileNotFoundError(
            f"Chybí {OUTPUT_FILE}. Nejdřív spusťte `python src/ruian_data_processing.py`."
        )
    df = read_csv_auto(OUTPUT_FILE)

    print("=== Hranice krajů a okresů ČR + SR ===")
    mapshaper_cmd()  # ověření dostupnosti dřív, než se stáhne 240 MB

    ok = True
    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        cz_dir = download_cz_shapes(tmp)

        for name, level in LEVELS.items():
            print(f"\n--- {name} ---")
            sources = prepare_sources(name, level, cz_dir, tmp)

            for variant, countries in VARIANTS.items():
                print(f"  Sestavuji {variant} (zjednodušení {SIMPLIFY})...")
                out = build_variant(name, variant, [sources[c] for c in countries])
                subset = df[df["country_code"].isin(countries)]
                ok = verify(out, name, variant, level, subset) and ok

    if not ok:
        print("\n=== Výsledek: FAILED ✗ ===")
        raise SystemExit(1)

    # Varianty s prvkem pro neznámou hodnotu se odvozují z právě hotových map
    print()
    build_unknown_shapes.main()

    print("\n=== Výsledek: PASSED ✓ ===")


if __name__ == "__main__":
    main()
