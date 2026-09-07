# Seznam českých a slovenských PSČ včetně obcí, okresů a krajů

Soubory v tomto projektu obsahují zpracovaná data o PSČ, obcích, okresech a krajích pro Českou republiku a Slovensko. 

## Jednotlivé soubory
- `seznampsc.csv`: Deduplikovaný seznam PSČ s přiřazenými obcemi, okresy a kraji pro ČR a SR; je-li k PSČ přiřazeno více obcí, je jako unikátní uváděna ta s největším počtem adresních míst.
- `seznampsc_all.csv`: Kompletní seznam PSČ s přiřazenými obcemi, okresy a kraji pro ČR a SR; obsahuje všechny kombinace PSČ a obcí, **včetně duplicit.** Není tak obvykle vhodný pro běžné použití.
- `src/lookup_sk_obec.csv`: Pomocný číselník kódů slovenských obcí, okresů a krajů. Slovenská adresní data obsahují pouze názvy, kódy se doplňují odsud.
- `shapes/*.topo.json`: Hranice krajů a okresů ve formátu TopoJSON (WGS84) — obě země pohromadě i každá zvlášť. Určeno pro vlastní mapy v Power BI, ale použitelné i jinde.
- `shapes/*_unknown.topo.json`: Totéž plus jeden útvar navíc — čtverec vedle mapy, do kterého spadnou řádky s neznámým krajem nebo okresem (`-1`).

## Struktura sloupců

| sloupec | popis |
|---|---|
| `country_zip` | unikátní klíč `CZ-10000` / `SK-01001` |
| `country_code` | `CZ` nebo `SK` |
| `country` | Česko / Slovensko |
| `zip` | PSČ, 5 číslic bez mezery — **u SK včetně vodicí nuly** |
| `municipality` | název obce |
| `county` | název okresu |
| `region` | název kraje |
| `address_count` | počet adresních míst (jen v `seznampsc_all.csv`) |
| `obec_kod` | kód obce (RÚIAN pro ČR, GKÚ pro SR) |
| `okres_kod` | kód okresu (RÚIAN pro ČR, GKÚ pro SR) |
| `okres_lau1` | kód okresu LAU1 — `CZ0201`, `SK0429`; **unikátní napříč oběma zeměmi** |
| `vusc_kod` | kód kraje (RÚIAN pro ČR, GKÚ pro SR) |
| `region_nuts3` | kód kraje NUTS3 — `CZ010`, `SK042` |
| `region_iso` | kód kraje ISO 3166-2 — `CZ-10`, `SK-KI` |

Soubory jsou v UTF-8 s BOM, oddělovač je středník. Sloupec `zip` je nutné načítat jako **text**, jinak přijdete o vodicí nuly u slovenských PSČ.

Pro spojení s vlastními daty nebo mapovými podklady používejte kódy, ne názvy — nemění se při přejmenování obce ani nezávisí na diakritice.

**Pozor na rozsah platnosti kódů:** `okres_lau1`, `region_nuts3` a `region_iso` jsou unikátní přes obě země a lze je použít jako klíč přímo. Národní kódy `obec_kod`, `okres_kod` a `vusc_kod` jsou unikátní **jen v rámci jedné země** — číselné řady RÚIAN a GKÚ se překrývají (u `obec_kod` konkrétně ve 4 případech), proto je vždy kombinujte s `country_code`.

## Mapové podklady

Ve složce `shapes/` jsou hranice krajů a okresů ve formátu **TopoJSON, souřadnicový systém WGS84 (EPSG:4326)**. Každá úroveň je ve třech variantách — obě země pohromadě (`CZSK_`) a každá zvlášť (`CZ_`, `SK_`).

| soubor | útvarů | velikost | klíč pro spojení |
|---|---|---|---|
| `shapes/CZSK_regions.topo.json` | 22 | 164 kB | `region_nuts3` (`CZ010`, `SK041`) |
| `shapes/CZ_regions.topo.json` | 14 | 120 kB | `region_nuts3` |
| `shapes/SK_regions.topo.json` | 8 | 43 kB | `region_nuts3` |
| `shapes/CZSK_districts.topo.json` | 156 | 351 kB | `okres_lau1` (`CZ0100`, `SK0319`) |
| `shapes/CZ_districts.topo.json` | 77 | 234 kB | `okres_lau1` |
| `shapes/SK_districts.topo.json` | 79 | 116 kB | `okres_lau1` |

Ke každému souboru je navíc varianta `_unknown` (např. `shapes/CZSK_regions_unknown.topo.json`) s prvkem pro neznámou hodnotu — viz [níže](#neznámé-hodnoty--1).

Každý útvar nese vlastnosti pojmenované stejně jako sloupce v CSV — u krajů `country_code`, `region`, `vusc_kod`, `region_nuts3`, `region_iso`, u okresů navíc `county`, `okres_kod` a `okres_lau1`. Spojení s daty tak nevyžaduje žádné přejmenovávání.

Vrstva uvnitř souboru se jmenuje podle úrovně (`regions`, resp. `districts`) bez ohledu na variantu, takže vizuál, který se na ni odkazuje, funguje se všemi soubory stejně.

### Použití v Power BI

1. V `Soubor → Možnosti → Funkce ve verzi Preview` zapněte **Shape Map** (vizuál je stále v preview).
2. Vložte vizuál Shape Map, v `Formát → Tvar mapy → Přidat mapu` nahrajte příslušný `.topo.json`.
3. Do pole `Location` dejte `region_nuts3` (kraje) nebo `okres_lau1` (okresy) — **ne názvy**, ty se pro párování nepoužívají.

Pokud report pokrývá jen jednu zemi, použijte variantu `CZ_` nebo `SK_` — mapa se sama přizpůsobí rozsahu dané země a nezůstane v ní poloprázdné místo po té druhé.

Pokud narazíte na limity Shape Map (neumí drill-through ani plné formátování), stejné soubory fungují i ve vizuálu **Deneb** (Vega-Lite umí TopoJSON nativně) nebo v Icon Map Pro.

Geometrie je zjednodušená na 5 % původního počtu bodů. Tvar hranic to zachovává i při zvětšení na celou obrazovku, ale data nejsou určena pro měření nebo katastrální účely — k tomu si stáhněte originály od ČÚZK a GKÚ.

*Poznámka: Hranice ČR a SR pocházejí ze dvou nezávislých registrů. Podél společné státní hranice proto mohou vznikat nepatrné odchylky v řádu metrů, které se ve vizualizaci neprojeví.*

### Neznámé hodnoty (`-1`)

Shape Map obarví jen ty útvary, které v mapě existují. Řádky, u kterých kraj nebo okres neznáte, se proto z vizuálu ztratí úplně — nikde nevidíte, kolik jich je. Varianty `_unknown` řeší tohle: obsahují o jeden útvar navíc, čtverec umístěný ve volném rohu vedle mapy, a **všechny jeho kódy mají hodnotu `-1`**.

| soubor | útvarů | umístění čtverce |
|---|---|---|
| `shapes/CZSK_regions_unknown.topo.json` | 22 + 1 | vpravo nahoře, nad východním Slovenskem |
| `shapes/CZ_regions_unknown.topo.json` | 14 + 1 | vpravo nahoře, nad Moravskoslezským krajem |
| `shapes/SK_regions_unknown.topo.json` | 8 + 1 | vpravo dole, pod východním Slovenskem |
| `shapes/CZSK_districts_unknown.topo.json` | 156 + 1 | vpravo nahoře |
| `shapes/CZ_districts_unknown.topo.json` | 77 + 1 | vpravo nahoře |
| `shapes/SK_districts_unknown.topo.json` | 79 + 1 | vpravo dole |

Čtverec leží uvnitř původního výřezu, ve volném místě — mapa se kvůli němu nezmenší a zůstane stejně velká jako v základní variantě. Vysoký je zhruba pětinu výšky mapy.

Použití je stejné jako u základních souborů, jen v datech nahradíte prázdnou hodnotu klíče za `-1`:

```
Kraj = COALESCE('Data'[region_nuts3], "-1")
```

Hodnotu `-1` nesou všechny kódové sloupce (`region_nuts3`, `okres_lau1`, `vusc_kod`, `okres_kod`, `region_iso` i `country_code`), takže funguje bez ohledu na to, přes který z nich spojujete. Textové sloupce `region` a `county` mají `Neznámé`, aby dávaly smysl v tooltipu.

Čtverec je čtvercový i na obrazovce — souřadnice se počítají v Mercatorově projekci, kterou se TopoJSON vykresluje, takže je ve stupních zeměpisné délky širší, než vysoký.

### Aktualizace mapových podkladů

Soubory vygeneruje skript `src/build_shapes.py`. Kromě Pythonu vyžaduje **Node.js** — mapshaper, který se stará o reprojekci, zjednodušení a export do TopoJSONu, se stáhne automaticky přes `npx`.

```
python src/ruian_data_processing.py   # nejdřív CSV, proti kterému se mapy ověřují
python src/build_shapes.py
```

Skript kontroluje, že se klíče v mapách přesně kryjí s `seznampsc.csv` na obou stranách, a při neshodě skončí chybou. Na závěr sám spustí `src/build_unknown_shapes.py`, který dopočítá varianty `_unknown`.

Chcete-li jen změnit velikost nebo umístění čtverce pro neznámou hodnotu, upravte konstanty `SQUARE_SIZE`, `MARGIN` a `CORNERS` v `src/build_unknown_shapes.py` a spusťte ho samostatně:

```
python src/build_unknown_shapes.py
```

Pracuje nad hotovými soubory ve `shapes/`, takže nic nestahuje a nepotřebuje Node.js. Umístění čtverce si ověřuje proti geometrii — pokud by zasahoval do některého kraje či okresu, skončí chybou místo aby vyrobil rozbitou mapu.

## Aktualizace dat
Chcete-li aktualizovat data, můžete si stáhnout celý repozitář, nainstalovat požadované závislosti z `src/requirements.txt` a spustit skript `src/ruian_data_processing.py`. Pokud v něm aktualizujete URL pro zdroje dat, skript sám vygeneruje příslušné CSV soubory s aktuálními daty.

Číselník `src/lookup_sk_obec.csv` je součástí repozitáře a běžně ho není potřeba obnovovat. Pokud na Slovensku dojde ke změně obcí nebo okresů, vygenerujte ho znovu skriptem `src/build_sk_lookup.py`.

Pokud si na to sami netroufáte, vytvořte issue s požadavkem na aktualizaci dat a já se o to postarám.

### Zdroje dat pro Českou republiku

Zdrojem jsou veřejně dostupná data z Českého úřadu zeměměřického a katastrálního (ČÚZK) - konkrétně data o krajích, okresech, obcích a adresách.

Aktuální odkaz pro seznam adresních míst z celé ČR je na https://nahlizenidokn.cuzk.gov.cz/StahniAdresniMistaRUIAN.aspx

Jednotlivé číselníky krajů, okresů a obcí jsou https://services.cuzk.gov.cz/sestavy/cis/

Hranice územních prvků pro mapové podklady se stahují z https://services.cuzk.gov.cz/shp/stat/ (SHP v S-JTSK / Krovak, licence CC-BY 4.0).

### Zdroje dat pro Slovensko

Zdrojem jsou veřejně dostupná data z portálu data.slovensko.sk - konkrétně data o adresách `Adresy podľa krajou (všetky kraje)` z https://data.slovensko.sk/datasety/b27f57f1-7e76-45e0-8968-631f9176b2e9

Kódy obcí, okresů a krajů (`obec_kod`, `okres_kod`, `okres_lau1`, `vusc_kod`, `region_nuts3`) v adresních datech nejsou. Doplňují se z registru administratívnych hraníc ZBGIS Geodetického a kartografického ústavu Bratislava přes WFS službu https://zbgisws.skgeodesy.sk/zbgis_administrativne_hranice_wfs/service.svc/get (licence CC-BY 4.0).


*Poznámka: Url pro stažení dat se může měnit, proto je potřeba zkontrolovat aktuální odkazy na výše uvedených stránkách. Uvedená data jsou platká k březnu 2026.*

---

# ENGLISH: List of Czech and Slovak postal codes (PSČ / ZIP) including municipalities, districts, and regions

The files in this project contain processed data about postal codes (PSČ), municipalities, districts, and regions for the Czech Republic and Slovakia.

## Individual files
- `seznampsc.csv`: A deduplicated list of postal codes with assigned municipalities, districts, and regions for the Czech Republic and Slovakia; if a postal code is assigned to multiple municipalities, the one with the largest number of address points is listed as unique.
- `seznampsc_all.csv`: A complete list of postal codes with assigned municipalities, districts, and regions for the Czech Republic and Slovakia; it contains all combinations of postal codes and municipalities, **including duplicates.** It is not usually suitable for general use.
- `src/lookup_sk_obec.csv`: A helper codebook of Slovak municipality, district, and region codes. The Slovak address data contains names only, so the codes are joined in from here.
- `shapes/*.topo.json`: Boundaries of regions and districts as TopoJSON (WGS84) — both countries together and each one separately. Intended for custom maps in Power BI, but usable anywhere.

## Column structure

| column | description |
|---|---|
| `country_zip` | unique key, `CZ-10000` / `SK-01001` |
| `country_code` | `CZ` or `SK` |
| `country` | country name (Czech spelling) |
| `zip` | postal code, 5 digits without a space — **including the leading zero for SK** |
| `municipality` | municipality name |
| `county` | district (okres) name |
| `region` | region (kraj) name |
| `address_count` | number of address points (only in `seznampsc_all.csv`) |
| `obec_kod` | municipality code (RÚIAN for CZ, GKÚ for SK) |
| `okres_kod` | district code (RÚIAN for CZ, GKÚ for SK) |
| `okres_lau1` | district LAU1 code — `CZ0201`, `SK0429`; **unique across both countries** |
| `vusc_kod` | region code (RÚIAN for CZ, GKÚ for SK) |
| `region_nuts3` | region NUTS3 code — `CZ010`, `SK042` |
| `region_iso` | region ISO 3166-2 code — `CZ-10`, `SK-KI` |

The files are UTF-8 with BOM and semicolon-separated. The `zip` column must be read as **text**, otherwise the leading zeros of Slovak postal codes are lost.

When joining to your own data or to map shapes, use the codes rather than the names — they survive renames and are independent of diacritics.

**Mind the scope of each code:** `okres_lau1`, `region_nuts3`, and `region_iso` are unique across both countries and can be used as a key directly. The national codes `obec_kod`, `okres_kod`, and `vusc_kod` are unique **within a single country only** — the RÚIAN and GKÚ number ranges overlap (in 4 cases for `obec_kod`), so always combine them with `country_code`.

## Map shapes

The `shapes/` folder contains boundaries of regions (kraje) and districts (okresy) as **TopoJSON in WGS84 (EPSG:4326)**. Each level comes in three variants — both countries together (`CZSK_`) and each one on its own (`CZ_`, `SK_`).

| file | shapes | size | join key |
|---|---|---|---|
| `shapes/CZSK_regions.topo.json` | 22 | 164 kB | `region_nuts3` (`CZ010`, `SK041`) |
| `shapes/CZ_regions.topo.json` | 14 | 120 kB | `region_nuts3` |
| `shapes/SK_regions.topo.json` | 8 | 43 kB | `region_nuts3` |
| `shapes/CZSK_districts.topo.json` | 156 | 351 kB | `okres_lau1` (`CZ0100`, `SK0319`) |
| `shapes/CZ_districts.topo.json` | 77 | 234 kB | `okres_lau1` |
| `shapes/SK_districts.topo.json` | 79 | 116 kB | `okres_lau1` |

Ke každému souboru je navíc varianta `_unknown` (např. `shapes/CZSK_regions_unknown.topo.json`) s prvkem pro neznámou hodnotu — viz [níže](#neznámé-hodnoty--1).

Each shape carries properties named exactly like the CSV columns — `country_code`, `region`, `vusc_kod`, `region_nuts3`, `region_iso` for regions, plus `county`, `okres_kod`, and `okres_lau1` for districts. Joining to the data needs no renaming.

The layer inside each file is named after the level (`regions` or `districts`) regardless of the variant, so a visual that references it works with all three files alike.

### Using them in Power BI

1. Enable **Shape Map** under `File → Options → Preview features` (the visual is still in preview).
2. Add a Shape Map visual and upload the `.topo.json` under `Format → Map shape → Add map`.
3. Put `region_nuts3` (regions) or `okres_lau1` (districts) into the `Location` field — **not the names**, they are not used for matching.

If your report covers a single country, use the `CZ_` or `SK_` variant — the map then fits that country's extent instead of leaving half the canvas empty.

If you hit the limits of Shape Map (no drill-through, limited formatting), the same files also work in the **Deneb** visual (Vega-Lite supports TopoJSON natively) or in Icon Map Pro.

The geometry is simplified to 5 % of the original vertex count. This preserves the shape of the boundaries even at full-screen size, but the data is not meant for measurement or cadastral use — download the originals from ČÚZK and GKÚ for that.

*Note: The Czech and Slovak boundaries come from two independent registers, so there may be minor discrepancies of a few metres along the shared national border. They are not visible in a visualisation.*

### Updating the map shapes

The files are generated by `src/build_shapes.py`. Besides Python it requires **Node.js** — mapshaper, which handles the reprojection, simplification, and TopoJSON export, is fetched automatically via `npx`.

```
python src/ruian_data_processing.py   # the CSV first, the maps are verified against it
python src/build_shapes.py
```

The script finishes by checking that the keys in the maps match `seznampsc.csv` exactly in both directions, and fails on any mismatch.

## Data updates
To update the data, you can download the entire repository, install the required dependencies from `src/requirements.txt`, and run the `src/ruian_data_processing.py` script. If you update the URLs for the data sources in it, the script will generate the corresponding CSV files with the current data.

The `src/lookup_sk_obec.csv` codebook is part of the repository and does not normally need refreshing. If Slovak municipalities or districts change, regenerate it with `src/build_sk_lookup.py`.

If you are not comfortable doing this yourself, create an issue with a request for data update and I will take care of it.

### Data sources for the Czech Republic

The source is publicly available data from the Czech Office for Surveying, Mapping and Cadastre (ČÚZK) - specifically data about regions, districts, municipalities, and addresses.

The current link for the list of address points for the entire Czech Republic is https://nahlizenidokn.cuzk.gov.cz/StahniAdresniMistaRUIAN.aspx

Individual lists of regions, districts, and municipalities are https://services.cuzk.gov.cz/sestavy/cis/

The boundaries used for the map shapes are downloaded from https://services.cuzk.gov.cz/shp/stat/ (SHP in S-JTSK / Krovak, CC-BY 4.0 licence).

### Data sources for Slovakia

The source is publicly available data from the data.slovensko.sk portal - specifically data about addresses `Adresy podľa krajou (všetky kraje)` from https://data.slovensko.sk/datasety/b27f57f1-7e76-45e0-8968-631f9176b2e9

The address data does not contain municipality, district, or region codes (`obec_kod`, `okres_kod`, `okres_lau1`, `vusc_kod`, `region_nuts3`). These are joined in from the ZBGIS administrative boundaries register of the Geodetic and Cartographic Institute Bratislava via the WFS service https://zbgisws.skgeodesy.sk/zbgis_administrativne_hranice_wfs/service.svc/get (CC-BY 4.0 licence).

*Note: The URLs for downloading data may change, so it is necessary to check the current links on the above pages. The provided data is valid as of March 2026.*
