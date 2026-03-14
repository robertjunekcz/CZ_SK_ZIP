# Seznam českých a slovenských PSČ včetně obcí, okresů a krajů

Soubory v tomto projektu obsahují zpracovaná data o PSČ, obcích, okresech a krajích pro Českou republiku a Slovensko. 

## Jednotlivé soubory
- `seznampsc.csv`: Deduplikovaný seznam PSČ s přiřazenými obcemi, okresy a kraji pro ČR a SR; je-li k PSČ přiřazeno více obcí, je jako unikátní uváděna ta s největším počtem adresních míst.
- `seznampsc_all.csv`: Kompletní seznam PSČ s přiřazenými obcemi, okresy a kraji pro ČR a SR; obsahuje všechny kombinace PSČ a obcí, **včetně duplicit.** Není tak obvykle vhodný pro běžné použití.

## Aktualizace dat
Chcete-li aktualizovat data, můžete si stáhnout celý repozitář, nainstalovat požadované závislosti z `src/requirements.txt` a spustit skript `src/ruian_data_processing.py`. Pokud v něm aktualizujete URL pro zdroje dat, skript sám vygeneruje příslušné CSV soubory s aktuálními daty.

Pokud si na to sami netroufáte, vytvořte issue s požadavkem na aktualizaci dat a já se o to postarám.

### Zdroje dat pro Českou republiku

Zdrojem jsou veřejně dostupná data z Českého úřadu zeměměřického a katastrálního (ČÚZK) - konkrétně data o krajích, okresech, obcích a adresách.

Aktuální odkaz pro seznam adresních míst z celé ČR je na https://nahlizenidokn.cuzk.gov.cz/StahniAdresniMistaRUIAN.aspx

Jednotlivé číselníky krajů, okresů a obcí jsou https://services.cuzk.gov.cz/sestavy/cis/

### Zdroje dat pro Slovensko

Zdrojem jsou veřejně dostupná data z portálu data.slovensko.sk - konkrétně data o adresách `Adresy podľa krajou (všetky kraje)` z https://data.slovensko.sk/datasety/b27f57f1-7e76-45e0-8968-631f9176b2e9


*Poznámka: Url pro stažení dat se může měnit, proto je potřeba zkontrolovat aktuální odkazy na výše uvedených stránkách. Uvedená data jsou platká k březnu 2026.*

---

# ENGLISH: List of Czech and Slovak postal codes (PSČ / ZIP) including municipalities, districts, and regions

The files in this project contain processed data about postal codes (PSČ), municipalities, districts, and regions for the Czech Republic and Slovakia.

## Individual files
- `seznampsc.csv`: A deduplicated list of postal codes with assigned municipalities, districts, and regions for the Czech Republic and Slovakia; if a postal code is assigned to multiple municipalities, the one with the largest number of address points is listed as unique.
- `seznampsc_all.csv`: A complete list of postal codes with assigned municipalities, districts, and regions for the Czech Republic and Slovakia; it contains all combinations of postal codes and municipalities, **including duplicates.** It is not usually suitable for general use.

## Data updates
To update the data, you can download the entire repository, install the required dependencies from `src/requirements.txt`, and run the `src/ruian_data_processing.py` script. If you update the URLs for the data sources in it, the script will generate the corresponding CSV files with the current data.

If you are not comfortable doing this yourself, create an issue with a request for data update and I will take care of it.

### Data sources for the Czech Republic

The source is publicly available data from the Czech Office for Surveying, Mapping and Cadastre (ČÚZK) - specifically data about regions, districts, municipalities, and addresses.

The current link for the list of address points for the entire Czech Republic is https://nahlizenidokn.cuzk.gov.cz/StahniAdresniMistaRUIAN.aspx

Individual lists of regions, districts, and municipalities are https://services.cuzk.gov.cz/sestavy/cis/

### Data sources for Slovakia

The source is publicly available data from the data.slovensko.sk portal - specifically data about addresses `Adresy podľa krajou (všetky kraje)` from https://data.slovensko.sk/datasety/b27f57f1-7e76-45e0-8968-631f9176b2e9

*Note: The URLs for downloading data may change, so it is necessary to check the current links on the above pages. The provided data is valid as of March 2026.*
