"""Vytvoří varianty map s prvkem pro neznámou hodnotu (`-1`).

Shape Map umí obarvit jen útvary, které v mapě existují. Řádky, u kterých kraj
ani okres neznáme, tak z vizuálu prostě zmizí. Tenhle skript proto do každé mapy
přidá jeden útvar navíc — čtverec ve volném rohu výřezu, jehož všechny kódy mají
hodnotu `-1`. Ať už spojujete přes `region_nuts3`, `okres_lau1`, nebo národní
kódy, neznámé řádky se sesypou do něj.

    python src/build_unknown_shapes.py

Skript pracuje nad hotovými soubory ve `shapes/`, nestahuje žádná zdrojová data
a nepotřebuje Node.js. Vstupy nemění, výstupy ukládá vedle nich s příponou
`_unknown`.

Souřadnice čtverce se počítají v Mercatorově projekci — tou se TopoJSON ve
Shape Map vykresluje, takže čtverec vyjde čtvercový i na obrazovce. Ve stupních
je proto širší, než vyšší.
"""

import json
import math
from pathlib import Path


SHAPES_DIR = Path(__file__).parent.parent / "shapes"

# Hodnota, kterou dostanou všechny kódové sloupce nového útvaru. Odpovídá tomu,
# čím se v datech obvykle značí "nezjištěno".
UNKNOWN_KEY = "-1"
UNKNOWN_LABEL = "Neznámé"

# Strana čtverce jako podíl výšky mapy (v projekci, tj. tak, jak ji vidíte na
# obrazovce), a odstup od okrajů výřezu ve stejných jednotkách.
SQUARE_SIZE = 0.20
MARGIN = 0.04

# Volný roh výřezu se u každé varianty liší — ČR a obě země dohromady mají místo
# vpravo nahoře (nad východním Slovenskem, resp. nad Moravskoslezským krajem),
# Slovensko naopak vpravo dole. Čtverec se tak vejde, aniž by mapa musela
# povyrůst a zmenšit se.
CORNERS = {
    "CZSK": "NE",
    "CZ": "NE",
    "SK": "SE",
}

LEVELS = ("regions", "districts")

# Textové sloupce dostanou popisek, všechny ostatní kód `-1`. Které vlastnosti
# útvar vůbec má, se čte ze zdrojového souboru, aby se seznam nemohl rozejít
# s tím, co generuje build_shapes.py.
NAME_PROPS = ("region", "county")


# ---------------------------------------------------------------------------
# Mercator
# ---------------------------------------------------------------------------

def to_merc(lat: float) -> float:
    return math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


def from_merc(y: float) -> float:
    return math.degrees(2 * math.atan(math.exp(y)) - math.pi / 2)


# ---------------------------------------------------------------------------
# Práce s kvantovaným TopoJSONem
# ---------------------------------------------------------------------------

def decode_arc(arcs: list, index: int) -> list[tuple[int, int]]:
    """Rozbalí delta-kódovaný oblouk do absolutních kvantovaných souřadnic."""
    arc = arcs[index if index >= 0 else ~index]
    points, x, y = [], 0, 0
    for dx, dy in arc:
        x += dx
        y += dy
        points.append((x, y))
    return points if index >= 0 else points[::-1]


def rings(topo: dict, layer: str):
    """Vrátí prstence všech útvarů jako seznamy kvantovaných bodů."""
    arcs = topo["arcs"]
    for geometry in topo["objects"][layer]["geometries"]:
        if geometry["type"] == "Polygon":
            polygons = [geometry["arcs"]]
        else:
            polygons = geometry["arcs"]
        for polygon in polygons:
            for ring in polygon:
                points = []
                for index in ring:
                    points += decode_arc(arcs, index)
                yield points


def bounds(topo: dict) -> tuple[float, float, float, float]:
    """Rozsah mapy ve stupních — v TopoJSONu z mapshaperu chybí `bbox`."""
    scale, translate = topo["transform"]["scale"], topo["transform"]["translate"]
    xs, ys = [], []
    for arc in topo["arcs"]:
        x = y = 0
        for dx, dy in arc:
            x += dx
            y += dy
            xs.append(x)
            ys.append(y)
    return (
        min(xs) * scale[0] + translate[0], max(xs) * scale[0] + translate[0],
        min(ys) * scale[1] + translate[1], max(ys) * scale[1] + translate[1],
    )


def quantize(topo: dict, lon: float, lat: float) -> tuple[int, int]:
    scale, translate = topo["transform"]["scale"], topo["transform"]["translate"]
    return (
        round((lon - translate[0]) / scale[0]),
        round((lat - translate[1]) / scale[1]),
    )


# ---------------------------------------------------------------------------
# Umístění čtverce
# ---------------------------------------------------------------------------

def square_coords(topo: dict, corner: str) -> tuple[float, float, float, float]:
    """Spočítá `(lon0, lon1, lat0, lat1)` čtverce v zadaném rohu výřezu."""
    lon_min, lon_max, lat_min, lat_max = bounds(topo)
    y_min, y_max = to_merc(lat_min), to_merc(lat_max)

    # Výška mapy v projekci; stupeň zeměpisné délky je v Mercatoru roven
    # jednomu radiánu / (180/π), proto se strana převádí přes stejný faktor.
    height = y_max - y_min
    side = SQUARE_SIZE * height
    gap = MARGIN * height
    side_lon = math.degrees(side)

    if corner.endswith("E"):
        lon1 = lon_max - math.degrees(gap)
        lon0 = lon1 - side_lon
    else:
        lon0 = lon_min + math.degrees(gap)
        lon1 = lon0 + side_lon

    if corner.startswith("N"):
        y1 = y_max - gap
        y0 = y1 - side
    else:
        y0 = y_min + gap
        y1 = y0 + side

    return lon0, lon1, from_merc(y0), from_merc(y1)


def segments_cross(a, b, c, d) -> bool:
    def side(p, q, r):
        return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])

    d1, d2 = side(a, b, c), side(a, b, d)
    d3, d4 = side(c, d, a), side(c, d, b)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def check_free(topo: dict, layer: str, box: tuple[int, int, int, int]) -> list[str]:
    """Ověří, že se čtverec nedotýká žádné hranice ani neleží uvnitř útvaru."""
    x0, x1, y0, y1 = box
    corners = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
    edges = list(zip(corners, corners[1:] + corners[:1]))
    center = ((x0 + x1) / 2, (y0 + y1) / 2)

    problems, crossings = [], 0
    for ring in rings(topo, layer):
        rx = [p[0] for p in ring]
        ry = [p[1] for p in ring]
        for point in ring:
            if x0 <= point[0] <= x1 and y0 <= point[1] <= y1:
                problems.append("hranice zasahuje dovnitř čtverce")
                break

        if min(rx) <= x1 and max(rx) >= x0 and min(ry) <= y1 and max(ry) >= y0:
            for p, q in zip(ring, ring[1:]):
                if any(segments_cross(p, q, e0, e1) for e0, e1 in edges):
                    problems.append("hranice protíná okraj čtverce")
                    break

        # paprsek doprava — sudý počet průsečíků znamená "venku"
        for p, q in zip(ring, ring[1:] + ring[:1]):
            if (p[1] > center[1]) != (q[1] > center[1]):
                x = p[0] + (center[1] - p[1]) * (q[0] - p[0]) / (q[1] - p[1])
                if x > center[0]:
                    crossings += 1

    if crossings % 2:
        problems.append("čtverec leží uvnitř některého útvaru")

    return sorted(set(problems))


# ---------------------------------------------------------------------------
# Sestavení varianty
# ---------------------------------------------------------------------------

def add_unknown(source: Path, name: str, variant: str) -> Path:
    topo = json.loads(source.read_text(encoding="utf-8"))
    layer = name

    lon0, lon1, lat0, lat1 = square_coords(topo, CORNERS[variant])
    x0, y0 = quantize(topo, lon0, lat0)
    x1, y1 = quantize(topo, lon1, lat1)

    problems = check_free(topo, layer, (x0, x1, y0, y1))
    if problems:
        raise RuntimeError(
            f"{source.name}: čtverec v rohu {CORNERS[variant]} koliduje s mapou "
            f"({'; '.join(problems)}). Upravte CORNERS nebo SQUARE_SIZE."
        )

    # Vnější prstenec po směru hodinových ručiček — stejnou orientaci mají
    # i útvary z mapshaperu, ať se soubor chová konzistentně.
    ring = [(x0, y0), (x0, y1), (x1, y1), (x1, y0), (x0, y0)]
    deltas, px, py = [], 0, 0
    for x, y in ring:
        deltas.append([x - px, y - py])
        px, py = x, y

    topo["arcs"].append(deltas)

    # Vlastnosti i jejich pořadí přebíráme od prvního útvaru — nový čtverec se
    # tak v tooltipu chová stejně jako kterýkoliv kraj nebo okres.
    reference = topo["objects"][layer]["geometries"][0]["properties"]
    properties = {
        key: UNKNOWN_LABEL if key in NAME_PROPS else UNKNOWN_KEY
        for key in reference
    }

    topo["objects"][layer]["geometries"].append({
        "arcs": [[len(topo["arcs"]) - 1]],
        "type": "Polygon",
        "properties": properties,
    })

    out = source.with_name(f"{source.name.split('.')[0]}_unknown.topo.json")
    out.write_text(json.dumps(topo, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    print(
        f"  {out.name}: čtverec {CORNERS[variant]} "
        f"lon [{lon0:.4f}, {lon1:.4f}], lat [{lat0:.4f}, {lat1:.4f}] "
        f"({out.stat().st_size / 1024:.0f} kB)"
    )
    return out


def main():
    print("=== Varianty map s prvkem pro neznámou hodnotu ===")
    built = []
    for name in LEVELS:
        for variant in CORNERS:
            source = SHAPES_DIR / f"{variant}_{name}.topo.json"
            if not source.exists():
                raise FileNotFoundError(
                    f"Chybí {source}. Nejdřív spusťte `python src/build_shapes.py`."
                )
            built.append(add_unknown(source, name, variant))
    print(f"\n=== Hotovo: {len(built)} souborů ===")


if __name__ == "__main__":
    main()
