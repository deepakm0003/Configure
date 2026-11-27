"""
build_roads.py – One-time script to download the drivable road network
around Koramangala, Bengaluru (12.9352, 77.6245), ~2.5 km radius,
and save to simulator/data/roads.geojson.

Run once:  python -m simulator.build_roads

Data is © OpenStreetMap contributors (ODbL). The output file is committed
together with this attribution notice.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    import osmnx as ox
    import networkx as nx
except ImportError:
    print("ERROR: osmnx is required. Install with: pip install osmnx", file=sys.stderr)
    sys.exit(1)

# Centre of Koramangala, Bengaluru
_LAT = 12.9352
_LON = 77.6245
_RADIUS_M = 2500  # ~2.5 km

# Default speed limits (km/h) by OSM highway type
_DEFAULT_SPEEDS: dict[str, int] = {
    "motorway": 80,
    "trunk": 60,
    "primary": 50,
    "secondary": 40,
    "tertiary": 30,
    "residential": 30,
    "unclassified": 30,
    "service": 20,
    "living_street": 15,
}


def _parse_speed(val: object) -> int:
    """Parse a maxspeed tag to km/h int, returning 0 on failure."""
    if val is None:
        return 0
    s = str(val).strip().lower().split()[0]
    try:
        return int(float(s))
    except ValueError:
        return 0


def build(output_path: str | Path = "simulator/data/roads.geojson") -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading drivable graph within {_RADIUS_M} m of ({_LAT}, {_LON})…")
    G = ox.graph_from_point((_LAT, _LON), dist=_RADIUS_M, network_type="drive", simplify=True)
    print(f"  {len(G.nodes)} nodes, {len(G.edges)} edges")

    # Convert to GeoDataFrame of edges
    _, edges = ox.graph_to_gdfs(G)
    edges = edges.reset_index()

    features = []
    for _, row in edges.iterrows():
        highway = row.get("highway", "unclassified")
        if isinstance(highway, list):
            highway = highway[0]

        # Speed limit
        maxspeed_raw = row.get("maxspeed", None)
        speed = _parse_speed(maxspeed_raw)
        if speed == 0:
            speed = _DEFAULT_SPEEDS.get(str(highway), 30)

        geom = row.geometry.__geo_interface__
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "u": int(row["u"]),
                "v": int(row["v"]),
                "key": int(row["key"]),
                "highway": highway,
                "speed_limit_kmh": speed,
                "name": row.get("name", ""),
                "length_m": round(float(row.get("length", 0)), 2),
                "osmid": str(row.get("osmid", "")),
            },
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "attribution": "© OpenStreetMap contributors, ODbL https://www.openstreetmap.org/copyright",
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)

    print(f"Saved {len(features)} edges to {output_path}")


if __name__ == "__main__":
    build()
