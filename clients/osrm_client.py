import logging
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)


class OsrmClient:
    def __init__(self, base_url: str, timeout_seconds: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def route_legs(self, coordinates: List[Dict[str, float]]) -> List[Dict[str, Any]]:
        if len(coordinates) < 2:
            raise ValueError("At least 2 coordinates are required.")

        coord_str = ";".join([f"{p['lng']},{p['lat']}" for p in coordinates])
        url = (
            f"{self.base_url}/route/v1/driving/{coord_str}"
            f"?overview=false&steps=false&annotations=false"
        )
        logger.info("[osrm] GET %s", url)
        response = requests.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()

        if data.get("code") != "Ok":
            raise ValueError(f"OSRM route failed: {data}")

        routes = data.get("routes") or []
        if not routes:
            raise ValueError("OSRM route response missing routes.")

        legs = routes[0].get("legs") or []
        if len(legs) != len(coordinates) - 1:
            raise ValueError(
                f"OSRM legs count mismatch. expected={len(coordinates)-1}, actual={len(legs)}"
            )

        return legs
