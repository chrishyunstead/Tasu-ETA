import json
import logging
from typing import Any, Dict, Optional

import pandas as pd
import requests
import boto3
logger = logging.getLogger(__name__)


class RouteResultClient:
    def __init__(
        self,
        api_url: str,
        timeout_seconds: int = 15,
        header_name: Optional[str] = None,
        header_value: Optional[str] = None,
    ):
        self.api_url = api_url.strip() if api_url else ""
        self.timeout_seconds = timeout_seconds
        self.header_name = header_name
        self.header_value = header_value

    def _build_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.header_name and self.header_value:
            headers[self.header_name] = self.header_value
        return headers
    
    def fetch_from_s3(self, bucket: str, key: str) -> Dict[str, Any]:
        if not bucket or not key:
            raise ValueError("route_s3 bucket/key is required.")

        logger.info("[route-s3] get_object bucket=%s key=%s", bucket, key)

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        return json.loads(body)

    def fetch_latest_ordering(self, user_id: int) -> Dict[str, Any]:
        if not self.api_url:
            raise ValueError("ROUTE_RESULT_API_URL is empty.")

        payload = {"user_id": user_id}
        logger.info("[route-api] POST %s payload=%s", self.api_url, payload)
        response = requests.post(
            self.api_url,
            headers=self._build_headers(),
            json=payload,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict) and "body" in data and isinstance(data["body"], str):
            try:
                return json.loads(data["body"])
            except json.JSONDecodeError:
                return data
        return data


def packed_df_to_dataframe(obj: Dict[str, Any]) -> pd.DataFrame:
    columns = obj.get("columns") or []
    data = obj.get("data") or []

    if not columns:
        raise ValueError("Packed df response missing columns.")

    if data and isinstance(data[0], dict):
        return pd.DataFrame(data, columns=columns)
    return pd.DataFrame(data, columns=columns)


def extract_order_df(route_result: Dict[str, Any]) -> pd.DataFrame:
    candidates = []

    if isinstance(route_result, dict):
        result = route_result.get("result")
        if isinstance(result, dict) and isinstance(result.get("df_ordered"), dict):
            candidates.append(result["df_ordered"])
        if isinstance(route_result.get("df_ordered"), dict):
            candidates.append(route_result["df_ordered"])
        if isinstance(route_result.get("data"), list):
            return pd.DataFrame(route_result["data"])

    for candidate in candidates:
        if isinstance(candidate, dict) and "columns" in candidate and "data" in candidate:
            return packed_df_to_dataframe(candidate)

    raise ValueError("Could not extract df_ordered from route result API response.")
