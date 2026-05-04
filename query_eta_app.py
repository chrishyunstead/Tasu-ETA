from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import boto3
from boto3.dynamodb.conditions import Key

from utils.event_parser import parse_event_payload

logger = logging.getLogger()
logger.setLevel(logging.INFO)

KST = timezone(timedelta(hours=9))


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return value.astimezone(KST).isoformat()
    return str(value)


def _build_headers(status_code: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
        },
    }


def _success(body: Dict[str, Any]) -> Dict[str, Any]:
    resp = _build_headers(200)
    resp["body"] = json.dumps(body, ensure_ascii=False, default=_json_default)
    return resp


def _error(status_code: int, message: str, error_code: str) -> Dict[str, Any]:
    resp = _build_headers(status_code)
    resp["body"] = json.dumps(
        {"success": False, "error_code": error_code, "message": message},
        ensure_ascii=False,
    )
    return resp


def _parse_payload(event: Any) -> Dict[str, Any]:
    """GET queryStringParameters와 JSON body 모두 지원한다."""
    payload: Dict[str, Any] = {}

    if isinstance(event, dict):
        query_params = event.get("queryStringParameters") or {}
        if isinstance(query_params, dict):
            payload.update({k: v for k, v in query_params.items() if v not in (None, "")})

    try:
        body_payload = parse_event_payload(event)
        if isinstance(body_payload, dict):
            payload.update({k: v for k, v in body_payload.items() if v not in (None, "")})
    except Exception:
        # GET 호출에는 body가 없을 수 있으므로 body parse 실패는 무시한다.
        pass

    return payload


def _extract_user_id(payload: Dict[str, Any]) -> int:
    value = payload.get("user_id")
    if value in (None, ""):
        raise ValueError("user_id is required.")
    return int(value)


def _get_tracking_number(payload: Dict[str, Any]) -> Optional[str]:
    value = payload.get("tracking_number")
    if value in (None, ""):
        return None
    return str(value).strip()


def _compact_eta_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tracking_number": item.get("tracking_number"),
        "ordering": item.get("ordering"),
        "lat": item.get("lat"),
        "lng": item.get("lng"),
        "address_id": item.get("address_id"),
        "adjusted_tasu_minutes": item.get("adjusted_tasu_minutes"),
        "cumulative_minutes": item.get("cumulative_minutes"),
        "eta": item.get("eta"),
    }


def _sort_eta_items(items: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]):
        ordering = item.get("ordering", 999999)
        sub_order = item.get("sub_order", 999999)
        try:
            ordering = int(ordering)
        except Exception:
            ordering = 999999
        try:
            sub_order = int(sub_order)
        except Exception:
            sub_order = 999999
        return (ordering, sub_order, str(item.get("tracking_number", "")))

    return sorted(items, key=sort_key)


def _query_user_eta(table, user_id: int) -> list[Dict[str, Any]]:
    items: list[Dict[str, Any]] = []
    last_evaluated_key = None

    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.query(**kwargs)
        items.extend(response.get("Items", []))

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return _sort_eta_items(items)


def lambda_handler(event, context):
    try:
        if isinstance(event, dict) and event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
            return _build_headers(200)
        if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
            return _build_headers(200)

        payload = _parse_payload(event)
        logger.info("[query-eta] payload=%s", json.dumps(payload, ensure_ascii=False, default=str))

        user_id = _extract_user_id(payload)
        tracking_number = _get_tracking_number(payload)
        table_name = os.environ.get("DYNAMODB_TABLE_NAME", "delivery_eta_latest")

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        if tracking_number:
            response = table.get_item(Key={"user_id": user_id, "tracking_number": tracking_number})
            item = response.get("Item")
            return _success(
                {
                    "success": True,
                    "user_id": user_id,
                    "count": 1 if item else 0,
                    "latest_calculated_at": item.get("calculated_at") if item else None,
                    "items": [_compact_eta_item(item)] if item else [],
                }
            )

        items = _query_user_eta(table, user_id)
        calculated_at_values = [str(item.get("calculated_at")) for item in items if item.get("calculated_at")]
        latest_calculated_at = max(calculated_at_values) if calculated_at_values else None
        compact_items = [_compact_eta_item(item) for item in items]

        return _success(
            {
                "success": True,
                "user_id": user_id,
                "count": len(compact_items),
                "latest_calculated_at": latest_calculated_at,
                "items": compact_items,
            }
        )

    except ValueError as exc:
        return _error(400, str(exc), "INVALID_INPUT")
    except Exception as exc:
        logger.exception("[query-eta] unexpected error: %s", exc)
        return _error(500, str(exc), "INTERNAL_SERVER_ERROR")
