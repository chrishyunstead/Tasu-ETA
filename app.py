from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

from clients.osrm_client import OsrmClient
from clients.route_result_client import RouteResultClient, extract_order_df
from model_loader import load_latest_model
from predict import predict_tasu_minutes
from queries.apartment_flag import ApartmentFlagProcessor
from queries.itemdata import ItemDatasetQuery
from utils.db_handler import DBHandler
from utils.db_handler_pg import PostgresDBHandler
from utils.event_parser import parse_event_payload
from utils.sector_utils import normalize_sector_value

logger = logging.getLogger()
logger.setLevel(logging.INFO)

KST = timezone(timedelta(hours=9))
APARTMENT_SERVICE_MINUTES = float(os.environ.get("APARTMENT_SERVICE_MINUTES", "3"))
DEFAULT_TASU_MINUTES = float(os.environ.get("DEFAULT_TASU_MINUTES", "5.0"))
DELIVERY_COMPLETE_STATUSES = {"DELIVERYCOMPLETE"}


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().upper()


def _is_shipping_notification_payload(payload: Dict[str, Any]) -> bool:
    return (
        str(payload.get("event_name") or "").strip() == "ShippingItemExternalNotification"
        or str(payload.get("source") or "").strip() == "eventbridge_shipping_complete"
    )


def _is_delivery_complete_payload(payload: Dict[str, Any]) -> bool:
    return (
        _is_shipping_notification_payload(payload)
        and _normalize_status(payload.get("status")) in DELIVERY_COMPLETE_STATUSES
        and payload.get("timestamp_delivery_complete") not in (None, "")
    )


def _kst_now() -> datetime:
    return datetime.now(KST)


def _to_kst_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(KST).isoformat()

# Lambda warm container에서 재사용하기 위해 module scope에 둔다.
db_handler = DBHandler()
db_handler_pg = PostgresDBHandler()
apartment_flag_query = ApartmentFlagProcessor(db_handler_pg)
item_dataset_query = ItemDatasetQuery(db_handler)


def _parse_kst_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        # EventBridge 배송완료 이벤트는 epoch milliseconds로 들어옴
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    else:
        text = str(value).strip()
        if text.isdigit():
            ts = float(text)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            text = text.replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)

    return dt.astimezone(KST)


def _get_float(payload: Dict[str, Any], key: str) -> Optional[float]:
    value = payload.get(key)
    if value in (None, ""):
        return None
    return float(value)


def _to_bool(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().lower() in {"1", "true", "t", "y", "yes", "아파트"}


def _normalize_tracking_number(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _extract_route_order_and_unit(route_result: Dict[str, Any]) -> Tuple[pd.DataFrame, Optional[Dict[str, float]]]:
    """
    경로최적화 API 결과에서는 ETA 계산에 필요한 순서 정보만 사용한다.
    - 필수: tracking_number, ordering
    - 선택: sub_order
    - unit row가 있으면 첫 배송지까지의 OSRM leg 계산에 사용한다.
    """
    route_df = extract_order_df(route_result)
    if route_df.empty:
        raise ValueError("Route result is empty.")

    if "tracking_number" not in route_df.columns or "ordering" not in route_df.columns:
        raise ValueError("Route result must include tracking_number and ordering.")

    work = route_df.copy()
    work["tracking_number"] = _normalize_tracking_number(work["tracking_number"])
    work["ordering"] = pd.to_numeric(work["ordering"], errors="coerce")

    if "sub_order" not in work.columns:
        work["sub_order"] = 0
    work["sub_order"] = pd.to_numeric(work["sub_order"], errors="coerce").fillna(0)

    unit: Optional[Dict[str, float]] = None
    unit_df = work[work["tracking_number"].str.lower() == "unit"].copy()
    if not unit_df.empty and {"lat", "lng"}.issubset(unit_df.columns):
        unit_df["lat"] = pd.to_numeric(unit_df["lat"], errors="coerce")
        unit_df["lng"] = pd.to_numeric(unit_df["lng"], errors="coerce")
        unit_df = unit_df.dropna(subset=["lat", "lng", "ordering"])
        if not unit_df.empty:
            unit_row = unit_df.sort_values(["ordering", "sub_order"]).iloc[0]
            unit = {"lat": float(unit_row["lat"]), "lng": float(unit_row["lng"])}

    order_df = work[work["tracking_number"].str.lower() != "unit"].copy()
    order_df = order_df.dropna(subset=["tracking_number", "ordering"])
    order_df = order_df[["tracking_number", "ordering", "sub_order"]]
    order_df = order_df.sort_values(["ordering", "sub_order"]).drop_duplicates("tracking_number", keep="first")

    if order_df.empty:
        raise ValueError("No delivery items found in route result.")

    return order_df.reset_index(drop=True), unit


def _fetch_item_dataset(user_id: int) -> pd.DataFrame:
    df = item_dataset_query.item_dataset_df(user_id)
    if df is None or df.empty:
        raise ValueError("No undelivered item data found in DB.")

    required = [
        "tracking_number",
        "Area",
        "lat",
        "lng",
        "address_id",
        "timestamp_outfordelivery",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"item dataset missing required columns: {missing}")

    work = df.copy()
    work["tracking_number"] = _normalize_tracking_number(work["tracking_number"])
    work["lat"] = pd.to_numeric(work["lat"], errors="coerce")
    work["lng"] = pd.to_numeric(work["lng"], errors="coerce")
    work["address_id"] = work["address_id"].astype(str).str.strip()
    # DB timestamp_outfordelivery는 UTC naive 값으로 내려오는 경우가 있어,
    # ETA 기준 시각으로 사용할 때는 UTC로 해석한 뒤 KST로 변환한다.
    work["timestamp_outfordelivery"] = pd.to_datetime(
        work["timestamp_outfordelivery"], errors="coerce", utc=True
    ).dt.tz_convert(KST)
    work = work.dropna(subset=["tracking_number", "lat", "lng", "timestamp_outfordelivery"])
    work = work.drop_duplicates("tracking_number", keep="first")

    if work.empty:
        raise ValueError("item dataset is empty after cleaning.")

    return work.reset_index(drop=True)


def _fetch_apartment_flags(item_df: pd.DataFrame) -> pd.DataFrame:
    address_ids = (
        item_df["address_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )
    if not address_ids:
        return pd.DataFrame(columns=["address_id", "apartment_flag"])

    flag_df = apartment_flag_query.apartmentflag_data(address_ids)
    if flag_df is None or flag_df.empty:
        return pd.DataFrame(columns=["address_id", "apartment_flag"])

    work = flag_df.copy()
    work["address_id"] = work["address_id"].astype(str).str.strip()
    work["apartment_flag"] = work["apartment_flag"].map(_to_bool)
    return work.drop_duplicates("address_id", keep="first")


def _merge_eta_source(item_df: pd.DataFrame, order_df: pd.DataFrame, strict_route_merge: bool) -> Tuple[pd.DataFrame, List[str]]:
    """
    조인 중심은 DAAS의 미배송 송장이다.
    item + route ordering/sub_order + GIS apartment_flag 순서로 붙인다.
    """
    merged = item_df.merge(order_df, on="tracking_number", how="left", validate="one_to_one")

    missing_route_df = merged[merged["ordering"].isna()].copy()
    missing_route_tns = missing_route_df["tracking_number"].astype(str).tolist()
    if missing_route_tns and strict_route_merge:
        raise ValueError(f"Route ordering missing for undelivered items: {missing_route_tns[:20]}")

    if missing_route_tns:
        logger.warning("[merge] route ordering missing. count=%s sample=%s", len(missing_route_tns), missing_route_tns[:20])
        merged = merged[merged["ordering"].notna()].copy()

    if merged.empty:
        raise ValueError("No items left after merging item data with route ordering.")

    flag_df = _fetch_apartment_flags(merged)
    merged = merged.merge(flag_df, on="address_id", how="left")
    merged["apartment_flag"] = merged["apartment_flag"].map(_to_bool)

    merged["ordering"] = pd.to_numeric(merged["ordering"], errors="coerce")
    merged["sub_order"] = pd.to_numeric(merged["sub_order"], errors="coerce").fillna(0)
    merged = merged.sort_values(["ordering", "sub_order", "tracking_number"]).reset_index(drop=True)
    return merged, missing_route_tns


def _resolve_sector_code(payload: Dict[str, Any], item_df: pd.DataFrame) -> str:
    raw_sector_code = str(payload.get("sector_code") or "").strip()
    normalized_from_payload = normalize_sector_value(raw_sector_code)
    if normalized_from_payload:
        return normalized_from_payload

    if "Area" in item_df.columns:
        areas = item_df["Area"].dropna().astype(str).str.strip()
        areas = areas[areas != ""]
        if not areas.empty:
            # 여러 Area가 섞이면 가장 많이 등장한 Area 기준으로 타수 모델을 조회한다.
            most_common_area = areas.value_counts().idxmax()
            normalized_from_area = normalize_sector_value(most_common_area)
            if normalized_from_area:
                return normalized_from_area

    raise ValueError("sector_code is required. It can also be inferred from item Area.")

def _get_model_sector_column(table: Any) -> Optional[str]:
    if table is None:
        return None
    columns = getattr(table, "columns", [])
    for col in ("Area", "sector_code"):
        if col in columns:
            return col
    return None

def _resolve_eta_base_time(
    payload: Dict[str, Any],
    item_df: pd.DataFrame,
    *,
    has_existing_eta: bool,
) -> datetime:
    """
    최초 ETA 생성:
        base_time = 남은 아이템 timestamp_outfordelivery max

    이후 ETA 갱신:
        base_time = 현재 시각(KST)

    판단 기준:
        - user_id 기준으로 DynamoDB에 기존 ETA가 없으면 최초 계산으로 본다.
        - 기존 ETA가 있으면 호출 종류와 무관하게 현재 시각(KST)을 사용한다.
    """

    if has_existing_eta:
        return _kst_now()

    latest_out_for_delivery = item_df["timestamp_outfordelivery"].max()
    out_dt = _parse_kst_datetime(latest_out_for_delivery)
    if out_dt is None:
        raise ValueError("timestamp_outfordelivery is empty. Cannot resolve ETA base_time.")

    return out_dt.astimezone(KST)


def _is_sector_known_in_model_bundle(bundle: Dict[str, Any], sector_code: str) -> bool:
    """
    모델 학습/집계 테이블에 없는 지역이면 예측을 강행하지 않고 기본 타수로 fallback한다.
    LightGBM categorical feature가 미등록 카테고리를 처리하더라도 운영상 신뢰도가 낮으므로,
    sector_avg 또는 sector_time_avg에 존재하는 지역만 모델 예측 대상으로 본다.
    """
    normalized_sector_code = normalize_sector_value(sector_code)
    if not normalized_sector_code:
        return False

    avg_tables = bundle.get("avg_feature_tables") or {}
    for table_name in ("sector_avg", "sector_time_avg"):
        table = avg_tables.get(table_name)
        if table is None or getattr(table, "empty", True):
            continue
        sector_col = _get_model_sector_column(table)
        if sector_col is None:
            continue
        sector_values = table[sector_col].dropna().astype(str).map(normalize_sector_value)
        if (sector_values == normalized_sector_code).any():
            return True

    return False

def _has_existing_eta_in_dynamodb(
    *,
    table_name: str,
    user_id: int,
) -> bool:
    if not table_name:
        return False

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ProjectionExpression="tracking_number",
        Limit=1,
    )
    return bool(response.get("Items"))


def _find_existing_route_s3_from_dynamodb(
    *,
    table_name: str,
    user_id: int,
) -> Dict[str, Any]:
    if not table_name:
        return {}

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        Limit=1,
    )

    items = response.get("Items", [])
    if not items:
        return {}

    route_s3 = items[0].get("route_s3") or {}
    if isinstance(route_s3, dict) and route_s3.get("bucket") and route_s3.get("key"):
        return {
            "bucket": str(route_s3["bucket"]),
            "key": str(route_s3["key"]),
            "etag": str(route_s3.get("etag", "")) if route_s3.get("etag") else None,
        }

    return {}

def _fallback_prediction_context(sector_code: str, departure_dt: datetime, reason: str) -> Dict[str, Any]:
    normalized_sector_code = normalize_sector_value(sector_code) or str(sector_code or "").strip()
    return {
        "predicted_tasu_minutes": DEFAULT_TASU_MINUTES,
        "normalized_sector_code": normalized_sector_code,
        "normalized_hour": int(departure_dt.hour),
        "weekday": int(departure_dt.weekday()),
        "time_block": None,
        "avg_time_per_sector_block": DEFAULT_TASU_MINUTES,
        "fallback_used": True,
        "fallback_reason": reason,
        "default_tasu_minutes": DEFAULT_TASU_MINUTES,
    }


def _predict_tasu_minutes_with_fallback(bundle: Dict[str, Any], sector_code: str, departure_dt: datetime) -> Dict[str, Any]:
    normalized_sector_code = normalize_sector_value(sector_code)
    if not normalized_sector_code:
        return _fallback_prediction_context(
            sector_code=sector_code,
            departure_dt=departure_dt,
            reason="EMPTY_SECTOR_CODE",
        )

    if not _is_sector_known_in_model_bundle(bundle, normalized_sector_code):
        logger.warning(
            "[prediction] sector_code not found in model bundle. sector_code=%s default_tasu=%s",
            normalized_sector_code,
            DEFAULT_TASU_MINUTES,
        )
        return _fallback_prediction_context(
            sector_code=normalized_sector_code,
            departure_dt=departure_dt,
            reason="UNKNOWN_SECTOR_CODE",
        )

    try:
        pred = predict_tasu_minutes(bundle, normalized_sector_code, departure_dt)
        pred["fallback_used"] = False
        pred["fallback_reason"] = None
        pred["default_tasu_minutes"] = DEFAULT_TASU_MINUTES
        return pred
    except Exception as exc:
        logger.exception(
            "[prediction] model prediction failed. sector_code=%s default_tasu=%s",
            normalized_sector_code,
            DEFAULT_TASU_MINUTES,
        )
        return _fallback_prediction_context(
            sector_code=normalized_sector_code,
            departure_dt=departure_dt,
            reason=f"MODEL_PREDICTION_FAILED: {str(exc)}",
        )

def _build_coordinate_chain(unit: Optional[Dict[str, float]], item_df: pd.DataFrame) -> Tuple[List[Dict[str, float]], bool]:
    coords: List[Dict[str, float]] = []
    has_unit = unit is not None
    if has_unit:
        coords.append({"lat": float(unit["lat"]), "lng": float(unit["lng"])})
    coords.extend(item_df[["lat", "lng"]].astype(float).to_dict("records"))
    return coords, has_unit


def _compute_average_edge_distance(legs: List[Dict[str, Any]]) -> float:
    distances = [float(leg.get("distance", 0.0) or 0.0) for leg in legs]
    positives = [d for d in distances if d > 0]
    if not positives:
        return 1.0
    return sum(positives) / len(positives)


def _compute_eta_rows(
    departure_dt: datetime,
    predicted_tasu_minutes: float,
    item_df: pd.DataFrame,
    legs: List[Dict[str, Any]],
    has_unit: bool,
    weight_min: float,
    weight_max: float,
) -> Tuple[List[Dict[str, Any]], float]:
    edge_distances = [float(leg.get("distance", 0.0) or 0.0) for leg in legs]
    avg_edge_distance_m = _compute_average_edge_distance(legs)

    rows: List[Dict[str, Any]] = []
    cumulative_minutes = 0.0
    stop_eta_by_address_id: Dict[str, Dict[str, Any]] = {}

    for idx, (_, row) in enumerate(item_df.iterrows()):
        previous_row = item_df.iloc[idx - 1] if idx > 0 else None
        previous_is_apartment = bool(previous_row["apartment_flag"]) if previous_row is not None else False
        current_is_apartment = bool(row["apartment_flag"])

        address_id = str(row.get("address_id") or "").strip()
        same_ordering_group = False
        if previous_row is not None:
            same_ordering_group = int(row["ordering"]) == int(previous_row["ordering"])

        same_address_group = bool(address_id) and address_id in stop_eta_by_address_id

        if same_address_group:
            representative = stop_eta_by_address_id[address_id]
            edge_distance_m = 0.0
            raw_weight = 0.0
            weight = 0.0
            travel_minutes = 0.0
            service_minutes = 0.0
            adjusted_tasu = 0.0
            eta_value = representative["eta"]
            cumulative_value = representative["cumulative_minutes"]
        else:
            service_minutes = APARTMENT_SERVICE_MINUTES * int(previous_is_apartment) + APARTMENT_SERVICE_MINUTES * int(current_is_apartment)

            if same_ordering_group:
                # 같은 ordering 안에서는 sub_order 순서만 반영하고, 이동시간은 0으로 둔다.
                edge_distance_m = 0.0
                raw_weight = 0.0
                weight = 0.0
                travel_minutes = 0.0
            else:
                if has_unit:
                    edge_idx = idx
                else:
                    edge_idx = idx - 1

                if edge_idx < 0 or edge_idx >= len(edge_distances):
                    # unit 좌표가 없으면 첫 stop은 기준 타수 1회로 계산한다.
                    edge_distance_m = avg_edge_distance_m
                    raw_weight = 1.0
                else:
                    edge_distance_m = edge_distances[edge_idx]
                    raw_weight = edge_distance_m / avg_edge_distance_m if avg_edge_distance_m > 0 else 1.0

                weight = max(weight_min, min(raw_weight, weight_max))
                travel_minutes = predicted_tasu_minutes * weight

            adjusted_tasu = travel_minutes + service_minutes
            cumulative_minutes += adjusted_tasu
            eta_dt = departure_dt.astimezone(KST) + timedelta(minutes=cumulative_minutes)
            eta_value = _to_kst_iso(eta_dt)
            cumulative_value = round(cumulative_minutes, 4)

            if address_id:
                stop_eta_by_address_id[address_id] = {
                    "eta": eta_value,
                    "cumulative_minutes": cumulative_value,
                }

        rows.append(
            {
                "tracking_number": str(row["tracking_number"]),
                "ordering": int(row["ordering"]),
                "sub_order": int(row.get("sub_order", 0) or 0),
                "area": row.get("Area"),
                "address_id": address_id,
                "address_road": row.get("address_road"),
                "address2": row.get("address2"),
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "apartment_flag": current_is_apartment,
                "previous_apartment_flag": previous_is_apartment,
                "same_ordering_group": same_ordering_group,
                "same_address_group": same_address_group,
                "edge_distance_m": round(edge_distance_m, 2),
                "raw_weight": round(raw_weight, 4),
                "weight": round(weight, 4),
                "base_tasu_minutes": round(travel_minutes, 4),
                "apartment_service_minutes": round(service_minutes, 4),
                "adjusted_tasu_minutes": round(adjusted_tasu, 4),
                "cumulative_minutes": cumulative_value,
                "eta": eta_value,
            }
        )

    return rows, avg_edge_distance_m




def _to_dynamodb_value(value: Any) -> Any:
    """boto3 DynamoDB resource는 float을 직접 저장할 수 없어서 Decimal로 변환한다."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, dict):
        converted_dict: Dict[str, Any] = {}
        for k, v in value.items():
            converted = _to_dynamodb_value(v)
            if converted is not None:
                converted_dict[str(k)] = converted
        return converted_dict

    if isinstance(value, list):
        converted_list: List[Any] = []
        for v in value:
            converted = _to_dynamodb_value(v)
            if converted is not None:
                converted_list.append(converted)
        return converted_list

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return Decimal(str(value))

    if isinstance(value, Decimal):
        return value

    if isinstance(value, datetime):
        return value.astimezone(KST).isoformat()

    return str(value)


def _clean_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in item.items():
        converted = _to_dynamodb_value(value)
        if converted is not None:
            cleaned[key] = converted
    return cleaned


def _save_eta_rows_to_dynamodb(
    *,
    table_name: str,
    user_id: int,
    eta_rows: List[Dict[str, Any]],
    body: Dict[str, Any],
) -> Dict[str, Any]:
    """
    DynamoDB 최신 ETA 테이블 저장.
    Key: user_id(Number) + tracking_number(String)

    현재 계산된 미배송 송장은 upsert하고,
    같은 user_id 기준으로 이전에 저장돼 있었지만 현재 계산 대상에서 빠진 송장은 삭제한다.
    """
    if not table_name:
        return {"enabled": False, "table_name": None, "upserted_count": 0, "deleted_stale_count": 0}

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    calculated_at = _to_kst_iso(_kst_now())
    current_tracking_numbers = {str(row["tracking_number"]) for row in eta_rows}

    existing_tracking_numbers: set[str] = set()
    last_evaluated_key = None
    while True:
        query_kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "tracking_number",
        }
        if last_evaluated_key:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.query(**query_kwargs)
        for item in response.get("Items", []):
            tracking_number = item.get("tracking_number")
            if tracking_number is not None:
                existing_tracking_numbers.add(str(tracking_number))

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    stale_tracking_numbers = existing_tracking_numbers - current_tracking_numbers

    common_fields = {
        "user_id": user_id,
        "sector_code": body.get("sector_code"),
        "base_time": body.get("departure_time"),
        "departure_time": body.get("departure_time"),
        "model_version": body.get("model_version"),
        "predicted_tasu_minutes": body.get("predicted_tasu_minutes"),
        "prediction_context": body.get("prediction_context"),
        "distance_weighting": body.get("distance_weighting"),
        "service_time_rule": body.get("service_time_rule"),
        "route_meta": body.get("route_meta"),
        "calculated_at": calculated_at,
        "source": "eta_lambda",
        "route_s3": body.get("meta", {}).get("route_s3"),
    }

    with table.batch_writer(overwrite_by_pkeys=["user_id", "tracking_number"]) as batch:
        for row in eta_rows:
            item = {
                **common_fields,
                **row,
                "user_id": user_id,
                "tracking_number": str(row["tracking_number"]),
            }
            batch.put_item(Item=_clean_dynamodb_item(item))

        for tracking_number in stale_tracking_numbers:
            batch.delete_item(Key={"user_id": user_id, "tracking_number": str(tracking_number)})

    logger.info(
        "[dynamodb] table=%s user_id=%s upserted=%s deleted_stale=%s",
        table_name,
        user_id,
        len(eta_rows),
        len(stale_tracking_numbers),
    )

    return {
        "enabled": True,
        "table_name": table_name,
        "upserted_count": len(eta_rows),
        "deleted_stale_count": len(stale_tracking_numbers),
        "calculated_at": calculated_at,
    }


def _build_headers(status_code: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "POST,OPTIONS,GET",
        },
    }


def _success(body: Dict[str, Any]) -> Dict[str, Any]:
    resp = _build_headers(200)
    resp["body"] = json.dumps(body, ensure_ascii=False, default=str)
    return resp


def _error(status_code: int, message: str, error_code: str) -> Dict[str, Any]:
    resp = _build_headers(status_code)
    resp["body"] = json.dumps(
        {"success": False, "error_code": error_code, "message": message},
        ensure_ascii=False,
    )
    return resp


def lambda_handler(event, context):
    # 전체 실행시간 측정용. perf_counter는 단조 증가 타이머라 시스템 시간 변경 영향을 받지 않음.
    perf_start = time.perf_counter()
    perf_status = "unknown"
    perf_user_id = None
    perf_source = None
    perf_event_name = None
    perf_route_source = None
    perf_items = None
    perf_eta_items = None
    perf_dynamo_upserted = None
    perf_dynamo_deleted_stale = None

    try:
        if isinstance(event, dict) and event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
            perf_status = "options"
            return _build_headers(200)
        if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
            perf_status = "options"
            return _build_headers(200)

        payload = parse_event_payload(event)
        perf_source = payload.get("source")
        perf_event_name = payload.get("event_name")
        logger.info("[lambda] payload=%s", json.dumps(payload, ensure_ascii=False, default=str))

        if _is_shipping_notification_payload(payload) and not _is_delivery_complete_payload(payload):
            perf_status = "skipped_not_delivery_complete"
            logger.info(
                "[lambda] skip shipping notification. status=%s timestamp_delivery_complete=%s tracking_number=%s",
                payload.get("status"),
                payload.get("timestamp_delivery_complete"),
                payload.get("tracking_number"),
            )
            return _success({
                "success": True,
                "skipped": True,
                "reason": "NOT_DELIVERYCOMPLETE_EVENT",
                "tracking_number": payload.get("tracking_number"),
                "status": payload.get("status"),
            })

        user_id = payload.get("user_id")

        if user_id is None and payload.get("tracking_number"):
            user_df = item_dataset_query.user_id_by_tracking_number(
                str(payload["tracking_number"])
            )

            if user_df.empty:
                perf_status = "skipped_user_id_not_found"
                return _success({
                    "success": True,
                    "skipped": True,
                    "reason": "USER_ID_NOT_FOUND_BY_TRACKING_NUMBER",
                    "tracking_number": payload.get("tracking_number"),
                })

            user_id = int(user_df.iloc[0]["user_id"])

        if user_id is None:
            perf_status = "missing_user_id"
            return _error(400, "user_id or tracking_number is required.", "MISSING_USER_ID")

        user_id = int(user_id)
        perf_user_id = user_id

        model_bucket = os.environ["MODEL_S3_BUCKET"]
        model_prefix = os.environ.get("MODEL_S3_PREFIX", "tasu-predict")
        route_api_url = payload.get("route_result_api_url") or os.environ.get("ROUTE_RESULT_API_URL", "")
        route_api_header_name = os.environ.get("ROUTE_RESULT_API_HEADER_NAME", "")
        route_api_header_value = os.environ.get("ROUTE_RESULT_API_HEADER_VALUE", "")
        osrm_base_url = payload.get("osrm_base_url") or os.environ.get("OSRM_BASE_URL", "http://your-osrm-endpoint:5000")
        weight_min = float(os.environ.get("WEIGHT_CLIP_MIN", "0.7"))
        weight_max = float(os.environ.get("WEIGHT_CLIP_MAX", "1.3"))
        route_api_timeout = int(os.environ.get("ROUTE_RESULT_API_TIMEOUT_SECONDS", "15"))
        osrm_timeout = int(os.environ.get("OSRM_TIMEOUT_SECONDS", "10"))
        strict_route_merge = str(os.environ.get("STRICT_ROUTE_MERGE", "false")).lower() == "true"
        dynamodb_table_name = os.environ.get("DYNAMODB_TABLE_NAME", "delivery_eta_latest")

        has_existing_eta = _has_existing_eta_in_dynamodb(
            table_name=dynamodb_table_name,
            user_id=user_id,
        )

        item_df = _fetch_item_dataset(user_id)
        perf_items = int(len(item_df))
        sector_code = _resolve_sector_code(payload, item_df)

        route_client = RouteResultClient(
            api_url=route_api_url,
            timeout_seconds=route_api_timeout,
            header_name=route_api_header_name or None,
            header_value=route_api_header_value or None,
        )
        route_s3 = payload.get("route_s3") or {}

        if not route_s3.get("bucket") or not route_s3.get("key"):
            route_s3 = _find_existing_route_s3_from_dynamodb(
                table_name=dynamodb_table_name,
                user_id=user_id,
            )

        if route_s3.get("bucket") and route_s3.get("key"):
            perf_route_source = "s3"
            route_result = route_client.fetch_from_s3(
                bucket=route_s3["bucket"],
                key=route_s3["key"],
            )
        else:
            perf_route_source = "api"
            route_result = route_client.fetch_latest_ordering(user_id=user_id)

        order_df, unit = _extract_route_order_and_unit(route_result)
        eta_source_df, missing_route_tns = _merge_eta_source(
            item_df,
            order_df,
            strict_route_merge=strict_route_merge,
        )
        departure_dt = _resolve_eta_base_time(payload, eta_source_df, has_existing_eta=has_existing_eta)

        bundle, latest = load_latest_model(model_bucket, model_prefix)
        pred = _predict_tasu_minutes_with_fallback(bundle, sector_code, departure_dt)
        predicted_tasu_minutes = pred["predicted_tasu_minutes"]

        coords, has_unit = _build_coordinate_chain(unit, eta_source_df)
        osrm_client = OsrmClient(base_url=osrm_base_url, timeout_seconds=osrm_timeout)
        legs = osrm_client.route_legs(coords) if len(coords) >= 2 else []

        eta_rows, avg_edge_distance_m = _compute_eta_rows(
            departure_dt=departure_dt,
            predicted_tasu_minutes=predicted_tasu_minutes,
            item_df=eta_source_df,
            legs=legs,
            has_unit=has_unit,
            weight_min=weight_min,
            weight_max=weight_max,
        )
        perf_eta_items = len(eta_rows)

        route_meta = route_result.get("meta", {}) if isinstance(route_result, dict) else {}
        body = {
            "success": True,
            "user_id": user_id,
            "sector_code": pred["normalized_sector_code"],
            "departure_time": _to_kst_iso(departure_dt),
            "model_version": latest["model_version"],
            "model_bucket": latest["s3_bucket"],
            "model_key": latest["model_key"],
            "predicted_tasu_minutes": round(predicted_tasu_minutes, 4),
            "prediction": {
                "predicted_tasu_minutes": round(predicted_tasu_minutes, 4),
                "fallback_used": bool(pred.get("fallback_used", False)),
                "fallback_reason": pred.get("fallback_reason"),
                "default_tasu_minutes": round(float(pred.get("default_tasu_minutes", DEFAULT_TASU_MINUTES)), 4),
            },
            "prediction_context": {
                "weekday": pred["weekday"],
                "normalized_hour": pred["normalized_hour"],
                "time_block": pred.get("time_block"),
                "avg_time_per_sector_block": round(float(pred["avg_time_per_sector_block"]), 4),
                "fallback_used": bool(pred.get("fallback_used", False)),
                "fallback_reason": pred.get("fallback_reason"),
                "default_tasu_minutes": round(float(pred.get("default_tasu_minutes", DEFAULT_TASU_MINUTES)), 4),
            },
            "distance_weighting": {
                "avg_edge_distance_m": round(avg_edge_distance_m, 2),
                "weight_clip_min": weight_min,
                "weight_clip_max": weight_max,
                "has_unit_coordinate": has_unit,
            },
            "service_time_rule": {
                "apartment_service_minutes_per_true_side": APARTMENT_SERVICE_MINUTES,
                "rule": "previous/current apartment_flag가 true인 쪽마다 3분씩 추가",
            },
            "route_meta": {
                "request_id": route_result.get("request_id") if isinstance(route_result, dict) else None,
                "saved_at_utc": route_result.get("saved_at_utc") if isinstance(route_result, dict) else None,
                "status": route_meta.get("status"),
                "reason_code": route_meta.get("reason_code"),
                "reason": route_meta.get("reason"),
            },
            "meta": {
                "total_undelivered_items": int(len(item_df)),
                "total_eta_items": len(eta_rows),
                "missing_route_ordering_count": len(missing_route_tns),
                "missing_route_ordering_tracking_numbers": missing_route_tns[:50],
                "route_result_api_url": route_api_url,
                "osrm_base_url": osrm_base_url,
                "sector_value_mode": latest.get("sector_value_mode", "normalized_area"),
                "join_base": "undelivered_tracking_number",
                "route_result_source": "s3" if route_s3.get("bucket") and route_s3.get("key") else "api",
                "route_s3": route_s3,
            },
            "items": eta_rows,
        }
        dynamodb_save_result = _save_eta_rows_to_dynamodb(
            table_name=dynamodb_table_name,
            user_id=user_id,
            eta_rows=eta_rows,
            body=body,
        )
        body["dynamodb"] = dynamodb_save_result
        perf_dynamo_upserted = dynamodb_save_result.get("upserted_count")
        perf_dynamo_deleted_stale = dynamodb_save_result.get("deleted_stale_count")

        logger.info("[lambda] response=%s", json.dumps(body, ensure_ascii=False, default=str)[:4000])
        perf_status = "success"
        return _success(body)

    except KeyError as exc:
        perf_status = "missing_field"
        return _error(400, f"Missing required field: {exc}", "MISSING_FIELD")
    except ValueError as exc:
        perf_status = "invalid_input"
        return _error(400, str(exc), "INVALID_INPUT")
    except Exception as exc:
        perf_status = "internal_error"
        logger.exception("[lambda] unexpected error: %s", exc)
        return _error(500, str(exc), "INTERNAL_SERVER_ERROR")
    finally:
        elapsed_ms = round((time.perf_counter() - perf_start) * 1000, 2)
        request_id = getattr(context, "aws_request_id", None) if context is not None else None
        logger.info(
            "[perf][lambda_total] elapsed_ms=%s status=%s user_id=%s source=%s event_name=%s "
            "route_result_source=%s items=%s eta_items=%s dynamodb_upserted=%s "
            "dynamodb_deleted_stale=%s request_id=%s",
            elapsed_ms,
            perf_status,
            perf_user_id,
            perf_source,
            perf_event_name,
            perf_route_source,
            perf_items,
            perf_eta_items,
            perf_dynamo_upserted,
            perf_dynamo_deleted_stale,
            request_id,
        )