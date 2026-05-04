from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from utils.sector_utils import normalize_sector_value

def _get_sector_feature_column(df: pd.DataFrame) -> str | None:
    for col in ("Area", "sector_code"):
        if col in df.columns:
            return col
    return None

def _normalize_hour(hour: int, allowed_hours: List[int]) -> int:
    if hour in allowed_hours:
        return hour

    def circular_distance(candidate: int) -> int:
        diff = abs(candidate - hour)
        return min(diff, 24 - diff)

    return min(allowed_hours, key=circular_distance)



def _lookup_avg_tasu(sector_code: str, time_block: str, tables: Dict[str, Any]) -> float:
    sector_code = normalize_sector_value(sector_code)
    sector_time_avg = tables["sector_time_avg"]
    sector_avg = tables["sector_avg"]
    timeblock_avg = tables["timeblock_avg"]
    global_avg = float(tables["global_avg"])

    value = None

    if not sector_time_avg.empty:
        sector_col = _get_sector_feature_column(sector_time_avg)
        if sector_col is not None:
            sector_values = sector_time_avg[sector_col].dropna().astype(str).map(normalize_sector_value)
            match = sector_time_avg[(sector_values == sector_code) & (sector_time_avg["time_block"] == time_block)]
            if not match.empty:
                value = float(match.iloc[0]["avg_time_per_sector_block"])

    if value is None and not sector_avg.empty:
        sector_col = _get_sector_feature_column(sector_avg)
        if sector_col is not None:
            sector_values = sector_avg[sector_col].dropna().astype(str).map(normalize_sector_value)
            match = sector_avg[sector_values == sector_code]
            if not match.empty:
                value = float(match.iloc[0]["sector_avg_tasu"])

    if value is None and not timeblock_avg.empty:
        match = timeblock_avg[timeblock_avg["time_block"] == time_block]
        if not match.empty:
            value = float(match.iloc[0]["timeblock_avg_tasu"])

    if value is None:
        value = global_avg

    return float(value)



def predict_tasu_minutes(bundle: Dict[str, Any], sector_code: str, departure_dt: datetime) -> Dict[str, Any]:
    model = bundle["model"]
    best_iteration = bundle.get("best_iteration")
    feature_columns = bundle["feature_columns"]
    hour_mapping = bundle["hour_mapping"]
    avg_tables = bundle["avg_feature_tables"]

    normalized_sector_code = normalize_sector_value(sector_code)
    if not normalized_sector_code:
        raise ValueError("sector_code is empty after normalization.")

    allowed_hours = sorted(hour_mapping.keys())
    normalized_hour = _normalize_hour(departure_dt.hour, allowed_hours)
    time_block = hour_mapping[normalized_hour]
    weekday = int(departure_dt.weekday())

    avg_time_per_sector_block = _lookup_avg_tasu(
        sector_code=normalized_sector_code,
        time_block=time_block,
        tables=avg_tables,
    )

    row = {
        "weekday": weekday,
        "hour": normalized_hour,
        "avg_time_per_sector_block": avg_time_per_sector_block,
    }
    if "Area" in feature_columns:
        row["Area"] = normalized_sector_code
    if "sector_code" in feature_columns:
        row["sector_code"] = normalized_sector_code

    features = pd.DataFrame([row], columns=feature_columns)
    for col in ["Area", "sector_code", "weekday", "hour"]:
        if col in features.columns:
            features[col] = features[col].astype("category")

    prediction = float(model.predict(features, num_iteration=best_iteration)[0])
    prediction = max(0.1, min(prediction, 20.0))

    return {
        "predicted_tasu_minutes": prediction,
        "normalized_sector_code": normalized_sector_code,
        "normalized_hour": normalized_hour,
        "weekday": weekday,
        "time_block": time_block,
        "avg_time_per_sector_block": avg_time_per_sector_block,
    }
