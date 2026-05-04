import json
import logging
import os
import tempfile
from typing import Any, Dict, Tuple

import boto3
import joblib

logger = logging.getLogger(__name__)

S3_CLIENT = boto3.client("s3")
_MODEL_CACHE: Dict[str, Any] = {
    "model_version": None,
    "bundle": None,
    "latest": None,
}


def _read_json(bucket: str, key: str) -> Dict[str, Any]:
    obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))



def load_latest_model(bucket: str, prefix: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    prefix = prefix.strip("/")
    latest_key = f"{prefix}/latest.json"
    latest = _read_json(bucket, latest_key)
    model_version = latest["model_version"]

    if _MODEL_CACHE["model_version"] == model_version and _MODEL_CACHE["bundle"] is not None:
        logger.info("[model] cache hit model_version=%s", model_version)
        return _MODEL_CACHE["bundle"], latest

    model_key = latest["model_key"]
    logger.info("[model] downloading artifact s3://%s/%s", bucket, model_key)

    with tempfile.NamedTemporaryFile(suffix=".joblib") as tmp_file:
        S3_CLIENT.download_file(bucket, model_key, tmp_file.name)
        bundle = joblib.load(tmp_file.name)

    _MODEL_CACHE["model_version"] = model_version
    _MODEL_CACHE["bundle"] = bundle
    _MODEL_CACHE["latest"] = latest
    return bundle, latest
