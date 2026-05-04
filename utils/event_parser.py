import base64
import json
from typing import Any, Dict


def parse_event_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(event, dict):
        detail = event.get("detail")
        detail_type = event.get("detail-type") or event.get("detailType")

        if isinstance(detail, dict):
            event_name = detail.get("event_name") or detail_type
            params = detail.get("params")

            if event_name == "ShippingItemExternalNotification":
                if isinstance(params, dict):
                    payload = dict(params)
                else:
                    payload = dict(detail)
                    payload.pop("params", None)

                payload["event_name"] = "ShippingItemExternalNotification"
                payload["source"] = "eventbridge_shipping_complete"
                return payload

    if event is None:
        return {}

    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object.")

    if "body" not in event:
        return event

    body = event.get("body")
    if body is None:
        return {}

    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8")
        except Exception as exc:
            raise ValueError(f"Failed to decode base64 body: {exc}") from exc

    if isinstance(body, dict):
        return body

    if isinstance(body, str):
        body = body.strip()
        if not body:
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON body: {exc}") from exc

    raise ValueError("Unsupported event body type.")
