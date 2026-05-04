import re
from typing import Any


def normalize_sector_value(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip()
    if not text:
        return ""
    text = re.sub(r"[0-9]", "", text)
    text = re.sub(r"\s+", "", text)
    return text
