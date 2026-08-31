"""
Convert numpy/pandas values to native Python types for JSON serialization.

Used by validation report exports only — does not alter business logic.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def to_json_native(value: Any) -> Any:
    """
    Recursively convert a value tree into JSON-serializable native Python types.

    - bool / numpy.bool_ -> bool
    - int / numpy.integer -> int
    - float / numpy.floating -> float (NaN/Inf -> null)
    - pandas.Timestamp / datetime / date -> ISO string
    - NaN / NA -> null
    - dict / list / tuple / set -> recursively converted
    """
    if value is None:
        return None

    if isinstance(value, dict):
        return {str(k): to_json_native(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [to_json_native(item) for item in value]

    if isinstance(value, pd.DataFrame):
        return to_json_native(value.to_dict(orient="records"))

    if isinstance(value, pd.Series):
        return to_json_native(value.tolist())

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, np.datetime64):
        if pd.isna(value):
            return None
        return pd.Timestamp(value).isoformat()

    if isinstance(value, np.generic):
        return to_json_native(value.item())

    if isinstance(value, (bool, np.bool_)):
        return bool(value)

    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        return int(value)

    if isinstance(value, (float, np.floating)):
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, str):
        return value

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return value


def dumps_json(obj: Any, *, indent: int | None = 2, **kwargs: Any) -> str:
    """json.dumps after recursive native-type conversion."""
    return json.dumps(to_json_native(obj), indent=indent, **kwargs)
