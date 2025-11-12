from datetime import datetime, date
from typing import Any, Optional


def meta_handler(obj: Any) -> Optional[str]:
    """Datetime Handlers (TODO: Have Converters for Various SQL Variants)

    Parameters
    ----------
    obj : datetime or date
        The datetime or date object to convert to a string

    Returns
    -------
    str
        The datetime or date object converted to a string
    """
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # None is handled by default JSON encoder
    return None
