from datetime import datetime, timedelta
from typing import Generator


class ApiException(Exception):
    """IsoStream API Exception"""

    pass


def time_chunk(start: datetime, end: datetime, delta: timedelta) -> Generator:
    """Return a generator for start / end times to break a timeframe up into chunks

    Parameters
    ----------
    start : datetime
        The start date
    end : datetime
        The end date
    delta : timedelta
        The chunk

    Returns
    -------
    Generator:  A generator of Tuple[datetime, datetime] for sub periods a max of delta apart
    """
    _start = start
    while _start < end:
        _end = min(_start + delta, end)
        yield _start, _end

        _start = _end
