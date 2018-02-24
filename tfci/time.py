from datetime import datetime

import pytz

ISO8601 = '%Y-%m-%d %H:%M:%S.%f'
ISO8601_DATE = '%Y-%m-%d'


def time_now():
    return datetime.now(tz=pytz.UTC)


def time_parse(v, format=ISO8601) -> datetime:
    return pytz.utc.localize(datetime.strptime(v, format))
