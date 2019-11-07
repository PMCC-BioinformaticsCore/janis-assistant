from datetime import datetime

from dateutil import parser, tz


class DateUtil:
    @staticmethod
    def parse_iso(d):
        if not d:
            return None
        try:
            dd = parser.parse(d)
            return dd.astimezone(tz.UTC)

        except Exception as e:
            return None

    @staticmethod
    def as_utc(d):
        return d.astimezone(tz.UTC) if d else None

    @staticmethod
    def now():
        return datetime.utcnow().replace(tzinfo=tz.UTC)

    @staticmethod
    def max():
        return DateUtil.as_utc(
            datetime.max.replace(day=datetime.max.day - 1, tzinfo=tz.UTC)
        )

    @staticmethod
    def secs_difference(a: datetime, b: datetime):
        if a.tzinfo is None or b.tzinfo is None:
            aorb = a if a.tzinfo is None else b
            raise Exception(
                f"input '{aorb}' doesn't have tzinfo, "
                "please use DateUtil methods to create all dates"
            )

        return (b - a).total_seconds()
