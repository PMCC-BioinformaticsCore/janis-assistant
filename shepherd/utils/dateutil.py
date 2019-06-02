from typing import Optional, List
from datetime import datetime


class DateUtil:

    @staticmethod
    def parse_iso(d) -> Optional[datetime]:
        q = datetime.fromisoformat(d)
        return q
