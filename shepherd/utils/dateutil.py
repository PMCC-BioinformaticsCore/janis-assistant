from typing import Optional, List
from datetime import datetime


class DateUtil:

    @staticmethod
    def parse_iso(d) -> Optional[datetime]:
        if not d:
            return None
        try:
            return datetime.fromisoformat(d)

        except Exception as e:
            return None
