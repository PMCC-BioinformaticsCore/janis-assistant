from typing import Optional, List
from dateutil import parser

class DateUtil:

    @staticmethod
    def parse_iso(d):
        if not d:
            return None
        try:
            dd = parser.parse(d)
            return dd

        except Exception as e:
            return None
