"""
This file contains information about Janis' BatchRun functionality
"""
from typing import List


class BatchRunRequirements:
    def __init__(self, fields: List[str], groupby: str):
        self.fields = fields
        self.groupby = groupby
