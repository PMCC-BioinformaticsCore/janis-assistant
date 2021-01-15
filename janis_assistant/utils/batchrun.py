"""
This file contains information about Janis' BatchRun functionality
"""
from typing import List

from janis_assistant.data.models.util import Serializable


class BatchRunRequirements(Serializable):
    def __init__(self, fields: List[str], groupby: str):
        self.fields = fields
        self.groupby = groupby
