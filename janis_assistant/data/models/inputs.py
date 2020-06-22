import json
from janis_core.utils.logger import Logger


class WorkflowInputModel:
    def __init__(self, tag: str, value: str):
        self.tag = tag
        self.value = value

    def get_safe_value(self):
        return json.dumps(self.value)

    @staticmethod
    def from_row(row):
        try:
            return WorkflowInputModel(row[1], json.loads(row[2]))
        except TypeError as e:
            # I've seen this happen even if you write a string to the database for some reason
            Logger.debug(f"Couldn't parse json value from row: {str(row)}: {repr(e)}")
            return WorkflowInputModel(row[1], row[2])

    def __repr__(self):
        return str((self.tag, self.value))
