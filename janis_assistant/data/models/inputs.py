import json


class WorkflowInputModel:
    def __init__(self, tag: str, value: str):
        self.tag = tag
        self.value = value

    def get_safe_value(self):
        return json.dumps(self.value)

    @staticmethod
    def from_row(row):
        return WorkflowInputModel(row[1], json.loads(row[2]))
