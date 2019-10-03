import datetime

from janis_runner.utils.dateutil import DateUtil


class WorkflowOutputModel:
    def __init__(self, tag, originalpath, newpath, prefix, tags, timestamp):
        self.tag = tag
        self.originalpath = originalpath
        self.newpath = newpath
        self.prefix = prefix
        self.tags = tags

        if not isinstance(timestamp, datetime.datetime):
            timestamp = DateUtil.parse_iso(timestamp)
        self.timestamp = timestamp

    @staticmethod
    def from_row(row):
        return WorkflowOutputModel(row[0], row[1], row[2], row[3], row[4], row[5])
