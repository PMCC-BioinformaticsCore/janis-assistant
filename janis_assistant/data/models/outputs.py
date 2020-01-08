import datetime
from os.path import commonprefix

from typing import Optional, List


from janis_assistant.utils.dateutil import DateUtil


class WorkflowOutputModel:

    ARRAY_SEPARATOR_1 = "::"
    ARRAY_SEPARATOR_NESTED = "||"

    def __init__(
        self,
        tag: str,
        iscopyable: bool,
        original_path: Optional[str],
        new_path: Optional[str],
        timestamp: Optional[str],
        output_name: Optional[str],
        output_folder: Optional[str],
        secondaries,
        extension,
        value: str = None,
    ):
        self.tag = tag
        self.iscopyable = iscopyable
        self.originalpath = original_path
        self.newpath = new_path

        self.prefix: Optional[List[str]] = None
        if output_name:
            if isinstance(output_name, str):
                self.prefix = self.to_array(output_name)
            else:
                self.prefix = [t for t in output_name if t is not None]

        self.tags = None
        if output_folder:
            if isinstance(output_folder, str):
                self.tags = self.to_array(output_folder)
            else:
                self.tags = [t for t in output_folder if t is not None]

        self.secondaries = None
        if secondaries:
            if isinstance(secondaries, str):
                self.secondaries = self.to_array(secondaries)
            else:
                self.secondaries = secondaries

        if not isinstance(timestamp, datetime.datetime):
            timestamp = DateUtil.parse_iso(timestamp)
        self.timestamp = timestamp
        self.extension = extension
        self.value = value

    @staticmethod
    def from_row(row):
        return WorkflowOutputModel(*row[1:])

    @staticmethod
    def from_array(array):
        if not array:
            return None
        """
        At most, two levels. One separated by one separator, then the second by the other
        :param array: iterable
        :return:
        """
        return WorkflowOutputModel.ARRAY_SEPARATOR_1.join(
            WorkflowOutputModel.ARRAY_SEPARATOR_NESTED.join(i)
            if isinstance(i, list)
            else str(i)
            for i in array
        )

    @staticmethod
    def to_array(value: str):
        if not value:
            return []
        s = value.split(WorkflowOutputModel.ARRAY_SEPARATOR_1)
        return [
            ss.split(WorkflowOutputModel.ARRAY_SEPARATOR_NESTED)
            if WorkflowOutputModel.ARRAY_SEPARATOR_NESTED in ss
            else ss
            for ss in s
        ]

    def format(self):
        return f"- {self.tag}: {self._format_value(self.newpath)}"

    @staticmethod
    def _format_value(value, isroot=True):
        if not value:
            return ""

        if isinstance(value, str):
            value = value.split("|")

        if isinstance(value, list):
            if len(value) == 0:
                return ""
            elif len(value) == 1:
                value = value[0]
            else:
                values = [
                    WorkflowOutputModel._format_value(v, isroot=False) for v in value
                ]
                if isroot:
                    return "(multiple) " + commonprefix(values) + "*"
                return commonprefix(values)

        if any(isinstance(value, T) for T in [str, float, int, bool]):
            return str(value)

        # hmmm, idk
        return str(value)
