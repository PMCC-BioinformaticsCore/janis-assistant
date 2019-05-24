import json
from datetime import datetime
from typing import Union

from shepherd.utils.logger import Logger
from shepherd.data.schema import TaskStatus, TaskMetadata, JobMetadata


def cromwell_status_to_status(status: str) -> TaskStatus:
    st = status.lower()
    if st == "fail" or st == "starting":
        return TaskStatus.PROCESSING
    elif st == "submitted" or st == 'queuedincromwell':
        return TaskStatus.QUEUED
    elif st == "running":
        return TaskStatus.RUNNING
    elif st == "succeeded" or st == "done":
        return TaskStatus.COMPLETED
    elif st == "failed":
        return TaskStatus.FAILED
    else:
        Logger.critical(f"Didn't recognise CromwellStatus '{status}', returning TaskStatus.FAILED")
        return TaskStatus.FAILED


class CromwellMetadata:

    def __init__(self, metadata_dictionary: dict):

        if not isinstance(metadata_dictionary, dict):
            raise Exception("parameter 'metadata_dictionary' must be of type dict, please use the .parse method.")

        self.meta = metadata_dictionary

    @staticmethod
    def parse(json_meta):
        return CromwellMetadata(metadata_dictionary=json.loads(json_meta))

    def generate_call_times(self):
        if "calls" not in self.meta:
            raise Exception("'calls' array was not found in metadata, cannot generate call times stats")

        calls = self._generate_call_times_from_calls(self.meta["calls"])

        # generate TSV
        tsv_lines = sorted([(k, v["time"]) for k, v in calls.items()], key=lambda l: l[0])
        print("\n".join(x[0] + "\t" + str(x[1]) for x in tsv_lines))

        return json.dumps(calls, separators=(",", ": "), indent=4)

    @classmethod
    def _generate_call_times_from_calls(cls, calls, prefix="", include_subworkflow_total=True) -> dict:
        dcalls = {}

        for call_key in calls:
            call = calls[call_key][0]

            s, f = call["start"], call.get("end")
            sd = datetime.fromisoformat(s)
            fd = datetime.fromisoformat(f) if f else None

            prefixed_call_key = prefix + call_key

            if prefixed_call_key in dcalls:
                Logger.warn("Doesn't know how to handle multiple instances of calls, skipping this one (with id: " \
                            + call["id"])
                continue

            is_subworkflow_total = "subWorkflowMetadata" in call
            if is_subworkflow_total:
                dcalls.update(
                    cls._generate_call_times_from_calls(call["subWorkflowMetadata"]["calls"],
                                                        prefix=call_key + "."))

            if not is_subworkflow_total or include_subworkflow_total:
                dcalls[prefixed_call_key] = {
                    "start": s,
                    "end": f,
                    "time": (fd - sd).total_seconds() if fd else "N/A"
                }

        return dcalls

    @staticmethod
    def _parse_date(datestr):
        if not datestr: return None
        return datetime.fromisoformat(datestr)

    def standard(self):
        jobs = []

        for k in self.meta.get("calls"):
            jobs.append(self.parse_standard_call(k, self.meta["calls"][k]))

        return TaskMetadata(wid=self.meta.get("id"), name=self.meta.get("workflowName"),
                            status=cromwell_status_to_status(self.meta.get("status")),
                            start=self._parse_date(self.meta.get("start")),
                            finish=self._parse_date(self.meta.get("end")),
                            outputs=[], jobs=jobs, error=self.get_caused_by_text())

    def get_caused_by_text(self):
        if "failures" not in self.meta:
            return ""

        return CromwellMetadata.unwrap_caused_by(self.meta["failures"])


    @staticmethod
    def unwrap_caused_by(d) -> str:
        if isinstance(d, list):
            return ", ".join(CromwellMetadata.unwrap_caused_by(x) for x in d)

        message = ""
        m = d.get("message")
        if m and not m.startswith("Workflow failed"):
            message = m + ": "

        causedby: Union[list, dict] = d.get("causedBy")

        if not causedby:
            return message

        if len(causedby) > 0:
            message += CromwellMetadata.unwrap_caused_by(causedby)
        return message


    @classmethod
    def parse_standard_call(cls, name, calls):
        if len(calls) > 1:
            raise Exception("Too many calls for " + name)
        call = calls[0]
        sjs = []

        status = cromwell_status_to_status(call.get("executionStatus"))

        if "subWorkflowMetadata" in call:
            sw = call["subWorkflowMetadata"]

            for k in sw.get("calls"):
                sjs.append(cls.parse_standard_call(k, sw["calls"][k]))

        return JobMetadata(
            name=name, status=status, job_id=call.get("jobId"), backend=None,
            runtime_attributes=None,
            outputs=call.get("outputs"),
            exec_dir="",
            stdout=call.get("stdout"),
            stderr=call.get("stderr"),
            start=cls._parse_date(call.get("start")), finish=cls._parse_date(call.get("end")),
            subjobs=sjs,
            from_cache=call["callCaching"].get("hit") if "callCaching" in call else False,
            shard=call.get("shardIndex")
        )
