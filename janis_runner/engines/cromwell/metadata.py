import json
from typing import Union

from janis_runner.data.models.schema import TaskStatus, TaskMetadata, JobMetadata
from janis_runner.utils.dateutil import DateUtil
from janis_runner.utils.logger import Logger


def cromwell_status_to_status(status: str) -> TaskStatus:
    st = status.lower()
    if st == "fail" or st == "starting":
        return TaskStatus.PROCESSING
    elif st == "submitted" or st == "queuedincromwell":
        return TaskStatus.QUEUED
    elif st == "running":
        return TaskStatus.RUNNING
    elif st == "succeeded" or st == "done":
        return TaskStatus.COMPLETED
    elif st == "failed":
        return TaskStatus.FAILED
    else:
        Logger.critical(
            f"Didn't recognise CromwellStatus '{status}', returning TaskStatus.FAILED"
        )
        return TaskStatus.FAILED


class CromwellMetadata:
    def __init__(self, metadata_dictionary: dict):

        if not isinstance(metadata_dictionary, dict):
            raise Exception(
                "parameter 'metadata_dictionary' must be of type dict, please use the .parse method."
            )

        self.meta = metadata_dictionary

    @staticmethod
    def parse(json_meta):
        return CromwellMetadata(metadata_dictionary=json.loads(json_meta))

    def generate_call_times(self):
        if "calls" not in self.meta:
            raise Exception(
                "'calls' array was not found in metadata, cannot generate call times stats"
            )

        calls = self._generate_call_times_from_calls(self.meta["calls"])

        # generate TSV
        tsv_lines = sorted(
            [(k, v["time"]) for k, v in calls.items()], key=lambda l: l[0]
        )
        print("\n".join(x[0] + "\t" + str(x[1]) for x in tsv_lines))

        return json.dumps(calls, separators=(",", ": "), indent=4)

    @classmethod
    def _generate_call_times_from_calls(
        cls, calls, prefix="", include_subworkflow_total=True
    ) -> dict:
        dcalls = {}

        for call_key in calls:
            call = calls[call_key][0]

            s, f = call["start"], call.get("end")
            sd = DateUtil.parse_iso(s)
            fd = DateUtil.parse_iso(f) if f else None

            prefixed_call_key = prefix + call_key

            if prefixed_call_key in dcalls:
                Logger.warn(
                    "Doesn't know how to handle multiple instances of calls, skipping this one (with id: "
                    + call["id"]
                )
                continue

            is_subworkflow_total = "subWorkflowMetadata" in call
            if is_subworkflow_total:
                dcalls.update(
                    cls._generate_call_times_from_calls(
                        call["subWorkflowMetadata"]["calls"], prefix=call_key + "."
                    )
                )

            if not is_subworkflow_total or include_subworkflow_total:
                dcalls[prefixed_call_key] = {
                    "start": s,
                    "end": f,
                    "time": (fd - sd).total_seconds() if fd else "N/A",
                }

        return dcalls

    def standard(self):
        jobs = []

        s = DateUtil.parse_iso(self.meta.get("start"))
        f = DateUtil.parse_iso(self.meta.get("end"))
        st = 0
        if s:
            ff = f if f else DateUtil.now()
            st = int(DateUtil.secs_difference(s, ff))

        for k in self.meta.get("calls"):

            jobs.extend(self.parse_standard_call(k, self.meta["calls"][k], st))

        return TaskMetadata(
            wid=self.meta.get("id"),
            name=self.meta.get("workflowName"),
            status=cromwell_status_to_status(self.meta.get("status")),
            start=DateUtil.parse_iso(self.meta.get("start")),
            finish=DateUtil.parse_iso(self.meta.get("end")),
            outputs=[],
            jobs=jobs,
            error=self.get_caused_by_text(),
        )

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
    def parse_standard_call(cls, name, calls, supertime):
        if len(calls) > 1:
            processed_calls = []
            for c in calls:
                processed_calls.extend(cls.parse_standard_call(name, [c], supertime=0))

            statuses = set(s.status for s in processed_calls)
            status = list(statuses)[0] if len(statuses) == 1 else TaskStatus.RUNNING
            start = min(s.start for s in processed_calls)
            finishes = [s.finish for s in processed_calls]
            finish = None if any(f is None for f in finishes) else max(finishes)

            st = 0.01
            if start:
                ff = finish if finish else DateUtil.now()

                st = (ff - start).total_seconds()
            for c in processed_calls:
                c.supertime = st

            return [
                JobMetadata(
                    name=name,
                    status=status,
                    job_id=None,
                    backend=None,
                    runtime_attributes=None,
                    exec_dir=None,
                    stdout=None,
                    stderr=None,
                    start=start,
                    finish=finish,
                    subjobs=processed_calls,
                    from_cache=False,
                    shard=None,
                    outputs=[],
                    super_time=supertime,
                )
            ]

        call = calls[0]
        sjs = []

        status = cromwell_status_to_status(call.get("executionStatus"))

        s = DateUtil.parse_iso(call.get("start"))
        f = DateUtil.parse_iso(call.get("end"))
        st = 0
        if s:
            s = s
            ff = f if f else DateUtil.now()
            st = int(DateUtil.secs_difference(s, ff))

        if "subWorkflowMetadata" in call:
            sw = call["subWorkflowMetadata"]

            for k in sw.get("calls"):
                sjs.extend(cls.parse_standard_call(k, sw["calls"][k], st))

        return [
            JobMetadata(
                name=name,
                status=status,
                job_id=call.get("jobId"),
                backend=None,
                runtime_attributes=None,
                outputs=call.get("outputs"),
                exec_dir="",
                stdout=call.get("stdout"),
                stderr=call.get("stderr"),
                start=s,
                finish=f,
                super_time=supertime,
                subjobs=sjs,
                from_cache=call["callCaching"].get("hit")
                if "callCaching" in call
                else False,
                shard=call.get("shardIndex"),
            )
        ]
