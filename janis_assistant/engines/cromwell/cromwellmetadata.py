import json
import os.path
from typing import Union, List, Dict, Optional

from janis_assistant.data.enums.taskstatus import TaskStatus
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.utils.dateutils import DateUtil
from janis_core.utils.logger import Logger


def cromwell_status_to_status(status: str) -> TaskStatus:
    if status is None:
        return TaskStatus.PROCESSING
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
    elif st == "aborting":
        return TaskStatus.ABORTING
    elif st == "aborted":
        return TaskStatus.ABORTED
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

    def standard(self) -> RunModel:
        jobs = []

        s = DateUtil.parse_iso(self.meta.get("start"))
        f = DateUtil.parse_iso(self.meta.get("end"))
        st = 0
        if s:
            ff = f if f else DateUtil.now()
            st = int(DateUtil.secs_difference(s, ff))

        jid = self.meta.get("id")
        for stepname, call in self.meta.get("calls").items():
            stepname = ".".join(stepname.split(".")[1:])
            jobs.extend(self.parse_standard_calls(None, stepname, call))

        model = RunModel(
            submission_id=None,
            id_=None,
            engine_id=jid,
            name=self.meta.get("workflowName"),
            # start=s,
            # finish=f,
            execution_dir=self.meta.get("workflowRoot"),
            status=cromwell_status_to_status(self.meta.get("status")),
            error=self.get_caused_by_text(),
        )
        model.jobs = jobs
        return model

    def get_caused_by_text(self):
        if "failures" not in self.meta:
            return ""

        return CromwellMetadata.unwrap_caused_by(self.meta["failures"])

    @staticmethod
    def unwrap_caused_by(d: Union[List, Dict]) -> Optional[str]:
        if d is None:
            return None
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
    def parse_standard_call(cls, parentid, stepname, call):
        parent = parentid
        jid = f"{parentid}_{stepname}" if parentid else stepname
        shard = call.get("shardIndex")
        attempt = call.get("attempt")

        is_shard = shard is not None and shard >= 0
        is_second_attempt = attempt is not None and attempt > 1

        # We'll rebase the parent job if there are any shards (so they all sit under one heading)
        # OR if it's a second attempt, we'll group it under the first attempt.

        if is_shard:
            parent = jid
            # We rely on the calling function to create a job with the parentId we're setting.
            jid += f"_shard-{shard}"
        if is_second_attempt:
            # There will already be a parent with this id_ (as this is the second attempt)
            parent = jid
            jid += f"_attempt-{attempt}"

        subjobs = []
        status = cromwell_status_to_status(
            call.get("status") or call.get("executionStatus")
        )
        start = call.get("start")
        finish = call.get("end")

        # if the call has "subworkflowMetadata", we have a different schema:
        if "subWorkflowMetadata" in call:
            # it's actually a workflow
            submeta = call.get("subWorkflowMetadata")
            for sn, subcalls in submeta.get("calls", {}).items():
                sn = ".".join(sn.split(".")[1:])
                subjobs.extend(cls.parse_standard_calls(jid, sn, subcalls))

            start = start or min(s.start for s in subjobs)
            finish = finish

        callroot = call.get("callRoot")

        return RunJobModel(
            id_=jid,
            submission_id=None,
            run_id=None,
            parent=parent,
            container=call.get(
                "dockerImageUsed", call.get("runtimeAttributes", {}).get("docker")
            ),
            name=stepname,
            status=status,
            batchid=call.get("jobId"),
            backend=None,
            stdout=call.get("stdout"),
            stderr=call.get("stderr"),
            start=DateUtil.parse_iso(start) if start is not None else DateUtil.now(),
            finish=DateUtil.parse_iso(finish) if finish is not None else None,
            jobs=subjobs or None,
            cached=call.get("callCaching").get("hit")
            if "callCaching" in call
            else False,
            shard=shard,
            attempt=attempt,
            analysis=None,
            memory=None,
            cpu=None,
            error=CromwellMetadata.unwrap_caused_by(call.get("failures")),
            returncode=call.get("returnCode"),
            workdir=call.get("callRoot"),
            script=("file:/" + os.path.join(callroot, "execution/script"))
            if callroot
            else None,
        )

    @classmethod
    def parse_standard_calls(cls, parentid, name, calls) -> List[RunJobModel]:

        jobs = []
        parents: Dict[str, RunJobModel] = {}
        for c in calls:
            job = cls.parse_standard_call(parentid, name, c)

            if job.shard is None:
                jobs.append(job)
            else:
                if job.parent in parents:
                    parents[job.parent].jobs.append(job)
                else:
                    parents[job.parent] = RunJobModel(
                        submission_id=job.submission_id,
                        run_id=job.run_id,
                        id_=job.parent,
                        parent=parentid,
                        jobs=[job],
                        name=job.name,
                        status=TaskStatus.PROCESSING,
                    )

        for parent in parents.values():
            parent.start = min(j.start for j in parent.jobs)
            finishes = [j.finish for j in parent.jobs]
            if all(f is not None for f in finishes):
                parent.finish = max(finishes)
            parent.status = TaskStatus.collapse_states([j.status for j in parent.jobs])

        return [*parents.values(), *jobs]

        # jid_temp = parentid + "_" + name
        #
        # if len(calls) > 1:
        #     # This means there are multiple shards / attempts
        #     processed_calls = []
        #
        #     for c in calls:
        #         jid = jid_temp
        #         shard = c.get("shardIndex")
        #         attempt = c.get("attempt")
        #
        #         if shard is not None and shard >= 0:
        #             jid += f"_shard-{shard}"
        #         if attempt and attempt > 1:
        #             jid += f"_attempt-{attempt}"
        #
        #         processed_calls.extend(
        #             cls.parse_standard_calls(jid, name, [c], supertime=0)
        #         )
        #
        #     statuses = set(s.status for s in processed_calls)
        #     status = list(statuses)[0] if len(statuses) == 1 else TaskStatus.RUNNING
        #     start = min(s.start for s in processed_calls)
        #     finishes = [s.finish for s in processed_calls]
        #     finish = None if any(f is None for f in finishes) else max(finishes)
        #
        #     st = 0.01
        #     if start:
        #         ff = finish if finish else DateUtil.now()
        #
        #         st = (ff - start).total_seconds()
        #     for c in processed_calls:
        #         c.supertime = st
        #
        #     j = RunJobModel(
        #         jid=jid,
        #         name=name,
        #         parentjid=parentid,
        #         status=status,
        #         batchid=None,
        #         backend=None,
        #         # runtime_attributes=None,
        #         # exec_dir=None,
        #         stdout=None,
        #         stderr=None,
        #         start=start,
        #         finish=finish,
        #         # subjobs=processed_calls,
        #         cached=False,
        #         shard=None,
        #         # outputs=[],
        #         # super_time=supertime,
        #         container=None,
        #     )
        #     j.jobs = processed_calls
        #     return [j]
        #
        # call = calls[0]
        # sjs = []
        #
        # status = cromwell_status_to_status(call.get("executionStatus"))
        #
        # s = DateUtil.parse_iso(call.get("start"))
        # f = DateUtil.parse_iso(call.get("end"))
        # st = 0
        # if s:
        #     s = s
        #     ff = f if f else DateUtil.now()
        #     st = int(DateUtil.secs_difference(s, ff))
        #
        # if "subWorkflowMetadata" in call:
        #     sw = call["subWorkflowMetadata"]
        #
        #     for k in sw.get("calls"):
        #         sjs.extend(cls.parse_standard_calls(jid, k, sw["calls"][k], st))
        #
        # j = RunJobModel(
        #     jid=jid,
        #     parentjid=parentid,
        #     container=call.get(
        #         "dockerImageUsed", call.get("runtimeAttributes", {}).get("docker")
        #     ),
        #     name=name,
        #     status=status,
        #     batchid=call.get("jobId"),
        #     backend=None,
        #     # outputs=call.get("outputs"),
        #     # exec="",
        #     stdout=call.get("stdout"),
        #     stderr=call.get("stderr"),
        #     start=s,
        #     finish=f,
        #     # super_time=supertime,
        #     jobs=sjs,
        #     cached=call["callCaching"].get("hit") if "callCaching" in call else False,
        #     shard=shard,
        # )
        # return [j]
