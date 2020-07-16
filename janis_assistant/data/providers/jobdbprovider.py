from sqlite3 import OperationalError
from typing import List, TypeVar, Iterable, Union, Callable, Dict, Optional

from janis_core import Logger

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel

T = TypeVar("T")


def groupby(
    iterable: Iterable[T], selector: Union[str, Callable[[T], str]]
) -> Dict[str, T]:
    q = {}
    if isinstance(selector, str):
        k = selector
        selector = (
            lambda x: x[k] if hasattr(x, "__getitem__") else x.__getattribute__(k)
        )
    for i in iterable:
        key = selector(i)
        if key not in q:
            q[key] = []
        q[key].append(i)

    return q


class JobDbProvider(DbProviderBase):
    def __init__(self, db, submission_id):
        super().__init__(
            base_type=RunJobModel,
            db=db,
            tablename="jobs",
            scopes={"submission_id": submission_id},
        )
        self.submission_id = submission_id
        # self.eventsDB = JobEventDbProvider(self.db, self.wid)

    def get_single(
        self, id_: str, run_id: str = RunModel.DEFAULT_ID
    ) -> Optional[RunJobModel]:
        inp = self.get(where=("id = ? and run_id = ?", [id_, run_id]))
        if not inp:
            return None

        return inp[0]

    def get_with_children(self, jid: str) -> List[RunJobModel]:
        parent = self.get(jid)

        return parent

    def get_all_children(self, jids: List[str]) -> List[RunJobModel]:
        parsed = self.get(where=("parent in ?", [jids]))

        newjids = {p.jid for p in parsed}
        if len(newjids) > 0:
            children = self.get_all_children(list(newjids))
            mappedchildren = groupby(children, "parent")
            for p in parsed:
                p.jobs = mappedchildren[p.parentjid]

        return parsed

    def get_all(self, run_id=None) -> Optional[List[RunJobModel]]:
        where = None
        if run_id:
            where = ("run_id = ?", (run_id,))
        els: List[RunJobModel] = self.get(where=where)
        if not els:
            return els

        # I know there are duplicates, I don't know why yet so we'll mask it for now

        seen = set()
        rets = []
        for e in els:
            k = (e.submission_id, e.run_id, e.id_)
            if k in seen:
                continue
            seen.add(k)
            rets.append(e)

        return rets

        # query = "SELECT * FROM jobs WHERE wid = ?"
        # with self.with_cursor() as cursor:
        #     try:
        #         cursor.execute(query, (self.wid,))
        #         rows = cursor.fetchall()
        #     except OperationalError as e:
        #         if "readonly database" in str(e):
        #             # mfranklin: idk, this sometimes happens. We're doing a select query, idk sqlite3 driver...
        #             Logger.debug(
        #                 "Got readonly error when running query: '{query}', skipping for now"
        #             )
        #             return None
        #         elif "locked" in str(e):
        #             Logger.debug(
        #                 "We hit the database at the same time the janis process wrote to it, meh"
        #             )
        #             return None
        #         raise e
        #
        # return [RunJobModel.from_row(row) for row in rows]

    def get_all_mapped(self) -> Optional[List[RunJobModel]]:

        alljobs = self.get_all()
        if alljobs is None:
            return None
        # mfranklin: 2020-06-05 - nothing actually is stored in events, so skip it atm
        # events = self.eventsDB.get_all()

        groupedjobs = groupby([a for a in alljobs if a.parent], "parent")
        # groupedevents = groupby(events, lambda e: e.jid)

        for job in alljobs:
            job.jobs = groupedjobs.get(job.id_)
            # job.events = groupedevents.get(job.jid, [])

        return [j for j in alljobs if j.parent is None]

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
