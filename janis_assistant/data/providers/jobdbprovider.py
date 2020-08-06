from sys import getsizeof
from sqlite3 import OperationalError
from typing import List, TypeVar, Iterable, Union, Callable, Dict, Optional, Tuple

from janis_assistant.data.enums import TaskStatus
from janis_core import Logger

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.run import RunModel
from janis_assistant.data.models.workflowjob import RunJobModel

T = TypeVar("T")


def groupby(
    iterable: Iterable[T], selector: Union[str, Callable[[T], str]]
) -> Dict[str, Iterable[T]]:
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
    job_cache_warnings = [10, 100, 500, 1000]

    def __init__(self, db, readonly, submission_id):
        super().__init__(
            base_type=RunJobModel,
            db=db,
            readonly=readonly,
            tablename="jobs",
            scopes={"submission_id": submission_id},
        )
        self.submission_id = submission_id
        self.job_cache_last_idx = 0

        """
        We seem to persistently have problems with updating the DB, we can't use
        newer versions of SQLite because of a stakeholder issue.
        """
        self._cache_completed_ids: Dict[Tuple, str] = {}

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

    ## FILTERING

    def populate_cache_if_required(self):
        if self._cache_completed_ids is not None:
            return False
        self.populate_cache()
        return True

    def populate_cache(self):
        self._cache_completed_ids = {}
        idkeys = set(self.get_id_keys())
        idkeys_ordered = list(idkeys)
        prows = f"SELECT {', '.join(idkeys_ordered)}, status FROM {self._tablename}"
        with self.with_cursor() as cursor:
            rows = cursor.execute(prows).fetchall()

            if len(rows) == 0:
                return

            for row in rows:
                idkeys = tuple(row[:-2])
                # is_updateable = not TaskStatus(row[-1]).is_in_final_state()
                self._cache_completed_ids[idkeys] = row[-1]

    def filter_updates(
        self, jobs: List[RunJobModel], add_inserts_to_cache=True
    ) -> Tuple[List[RunJobModel], List[RunJobModel]]:
        # don't call super, it'll break because of the cache

        updates = []
        inserts = []

        if len(jobs) == 0:
            return updates, inserts

        self.populate_cache_if_required()

        idkeys = set(self.get_id_keys())
        idkeys_ordered = list(idkeys)
        dbalias_map = {t.dbalias: t.name for t in self._base.keymap()}
        skipped = 0

        for job in jobs:
            el_idkey = tuple([getattr(job, dbalias_map[_k]) for _k in idkeys_ordered])

            jstatus = self._cache_completed_ids.get(el_idkey)
            if jstatus is None:
                inserts.append(job)
            elif job.status.value != jstatus:
                updates.append(job)
            elif jstatus:
                skipped += 1
            self._cache_completed_ids[el_idkey] = job.status.value

        if skipped:
            Logger.log(
                f"Skipped updating {skipped} jobs as those jobs were already in a final state"
            )
        memory = getsizeof(self._cache_completed_ids) // 1024
        if (
            self.job_cache_last_idx < len(self.job_cache_warnings)
            and memory > self.job_cache_warnings[self.job_cache_last_idx]
        ):
            Logger.warn(f"Job cache is using {memory} MB")
            self.job_cache_last_idx += 1

        return updates, inserts

        # We'll remove all updates

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
