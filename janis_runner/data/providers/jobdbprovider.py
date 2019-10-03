from typing import List, TypeVar, Iterable, Union, Callable, Dict

from janis_runner.data.dbproviderbase import DbProviderBase
from janis_runner.data.providers.jobeventdbprovider import JobEventDbProvider
from janis_runner.data.models.workflowjob import WorkflowJobModel

T = TypeVar("T")


def groupby(
    iterable: Iterable[T], selector: Union[str, Callable[[T], str]]
) -> Dict[str, T]:
    q = {}
    if isinstance(selector, str):
        key = selector
        selector = lambda x: x[key] if hasattr(x, "__getitem__") else x.__getattr__(key)
    for i in iterable:
        key = selector(i)
        if key not in q:
            q[key] = []
        q[key].append(i)

    return q


class JobDbProvider(DbProviderBase):
    CURRENT_SCHEMA_VERSION = 1

    def create_table(self):
        return """\
        CREATE TABLE IF NOT EXISTS jobs (
            jid STRING PRIMARY KEY,
            parentjid NULLABLE STRING,
            name STRING,
            batchid STRING,
            shard INT,
            container STRING,
            start STRING,
            finish NULLABLE STRING,
            backend STRING,
            cached BOOLEAN,
            stderr STRING,
            stdout STRING,
            FOREIGN KEY (parentjid) REFERENCES jobs
        )
        """

    def __init__(self, db, cursor):
        super().__init__(db, cursor)
        self.eventsDB = JobEventDbProvider(self.db, self.cursor)

    def get(self, jid: str) -> WorkflowJobModel:
        self.cursor.execute("SELECT * FROM jobs WHERE jid = ?", (jid,))
        row = self.cursor.fetchone()
        if not row:
            raise KeyError("Couldn't find output with id = " + jid)

        return WorkflowJobModel.from_row(row)

    def get_with_children(self, jid: str) -> WorkflowJobModel:
        parent = self.get(jid)

        return parent

    def get_all_children(self, jids: List[str]) -> List[WorkflowJobModel]:
        self.cursor.execute("SELECT * FROM jobs WHERE parentjid in ?", (jids,))
        rows = self.cursor.fetchall()
        if not rows:
            return []
        parsed = [WorkflowJobModel.from_row(r) for r in rows]

        newjids = {p.jid for p in parsed}
        if len(newjids) > 0:
            children = self.get_all_children(list(newjids))
            mappedchildren = groupby(children, "parentjid")
            for p in parsed:
                p.jobs = mappedchildren[p.parentjid]

        return parsed

    def get_all(self, tag: str) -> List[WorkflowJobModel]:
        self.cursor.execute("SELECT * FROM jobs", (tag,))
        rows = self.cursor.fetchall()
        return [WorkflowJobModel.from_row(row) for row in rows]

    def get_all_mapped(self, tag: str) -> List[WorkflowJobModel]:
        self.cursor.execute("SELECT * FROM jobs", (tag,))
        rows = self.cursor.fetchall()
        alljobs = [WorkflowJobModel.from_row(row) for row in rows]
        events = self.eventsDB.get_all()

        groupedjobs = groupby([a for a in alljobs if a.parentjid], "parentjid")
        groupedevents = groupby(events, lambda e: e.jid)

        for job in alljobs:
            job.jobs = groupedjobs.get(job.parentjid)
            job.events = groupedevents.get(job.jid, [])

        return alljobs

    def insert(self, model: WorkflowJobModel):
        return self.cursor.execute(
            self._insert_statement, self._insert_model_obj(model)
        )

    def update(self, model: WorkflowJobModel):
        return self.cursor.execute(
            self._update_statement, self._update_model_obj(model)
        )

    _insert_statement = """\
        INSERT INTO jobs (
            jid, parentjid, jobname, batchid, shard, container, 
            start, finish, backend, cached, stdout, stderr
        ) VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    _update_statement = """\
        UPDATE jobs SET
            parentjid=?,
            jobname=?,
            batchid=?,
            shard=?,
            container=?,
            start=?,
            finish=?,
            backend=?,
            cached=?,
            stdout=?,
            stderr=?,
        WHERE jid = ?
        """

    def _insert_model_obj(self, model: WorkflowJobModel):
        return (
            model.jid,
            model.parentjid,
            model.name,
            model.batchid,
            model.shard,
            model.container,
            model.start,
            model.finish,
            model.backend,
            model.cached,
            model.stdout,
            model.stderr,
        )

    def _update_model_obj(self, model: WorkflowJobModel):
        return (
            model.parentjid,
            model.name,
            model.batchid,
            model.shard,
            model.container,
            model.start,
            model.finish,
            model.backend,
            model.cached,
            model.stdout,
            model.stderr,
            model.jid,
        )

    def update_or_insert_many(self, jobs: List[WorkflowJobModel]):
        allidsr = self.cursor.execute("SELECT jid FROM jobs").fetchall()
        allids = set(allidsr)

        updates = []
        inserts = []
        events = []

        for job in jobs:
            if job.jid in allids:
                # Update
                updates.append(self._update_model_obj(job))
            else:
                # Insert
                inserts.append(self._insert_model_obj(job))
            if job.events:
                events.extend(job.events)

        if updates:
            self.cursor.executemany(self._update_statement, updates)

        if inserts:
            self.cursor.executemany(self._insert_statement, inserts)

        self.eventsDB.update_or_insert_many(events)

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
