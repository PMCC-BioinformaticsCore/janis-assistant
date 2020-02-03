from typing import List, TypeVar, Iterable, Union, Callable, Dict

from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.providers.jobeventdbprovider import JobEventDbProvider
from janis_assistant.data.models.workflowjob import WorkflowJobModel

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
    CURRENT_SCHEMA_VERSION = 1

    def table_schema(self):
        return """\
        CREATE TABLE IF NOT EXISTS jobs (
            wid STRING,
            jid STRING,
            parentjid NULLABLE STRING,
            name STRING,
            batchid STRING,
            shard NULLABLE INT,
            attempt NULLABLE INT,
            container STRING,
            status STRING,
            start STRING,
            finish NULLABLE STRING,
            backend STRING,
            cached BOOLEAN,
            stdout STRING,
            stderr STRING,
            PRIMARY KEY (wid, jid),
            FOREIGN KEY (wid, parentjid) REFERENCES jobs(wid, parentjid)
        )
        """

    def __init__(self, db, cursor, wid):
        super().__init__(db, cursor)
        self.wid = wid
        self.eventsDB = JobEventDbProvider(self.db, self.cursor, self.wid)

    def get(self, jid: str) -> WorkflowJobModel:
        self.cursor.execute(
            "SELECT * FROM jobs WHERE wid = ? AND jid = ?", (self.wid, jid)
        )
        row = self.cursor.fetchone()
        if not row:
            raise KeyError("Couldn't find output with id = " + jid)

        return WorkflowJobModel.from_row(row)

    def get_with_children(self, jid: str) -> WorkflowJobModel:
        parent = self.get(jid)

        return parent

    def get_all_children(self, jids: List[str]) -> List[WorkflowJobModel]:
        self.cursor.execute(
            "SELECT * FROM jobs WHERE wid = ? AND parentjid in ?", (self.wid, jids)
        )
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

    def get_all(self) -> List[WorkflowJobModel]:
        self.cursor.execute("SELECT * FROM jobs WHERE wid = ?", (self.wid,))
        rows = self.cursor.fetchall()
        return [WorkflowJobModel.from_row(row) for row in rows]

    def get_all_mapped(self) -> List[WorkflowJobModel]:
        self.cursor.execute("SELECT * FROM jobs WHERE wid = ?", (self.wid,))
        rows = self.cursor.fetchall()
        alljobs = [WorkflowJobModel.from_row(row) for row in rows]
        events = self.eventsDB.get_all()

        groupedjobs = groupby([a for a in alljobs if a.parentjid], "parentjid")
        groupedevents = groupby(events, lambda e: e.jid)

        for job in alljobs:
            job.jobs = groupedjobs.get(job.jid)
            job.events = groupedevents.get(job.jid, [])

        return [j for j in alljobs if j.parentjid is None]

    def insert(self, model: WorkflowJobModel):
        return self.cursor.execute(
            self._insert_statement, self._insert_model_obj(model)
        )

    def update(self, model: WorkflowJobModel):
        return self.cursor.execute(*self._update_model_obj(model))

    _insert_statement = """\
        INSERT INTO jobs (
            wid, jid, parentjid, name, batchid, shard, attempt, container, status,
            start, finish, backend, cached, stdout, stderr
        ) VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    def _insert_model_obj(self, model: WorkflowJobModel):
        return (
            self.wid,
            model.jid,
            model.parentjid,
            model.name,
            model.batchid,
            model.shard,
            model.attempt,
            model.container,
            model.status.value,
            model.start,
            model.finish,
            model.backend,
            model.cached,
            model.stdout,
            model.stderr,
        )

    def _update_model_obj(self, model: WorkflowJobModel):
        obj = {
            "parentjid": model.parentjid,
            "name": model.name,
            "batchid": model.batchid,
            "shard": model.shard,
            "attempt": model.attempt,
            "container": model.container,
            "status": model.status.value,
            "start": model.start,
            "finish": model.finish,
            "backend": model.backend,
            "cached": model.cached,
            "stdout": model.stdout,
            "stderr": model.stderr,
        }

        kvs = [(k, v) for k, v in obj.items() if v]
        mapped = ", ".join(f"{k[0]}=?" for k in kvs)
        return (
            f"UPDATE jobs SET {mapped} WHERE wid = ? AND jid = ? ",
            [v[1] for v in kvs] + [self.wid, model.jid],
        )

    def update_or_insert_many(self, jobs: List[WorkflowJobModel]):
        allidsr = self.cursor.execute(
            "SELECT jid FROM jobs WHERE wid = ?", (self.wid,)
        ).fetchall()
        allids = set(r[0] for r in allidsr)

        inserts = []
        events = []

        for job in jobs:
            if job.jid in allids:
                # Update
                self.cursor.execute(*self._update_model_obj(job))
            else:
                # Insert
                inserts.append(self._insert_model_obj(job))
            if job.events:
                events.extend(job.events)

        if inserts:
            try:
                self.cursor.executemany(self._insert_statement, inserts)
            except Exception as e:
                print(e)

        self.db.commit()

        self.eventsDB.insert_many(events)

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
