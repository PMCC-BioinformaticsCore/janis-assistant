from datetime import datetime
from typing import Tuple, Optional, List, Union

from janis_assistant.utils.dateutils import DateUtil

from janis_assistant.data.dbproviderbase import DbProviderBase

# different to archivable
from janis_assistant.data.models.base import DatabaseObject, DatabaseObjectField
from janis_assistant.utils import Logger, fully_qualify_filename


class TaskRow(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[DatabaseObjectField]:
        return [
            DatabaseObjectField("submission_id", "id", True),
            DatabaseObjectField("output_dir"),
            DatabaseObjectField("execution_dir"),
            DatabaseObjectField("timestamp"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id              varchar(6), 
        output_dir      text,
        execution_dir   text,
        timestamp       text,
        """

    def __init__(
        self,
        submission_id,
        output_dir: str,
        execution_dir: str,
        timestamp: Optional[Union[str, datetime]] = None,
    ):
        self.submission_id = submission_id
        self.output_dir = output_dir
        self.execution_dir = execution_dir

        if not timestamp:
            timestamp = DateUtil.now()
        elif isinstance(timestamp, str):
            timestamp = DateUtil.parse_iso(timestamp)
        self.timestamp = timestamp


class TasksDbProvider(DbProviderBase):
    table_name = "tasks"

    def __init__(self, connection, readonly):
        super().__init__(
            TaskRow,
            db=connection,
            readonly=readonly,
            tablename=TasksDbProvider.table_name,
            scopes={},
        )

    def get_by_id(self, id_) -> Optional[TaskRow]:
        rows = self.get(where=("id = ?", [id_]))
        if len(rows) != 1:
            return None
        return rows[0]

    def get_all_tasks(self) -> [TaskRow]:
        return self.get()

    def insert_task(self, task: TaskRow) -> None:
        return self.insert_or_update_many([task])

    def remove_by_id(self, id_: str) -> None:
        Logger.info(f"Removing '{id_}' from database")
        with self.with_cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE id = ?", (id_,))

        self.commit()

    def remove_by_ids(self, ids: List[str]) -> None:
        if not isinstance(ids, list):
            ids = [ids]

        Logger.info("Removing ids: " + ", ".join(ids))
        seq = ", ".join(["?"] * len(ids))
        with self.with_cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE id in ({seq})", ids)

        self.commit()
